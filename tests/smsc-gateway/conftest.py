"""Shared fixtures for SMSC Gateway integration tests.

These tests validate the gateway using only external open-source SMPP tools.
The gateway is a black box — tests connect to it as an ESME and verify that
messages flow correctly to/from the downstream SMSC simulator.

Topology:
    pytest (smpplib ESME)  -->  smsc-gateway:2776  -->  smppsim:2775
"""

import os
import select
import threading
import time
import logging
import pytest
import smpplib
import smpplib.client
import smpplib.consts
import smpplib.gsm
import smpplib.smpp

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("smscgw-test")

# Gateway config — set via env vars or defaults for compose stack.
GW_HOST = os.environ.get("GW_HOST", "127.0.0.1")
GW_PORT = int(os.environ.get("GW_PORT", "2776"))
GW_SYSTEM_ID = os.environ.get("GW_SYSTEM_ID", "test-esme")
GW_PASSWORD = os.environ.get("GW_PASSWORD", "password")


class ESMEClient:
    """Wrapper around smpplib.client for test convenience.

    smpplib's send_message() returns the *request* PDU, not the response.
    This wrapper polls for the submit_sm_resp and returns it so tests can
    read resp.message_id directly.

    ACK modes for deliver_sm:
      auto_ack=True   — immediately ACK every deliver_sm (default, current behavior)
      auto_ack=False  — store PDU, test must call ack_deliver(sequence) manually
      auto_ack="delay" — ACK after deliver_ack_delay_sec seconds
    """

    def __init__(self, host, port, system_id, password,
                 auto_ack=True, deliver_ack_delay_sec=1.0):
        self.host = host
        self.port = port
        self.system_id = system_id
        self.password = password
        self.auto_ack = auto_ack
        self.deliver_ack_delay_sec = deliver_ack_delay_sec
        self.client = None
        self.received_pdus = []
        self._deliver_events = []
        self._submit_resps = []
        self._unacked_delivers = {}  # sequence → PDU (for manual ACK mode)
        self._delay_timers = []

    def connect(self):
        self.client = smpplib.client.Client(self.host, self.port, timeout=10)
        self.client.connect()
        self.client.set_message_sent_handler(self._on_submit_resp)
        self.client.set_message_received_handler(self._on_deliver)

    def bind(self):
        self.client.bind_transceiver(
            system_id=self.system_id,
            password=self.password,
        )

    def bind_expect_fail(self):
        """Attempt bind and return True if it failed."""
        try:
            self.client.bind_transceiver(
                system_id=self.system_id,
                password=self.password,
            )
            return False
        except smpplib.exceptions.PDUError:
            return True
        except smpplib.exceptions.ConnectionError:
            return True

    def submit_sm(self, source_addr, dest_addr, short_message,
                  source_addr_ton=0x05, source_addr_npi=0x00,
                  dest_addr_ton=0x01, dest_addr_npi=0x01,
                  esm_class=0x00, protocol_id=0x00,
                  data_coding=0x00, registered_delivery=0x01):
        """Send a submit_sm and return the submit_sm_resp PDU."""
        sent = self.client.send_message(
            source_addr_ton=source_addr_ton,
            source_addr_npi=source_addr_npi,
            source_addr=source_addr,
            dest_addr_ton=dest_addr_ton,
            dest_addr_npi=dest_addr_npi,
            destination_addr=dest_addr,
            short_message=short_message,
            esm_class=esm_class,
            protocol_id=protocol_id,
            data_coding=data_coding,
            registered_delivery=registered_delivery,
        )
        return self._wait_for_submit_resp(sent.sequence)

    def submit_sm_raw(self, source_addr, dest_addr, short_message, **kwargs):
        """Send submit_sm with arbitrary kwargs. Returns submit_sm_resp."""
        sent = self.client.send_message(
            source_addr=source_addr,
            destination_addr=dest_addr,
            short_message=short_message,
            **kwargs,
        )
        return self._wait_for_submit_resp(sent.sequence)

    # ── deliver_sm ACK control ──────────────────────────────────────────

    def ack_deliver(self, sequence, status=smpplib.consts.SMPP_ESME_ROK):
        """Manually ACK a deliver_sm by sequence number.

        Only meaningful when auto_ack=False. Sends deliver_sm_resp with
        the given status code.
        """
        if sequence not in self._unacked_delivers:
            raise ValueError(f"No unacked deliver_sm with sequence {sequence}")
        del self._unacked_delivers[sequence]
        self._send_deliver_resp(sequence, status)

    def nack_deliver(self, sequence, status=smpplib.consts.SMPP_ESME_RSYSERR):
        """Manually NACK a deliver_sm by sequence number.

        Sends deliver_sm_resp with an error status code.
        """
        self.ack_deliver(sequence, status=status)

    def disconnect_without_ack(self):
        """Disconnect the TCP connection without ACKing pending deliver_sm PDUs.

        This simulates an abrupt northbound connection loss while the
        gateway is waiting for deliver_sm_resp.
        """
        # Cancel any pending delay timers.
        for timer in self._delay_timers:
            timer.cancel()
        self._delay_timers.clear()
        # Hard close — no unbind, no pending ACKs sent.
        try:
            self.client._socket.close()
        except Exception:
            pass

    @property
    def unacked_sequences(self):
        """Return list of deliver_sm sequence numbers awaiting ACK."""
        return list(self._unacked_delivers.keys())

    # ── Internal PDU handling ───────────────────────────────────────────

    def _send_deliver_resp(self, sequence, status=smpplib.consts.SMPP_ESME_ROK):
        """Send deliver_sm_resp for the given sequence."""
        resp = smpplib.smpp.make_pdu(
            "deliver_sm_resp",
            client=self.client,
            status=status,
            sequence=sequence,
        )
        self.client.send_pdu(resp)

    def _read_available(self, timeout=0.5):
        """Block up to timeout seconds for a PDU, then dispatch it.

        smpplib's poll() uses select with timeout=0 (non-blocking), so
        it misses PDUs that arrive even milliseconds later. This method
        does a proper blocking select before calling read_once().
        """
        readable, _, _ = select.select([self.client._socket], [], [], timeout)
        if readable:
            try:
                self.client.read_once()
            except Exception:
                pass

    def _wait_for_submit_resp(self, seq, timeout=10.0):
        """Block until we get the submit_sm_resp for the given sequence."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            for i, resp in enumerate(self._submit_resps):
                if resp.sequence == seq:
                    self._submit_resps.pop(i)
                    return resp
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            self._read_available(min(remaining, 1.0))
        raise TimeoutError(f"No submit_sm_resp for sequence {seq}")

    def wait_for_deliver(self, timeout=10.0, count=1):
        """Wait for deliver_sm PDUs to arrive. Returns list of PDUs."""
        deadline = time.time() + timeout
        while len(self._deliver_events) < count:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            self._read_available(min(remaining, 0.5))
        result = self._deliver_events[:count]
        self._deliver_events = self._deliver_events[count:]
        return result

    def enquire_link(self):
        """Send enquire_link and wait for response."""
        pdu = smpplib.smpp.make_pdu("enquire_link", client=self.client)
        self.client.send_pdu(pdu)
        self._read_available(timeout=5.0)

    def unbind(self):
        try:
            self.client.unbind()
        except Exception:
            pass

    def disconnect(self):
        for timer in self._delay_timers:
            timer.cancel()
        self._delay_timers.clear()
        try:
            self.client.disconnect()
        except Exception:
            pass

    def _on_submit_resp(self, pdu, **kwargs):
        """Called by smpplib when submit_sm_resp is received."""
        logger.debug("submit_sm_resp: seq=%d message_id=%s",
                      pdu.sequence, getattr(pdu, 'message_id', None))
        self._submit_resps.append(pdu)

    def _on_deliver(self, pdu, **kwargs):
        """Called by smpplib when deliver_sm is received."""
        logger.debug("deliver_sm received: seq=%d auto_ack=%s",
                      pdu.sequence, self.auto_ack)
        self.received_pdus.append(pdu)
        self._deliver_events.append(pdu)

        if self.auto_ack is True:
            self._send_deliver_resp(pdu.sequence)
        elif self.auto_ack == "delay":
            timer = threading.Timer(
                self.deliver_ack_delay_sec,
                self._send_deliver_resp,
                args=[pdu.sequence],
            )
            self._delay_timers.append(timer)
            timer.start()
        else:
            # auto_ack=False — store for manual ACK.
            self._unacked_delivers[pdu.sequence] = pdu


@pytest.fixture
def esme():
    """Create, connect, and bind an ESME client to the gateway."""
    client = ESMEClient(GW_HOST, GW_PORT, GW_SYSTEM_ID, GW_PASSWORD)
    client.connect()
    client.bind()
    yield client
    client.unbind()
    client.disconnect()


@pytest.fixture
def esme_no_ack():
    """Create an ESME client with auto_ack=False for ACK-boundary tests."""
    client = ESMEClient(GW_HOST, GW_PORT, GW_SYSTEM_ID, GW_PASSWORD,
                        auto_ack=False)
    client.connect()
    client.bind()
    yield client
    client.unbind()
    client.disconnect()


@pytest.fixture
def esme_unbound():
    """Create and connect an ESME client WITHOUT binding."""
    client = ESMEClient(GW_HOST, GW_PORT, GW_SYSTEM_ID, GW_PASSWORD)
    client.connect()
    yield client
    client.disconnect()


@pytest.fixture
def esme_factory():
    """Factory for creating multiple ESME clients (for multi-engine tests).

    Supports auto_ack parameter for ACK-boundary tests.
    """
    clients = []

    def _make(system_id=None, password=None, auto_ack=True,
              deliver_ack_delay_sec=1.0):
        sid = system_id or f"engine-{len(clients)+1}"
        pw = password or GW_PASSWORD
        client = ESMEClient(GW_HOST, GW_PORT, sid, pw,
                            auto_ack=auto_ack,
                            deliver_ack_delay_sec=deliver_ack_delay_sec)
        client.connect()
        client.bind()
        clients.append(client)
        return client

    yield _make

    for c in clients:
        c.unbind()
        c.disconnect()
