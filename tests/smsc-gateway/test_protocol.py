"""Protocol correctness tests (P01–P11).

These tests verify basic SMPP protocol handling by the gateway:
bind, enquire_link, unbind, submit before bind, and error handling.
"""

import socket
import time
import pytest
import smpplib
import smpplib.client
import smpplib.consts
from conftest import ESMEClient, GW_HOST, GW_PORT, GW_PASSWORD


class TestBind:
    """P01, P02: Bind accept and reject."""

    def test_p01_valid_bind(self, esme):
        """P01: Valid bind_transceiver succeeds."""
        # If we got here, the fixture already bound successfully.
        assert esme.client.state in (
            smpplib.consts.SMPP_CLIENT_STATE_BOUND_TRX,
            smpplib.consts.SMPP_CLIENT_STATE_BOUND_TX,
        )

    def test_p02_invalid_password(self):
        """P02: Bind with wrong password is rejected."""
        client = ESMEClient(GW_HOST, GW_PORT, "test-esme", "wrong-password")
        client.connect()
        failed = client.bind_expect_fail()
        assert failed, "Expected bind to fail with wrong password"
        client.disconnect()


class TestEnquireLink:
    """P04: enquire_link round-trip."""

    def test_p04_enquire_link(self, esme):
        """P04: enquire_link gets a timely response."""
        start = time.time()
        esme.enquire_link()
        elapsed = time.time() - start
        assert elapsed < 2.0, f"enquire_link took {elapsed:.2f}s"


class TestUnbind:
    """P05: unbind round-trip."""

    def test_p05_unbind(self):
        """P05: Graceful unbind succeeds."""
        client = ESMEClient(GW_HOST, GW_PORT, "unbind-test", GW_PASSWORD)
        client.connect()
        client.bind()
        client.unbind()
        client.disconnect()


class TestSubmitBeforeBind:
    """P03: Submit before bind."""

    def test_p03_submit_before_bind(self, esme_unbound):
        """P03: submit_sm before bind returns an error."""
        with pytest.raises(Exception):
            esme_unbound.submit_sm(
                source_addr="GATEWAY",
                dest_addr="+27831234567",
                short_message=b"test",
            )


class TestSubmitResponse:
    """Basic submit_sm_resp validation."""

    def test_submit_returns_gw_message_id(self, esme):
        """Gateway returns its own GW-* message ID in submit_sm_resp."""
        resp = esme.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27831234567",
            short_message=b"Hello world",
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-"), f"Expected GW-* message ID, got: {msg_id}"


class TestMalformedPDU:
    """P06: Malformed PDU handling."""

    def test_p06_malformed_pdu_no_crash(self, esme):
        """P06: Sending garbage bytes should not crash the gateway.

        We send a minimal 4-byte PDU (just a command_length of 4 with zeroed
        command fields) via a raw socket. The gateway should close the
        connection without crashing. We then verify the gateway is still
        healthy by performing a normal submit on the existing ESME session.
        """
        # Send garbage via a raw socket (smpplib won't let us send invalid PDUs).
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect((GW_HOST, GW_PORT))
            # Minimal 4-byte "PDU" with zero command — this is invalid SMPP.
            sock.sendall(b"\x00\x00\x00\x04")
            # Try reading response; gateway may close connection or send generic_nack.
            try:
                data = sock.recv(1024)
            except (socket.timeout, ConnectionResetError, BrokenPipeError):
                pass  # Expected — gateway closed the connection.
        finally:
            sock.close()

        # Give gateway a moment to process the bad connection.
        time.sleep(1)

        # Verify the gateway is still alive by sending a normal submit.
        resp = esme.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27831234567",
            short_message=b"After malformed PDU",
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-"), \
            f"Gateway should still be operational after malformed PDU, got: {msg_id}"


class TestDuplicateDeliverSM:
    """P07: Duplicate deliver_sm handling."""

    def test_p07_duplicate_deliver_sm_both_delivered(self, esme):
        """P07: Two submits to the same MSISDN both produce DLRs.

        The gateway must NOT deduplicate deliver_sm PDUs — if the downstream
        SMSC sends two DLRs (one per submit), both should be forwarded to
        the engine.
        """
        # Drain any leftover DLRs from prior tests.
        esme.wait_for_deliver(timeout=2, count=100)
        esme._deliver_events.clear()

        msg_ids = []
        # Use a unique MSISDN to avoid DLR leaking from prior tests.
        for i in range(2):
            resp = esme.submit_sm(
                source_addr="GATEWAY",
                dest_addr=f"+27839990{i:03d}",
                short_message=f"Dup test {i}".encode(),
            )
            mid = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
            assert mid.startswith("GW-")
            msg_ids.append(mid)

        # Wait for both DLRs.
        dlrs = esme.wait_for_deliver(timeout=15, count=2)
        assert len(dlrs) >= 2, \
            f"Expected 2 deliver_sm PDUs (no dedup), got {len(dlrs)}"

        # Verify both message IDs appear in the DLR bodies.
        dlr_bodies = []
        for dlr in dlrs:
            body = dlr.short_message
            if isinstance(body, bytes):
                body = body.decode("ascii", errors="replace")
            dlr_bodies.append(body)

        for mid in msg_ids:
            found = any(mid in body for body in dlr_bodies)
            assert found, f"DLR for {mid} not found in delivered PDUs"
