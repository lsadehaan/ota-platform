"""Chaos / fault-injection tests (X01–X04).

These tests use Toxiproxy to inject network faults between the gateway
and the downstream SMSC. They are skipped unless the TOXIPROXY_API
environment variable is set to the Toxiproxy HTTP API endpoint
(e.g. "http://127.0.0.1:8474").

Topology with Toxiproxy:
    pytest (ESME) --> smsc-gateway:2776 --> toxiproxy:2775 --> smppsim:12775

Toxiproxy proxy name: "downstream-smsc" (must be pre-configured).
"""

import os
import time
import pytest
import requests

from conftest import ESMEClient, GW_HOST, GW_PORT, GW_SYSTEM_ID, GW_PASSWORD

TOXIPROXY_API = os.environ.get("TOXIPROXY_API", "")
PROXY_NAME = "downstream-smsc"

skip_no_toxiproxy = pytest.mark.skipif(
    not TOXIPROXY_API,
    reason="TOXIPROXY_API env var not set — Toxiproxy tests skipped",
)


def add_toxic(name, toxic_type, attributes, stream="downstream"):
    """Add a toxic to the downstream-smsc proxy."""
    resp = requests.post(
        f"{TOXIPROXY_API}/proxies/{PROXY_NAME}/toxics",
        json={
            "name": name,
            "type": toxic_type,
            "stream": stream,
            "attributes": attributes,
        },
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()


def remove_toxic(name):
    """Remove a toxic from the downstream-smsc proxy."""
    resp = requests.delete(
        f"{TOXIPROXY_API}/proxies/{PROXY_NAME}/toxics/{name}",
        timeout=5,
    )
    resp.raise_for_status()


def reset_proxy():
    """Reset the downstream-smsc proxy by removing all toxics."""
    try:
        resp = requests.get(
            f"{TOXIPROXY_API}/proxies/{PROXY_NAME}",
            timeout=5,
        )
        resp.raise_for_status()
        proxy_info = resp.json()
        for toxic_name in proxy_info.get("toxics", []):
            name = toxic_name if isinstance(toxic_name, str) else toxic_name.get("name", "")
            if name:
                try:
                    remove_toxic(name)
                except Exception:
                    pass
    except Exception:
        pass


@skip_no_toxiproxy
class TestSouthboundLatency:
    """X01: Southbound latency injection."""

    def test_x01_500ms_latency_submit_succeeds(self, esme):
        """X01: Add 500ms southbound latency — submit should still succeed."""
        toxic_name = "x01-latency"
        try:
            add_toxic(toxic_name, "latency", {"latency": 500, "jitter": 50})

            resp = esme.submit_sm(
                source_addr="GATEWAY",
                dest_addr="+27831234567",
                short_message=b"Latency test X01",
            )
            msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
            assert msg_id.startswith("GW-"), f"Expected GW-* message ID, got: {msg_id}"

            dlrs = esme.wait_for_deliver(timeout=20, count=1)
            assert len(dlrs) >= 1, "DLR should arrive despite 500ms latency"
        finally:
            try:
                remove_toxic(toxic_name)
            except Exception:
                pass


@skip_no_toxiproxy
class TestSouthboundReset:
    """X03: Southbound connection reset mid-submit."""

    def test_x03_reset_southbound_no_deadlock(self, esme):
        """X03: Reset southbound connection mid-submit — verify no deadlock.

        After the toxic is removed and the gateway reconnects downstream,
        a subsequent submit should succeed.
        """
        toxic_name = "x03-reset"
        try:
            add_toxic(toxic_name, "reset_peer", {"timeout": 100})

            try:
                esme.submit_sm(
                    source_addr="GATEWAY",
                    dest_addr="+27831234567",
                    short_message=b"Reset test X03",
                )
            except Exception:
                pass  # Expected — connection was reset.
        finally:
            try:
                remove_toxic(toxic_name)
            except Exception:
                pass

        time.sleep(5)

        engine2 = ESMEClient(GW_HOST, GW_PORT, "x03-recover", GW_PASSWORD)
        try:
            engine2.connect()
            engine2.bind()

            resp = engine2.submit_sm(
                source_addr="GATEWAY",
                dest_addr="+27831234567",
                short_message=b"After reset X03",
            )
            msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
            assert msg_id.startswith("GW-"), \
                f"Gateway should recover after southbound reset, got: {msg_id}"

            dlrs = engine2.wait_for_deliver(timeout=15, count=1)
            assert len(dlrs) >= 1, "DLR should arrive after gateway reconnects downstream"
        finally:
            engine2.unbind()
            engine2.disconnect()


@skip_no_toxiproxy
class TestNorthboundResetDuringDLR:
    """X04: Northbound connection loss while deliver_sm is unACKed."""

    def test_x04_deliver_received_no_ack_tcp_reset(self, esme_factory):
        """X04: Engine receives deliver_sm, does NOT ACK, connection drops.

        Uses auto_ack=False so the engine holds the deliver_sm without
        sending deliver_sm_resp. Then disconnect_without_ack() simulates
        an abrupt TCP reset. The gateway should buffer the unACKed DLR
        and redeliver it when the engine reconnects.
        """
        engine = esme_factory(system_id="x04-engine", auto_ack=False)

        # Submit a message.
        resp = engine.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27839876543",
            short_message=b"DLR buffer test X04",
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        # Wait for the deliver_sm to arrive (but do NOT ACK).
        dlrs = engine.wait_for_deliver(timeout=15, count=1)
        assert len(dlrs) >= 1, "deliver_sm should arrive"
        assert len(engine.unacked_sequences) >= 1, \
            "Should have unacked deliver_sm"

        # Abrupt TCP reset — no deliver_sm_resp sent.
        engine.disconnect_without_ack()
        time.sleep(3)

        # Reconnect with the same system_id.
        engine2 = ESMEClient(GW_HOST, GW_PORT, "x04-engine", GW_PASSWORD)
        try:
            engine2.connect()
            engine2.bind()

            # Buffered DLR should be redelivered.
            dlrs2 = engine2.wait_for_deliver(timeout=20, count=1)
            assert len(dlrs2) >= 1, \
                "DLR should be buffered and redelivered after northbound reconnect"

            dlr_body = dlrs2[0].short_message
            if isinstance(dlr_body, bytes):
                dlr_body = dlr_body.decode("ascii", errors="replace")
            assert msg_id in dlr_body, \
                f"Buffered DLR should contain {msg_id}, got: {dlr_body}"
        finally:
            engine2.unbind()
            engine2.disconnect()
