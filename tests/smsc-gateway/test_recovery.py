"""Recovery and resilience tests (C01–C08).

These tests verify gateway behavior under failure conditions:
disconnect before ACK, retry exhaustion, synthetic DLR, reconnect buffering.

ACK-boundary tests use auto_ack=False to control exactly when (or whether)
deliver_sm_resp is sent back to the gateway.
"""

import os
import time
import pytest
import requests
from conftest import ESMEClient, GW_HOST, GW_PORT, GW_PASSWORD


class TestDeliverNoACK:
    """C04: Engine receives deliver_sm but never sends deliver_sm_resp."""

    def test_c04_deliver_received_no_ack_then_reconnect(self, esme_factory):
        """C04: Gateway delivers DLR, engine does NOT ACK, then disconnects.

        The gateway should detect the missing ACK, buffer the DLR, and
        redeliver it when the engine reconnects with the same system_id.
        """
        engine = esme_factory(system_id="c04-noack", auto_ack=False)

        # Submit — DLR will come later.
        resp = engine.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27837770004",
            short_message=b"C04 no-ack test",
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        # Wait for the deliver_sm to arrive (but do NOT ACK it).
        dlrs = engine.wait_for_deliver(timeout=15, count=1)
        assert len(dlrs) >= 1, "DLR should arrive at the engine"
        assert len(engine.unacked_sequences) >= 1, "Should have unacked deliver_sm"

        # Disconnect without sending deliver_sm_resp.
        engine.disconnect_without_ack()
        time.sleep(2)

        # Reconnect with same system_id.
        engine2 = ESMEClient(GW_HOST, GW_PORT, "c04-noack", GW_PASSWORD)
        engine2.connect()
        engine2.bind()

        # The gateway should redeliver the buffered DLR.
        dlrs2 = engine2.wait_for_deliver(timeout=20, count=1)
        assert len(dlrs2) >= 1, "DLR should be redelivered after reconnect"

        dlr_body = dlrs2[0].short_message
        if isinstance(dlr_body, bytes):
            dlr_body = dlr_body.decode("ascii", errors="replace")
        assert msg_id in dlr_body, \
            f"Redelivered DLR should contain {msg_id}, got: {dlr_body}"

        engine2.unbind()
        engine2.disconnect()

    def test_c04_disconnect_before_deliver_arrives(self, esme_factory):
        """C04b: Engine disconnects before DLR even arrives at the gateway.

        Submit, then disconnect immediately. The DLR arrives while the
        engine is offline. On reconnect, the buffered DLR should drain.
        """
        engine = esme_factory(system_id="c04-early-dc")

        resp = engine.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27837770041",
            short_message=b"C04b early disconnect",
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id

        # Disconnect immediately (before DLR arrives from downstream).
        engine.disconnect()
        time.sleep(2)

        # Reconnect with same system_id.
        engine2 = ESMEClient(GW_HOST, GW_PORT, "c04-early-dc", GW_PASSWORD)
        engine2.connect()
        engine2.bind()

        dlrs = engine2.wait_for_deliver(timeout=20, count=1)
        assert len(dlrs) >= 1, "DLR should be delivered after reconnect"

        engine2.unbind()
        engine2.disconnect()


class TestMultipleSubmits:
    """Verify multiple submits and DLRs in sequence."""

    def test_multiple_submits_all_get_dlrs(self, esme):
        """Multiple submits should each get a DLR."""
        msg_ids = []
        count = 5

        for i in range(count):
            resp = esme.submit_sm(
                source_addr="GATEWAY",
                dest_addr=f"+2783000{i:04d}",
                short_message=f"Message {i}".encode(),
            )
            mid = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
            msg_ids.append(mid)
            assert mid.startswith("GW-"), f"Expected GW-* ID, got: {mid}"

        # Wait for all DLRs.
        dlrs = esme.wait_for_deliver(timeout=30, count=count)
        assert len(dlrs) >= count, f"Expected {count} DLRs, got {len(dlrs)}"


class TestDuplicateIdentity:
    """R07: Two sessions with same system_id."""

    def test_r07_duplicate_identity_replaces(self, esme_factory):
        """R07: Second connection with same system_id replaces the first."""
        engine1 = esme_factory(system_id="dup-engine")

        # Submit via engine1.
        resp1 = engine1.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27838888888",
            short_message=b"From engine1",
        )

        # Connect engine2 with same system_id — should replace engine1.
        engine2 = ESMEClient(GW_HOST, GW_PORT, "dup-engine", GW_PASSWORD)
        engine2.connect()
        engine2.bind()

        # Give the gateway time to process the replacement.
        time.sleep(1)

        # Submit via engine2 to a different MSISDN.
        resp2 = engine2.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27836666666",
            short_message=b"From engine2",
        )

        # DLR for engine2's submit should go to engine2.
        dlrs = engine2.wait_for_deliver(timeout=15, count=1)
        assert len(dlrs) >= 1, "Engine2 should receive DLR after replacing engine1"

        engine2.unbind()
        engine2.disconnect()


class TestDuplicateDLRAfterReconnect:
    """Verify gateway does not deliver the same DLR twice after reconnect."""

    def test_no_duplicate_dlr_after_acked_reconnect(self, esme_factory):
        """ACKed DLR should not be redelivered after reconnect."""
        engine = esme_factory(system_id="dedup-test")

        resp = engine.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27837770099",
            short_message=b"Dedup test",
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id

        # Wait for DLR and let auto_ack=True handle the ACK.
        dlrs = engine.wait_for_deliver(timeout=15, count=1)
        assert len(dlrs) >= 1

        # Disconnect and reconnect.
        engine.unbind()
        engine.disconnect()
        time.sleep(1)

        engine2 = ESMEClient(GW_HOST, GW_PORT, "dedup-test", GW_PASSWORD)
        engine2.connect()
        engine2.bind()

        # Should NOT receive the same DLR again (it was already ACKed).
        dlrs2 = engine2.wait_for_deliver(timeout=5, count=1)
        assert len(dlrs2) == 0, \
            "ACKed DLR should not be redelivered after reconnect"

        engine2.unbind()
        engine2.disconnect()


PROBE_API = os.environ.get("PROBE_API", "")

skip_no_probe = pytest.mark.skipif(
    not PROBE_API,
    reason="PROBE_API env var not set — probe SMSC tests skipped",
)


class TestSouthboundRetry:
    """C07: Southbound submit retry and synthetic DLR.

    Requires the probe SMSC stack (compose.probe.yml) so that submit_sm
    responses can be controlled via the probe HTTP API.
    """

    @skip_no_probe
    def test_c07_southbound_retry_synthetic_dlr(self, esme_factory):
        """C07: When southbound retries are exhausted, gateway generates
        synthetic DLR with stat:UNDELIV so the engine gets a terminal outcome.

        Flow:
        1. Set probe to reject all submit_sm
        2. Engine submits — gateway ACKs immediately (store-and-forward)
        3. Gateway tries to forward, gets rejected, enqueues submit-retry
        4. After max retries exhausted, gateway sends synthetic UNDELIV DLR
        5. Restore probe to accept mode
        """
        # Put probe into reject mode.
        resp = requests.post(
            f"{PROBE_API}/config",
            json={"mode": "reject", "status": 8},
            timeout=5,
        )
        assert resp.status_code == 200

        try:
            engine = esme_factory(system_id="c07-retry")

            # Submit — gateway ACKs immediately (store-and-forward).
            submit_resp = engine.submit_sm(
                source_addr="GATEWAY",
                dest_addr="+27837770007",
                short_message=b"C07 retry test",
            )
            msg_id = submit_resp.message_id
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode()
            assert msg_id.startswith("GW-"), f"Expected GW-* ID, got: {msg_id}"

            # Wait for synthetic UNDELIV DLR.
            # Default: max_retries=3, retry_interval=10s → worst case ~35s.
            dlrs = engine.wait_for_deliver(timeout=60, count=1)
            assert len(dlrs) >= 1, (
                "Gateway should deliver a synthetic UNDELIV DLR "
                "after southbound retries are exhausted"
            )

            dlr_body = dlrs[0].short_message
            if isinstance(dlr_body, bytes):
                dlr_body = dlr_body.decode("ascii", errors="replace")

            # Verify it's a failure DLR with the correct message ID.
            assert "UNDELIV" in dlr_body, (
                f"Synthetic DLR should contain stat:UNDELIV, got: {dlr_body}"
            )
            assert msg_id in dlr_body, (
                f"Synthetic DLR should reference {msg_id}, got: {dlr_body}"
            )

            engine.unbind()
            engine.disconnect()
        finally:
            # Restore probe to accept mode.
            requests.post(
                f"{PROBE_API}/config",
                json={"mode": "accept"},
                timeout=5,
            )
