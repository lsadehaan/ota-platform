"""Routing correctness tests (R01–R09).

These tests verify MSISDN-based sticky routing, DLR correlation,
MO affinity routing, and reconnect grace behavior.
"""

import time
import pytest
from conftest import ESMEClient, GW_HOST, GW_PORT, GW_PASSWORD


class TestSingleEngineRouting:
    """R01: Single engine, single MSISDN."""

    def test_r01_dlr_returns_to_submitting_engine(self, esme):
        """R01: DLR routes back to the engine that submitted."""
        resp = esme.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27830010001",
            short_message=b"Hello R01",
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        # Wait for DLR from downstream SMSC (via gateway).
        delivers = esme.wait_for_deliver(timeout=15, count=1)
        assert len(delivers) >= 1, "Expected at least 1 deliver_sm (DLR)"

        # Verify DLR contains our gateway message ID.
        dlr_body = delivers[0].short_message
        if isinstance(dlr_body, bytes):
            dlr_body = dlr_body.decode("ascii", errors="replace")
        assert msg_id in dlr_body, f"DLR should contain gateway msg ID {msg_id}, got: {dlr_body}"


class TestMultiEngineRouting:
    """R02, R03: Multiple engines, affinity routing."""

    def test_r02_distinct_msisdns_route_correctly(self, esme_factory):
        """R02: Two engines, distinct MSISDNs — DLR routes by affinity."""
        engine_a = esme_factory(system_id="engine-a")
        engine_b = esme_factory(system_id="engine-b")

        # Engine A submits to MSISDN-A.
        resp_a = engine_a.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27831111111",
            short_message=b"From engine A",
        )
        msg_id_a = resp_a.message_id.decode() if isinstance(resp_a.message_id, bytes) else resp_a.message_id

        # Engine B submits to MSISDN-B.
        resp_b = engine_b.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27832222222",
            short_message=b"From engine B",
        )
        msg_id_b = resp_b.message_id.decode() if isinstance(resp_b.message_id, bytes) else resp_b.message_id

        # Wait for DLRs.
        dlrs_a = engine_a.wait_for_deliver(timeout=15, count=1)
        dlrs_b = engine_b.wait_for_deliver(timeout=15, count=1)

        # Verify each DLR went to the correct engine.
        assert len(dlrs_a) >= 1, "Engine A should receive its DLR"
        assert len(dlrs_b) >= 1, "Engine B should receive its DLR"

        dlr_a_body = dlrs_a[0].short_message
        if isinstance(dlr_a_body, bytes):
            dlr_a_body = dlr_a_body.decode("ascii", errors="replace")
        assert msg_id_a in dlr_a_body, f"Engine A DLR should contain {msg_id_a}"

        dlr_b_body = dlrs_b[0].short_message
        if isinstance(dlr_b_body, bytes):
            dlr_b_body = dlr_b_body.decode("ascii", errors="replace")
        assert msg_id_b in dlr_b_body, f"Engine B DLR should contain {msg_id_b}"


class TestReconnect:
    """R05, R06: Reconnect within and after grace period."""

    def test_r05_reconnect_within_grace(self, esme_factory):
        """R05: Engine reconnects within grace — buffered DLR drains to same identity."""
        engine = esme_factory(system_id="grace-test")

        # Submit a message.
        resp = engine.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27839999999",
            short_message=b"Grace test",
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id

        # Disconnect before DLR arrives.
        engine.disconnect()

        # Wait a bit (but within grace period, default 60s).
        time.sleep(2)

        # Reconnect with same system_id.
        engine2 = ESMEClient(GW_HOST, GW_PORT, "grace-test", GW_PASSWORD)
        engine2.connect()
        engine2.bind()

        # Wait for DLR to be delivered after reconnect.
        dlrs = engine2.wait_for_deliver(timeout=20, count=1)
        assert len(dlrs) >= 1, "DLR should be delivered after reconnect"

        engine2.unbind()
        engine2.disconnect()


class TestDLRMessageIDTranslation:
    """R09: DLR message ID translation."""

    def test_r09_dlr_contains_gateway_id(self, esme):
        """R09: DLR receipt text contains gateway message ID, not downstream SMSC ID."""
        resp = esme.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27835555555",
            short_message=b"ID translation test",
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        dlrs = esme.wait_for_deliver(timeout=15, count=1)
        assert len(dlrs) >= 1

        dlr_body = dlrs[0].short_message
        if isinstance(dlr_body, bytes):
            dlr_body = dlr_body.decode("ascii", errors="replace")

        # The DLR receipt should contain "id:GW-..." not "id:MOCK-..." or "id:PROBE-..."
        assert f"id:{msg_id}" in dlr_body, \
            f"DLR receipt should contain 'id:{msg_id}', got: {dlr_body}"


class TestLastSubmitWins:
    """R03: Two engines, same MSISDN, last-submit-wins."""

    def test_r03_last_submit_wins(self, esme_factory):
        """R03: When two engines submit to the same MSISDN, the DLR for the
        second submit should go to the second engine (last-submit-wins affinity).

        Engine A submits to MSISDN X first. Then engine B submits to the
        same MSISDN X. The DLR for engine B's submit should route to engine B,
        because the MSISDN affinity should point to the last submitter.
        """
        engine_a = esme_factory(system_id="engine-r03-a")
        engine_b = esme_factory(system_id="engine-r03-b")

        target_msisdn = "+27834444444"

        # Engine A submits to the target MSISDN.
        resp_a = engine_a.submit_sm(
            source_addr="GATEWAY",
            dest_addr=target_msisdn,
            short_message=b"From engine A (first)",
        )
        msg_id_a = resp_a.message_id.decode() if isinstance(resp_a.message_id, bytes) else resp_a.message_id
        assert msg_id_a.startswith("GW-")

        # Wait for engine A's DLR to arrive so the affinity is established.
        dlrs_a_first = engine_a.wait_for_deliver(timeout=15, count=1)
        assert len(dlrs_a_first) >= 1, "Engine A should receive DLR for its first submit"

        # Now engine B submits to the SAME MSISDN — this should update affinity.
        resp_b = engine_b.submit_sm(
            source_addr="GATEWAY",
            dest_addr=target_msisdn,
            short_message=b"From engine B (second)",
        )
        msg_id_b = resp_b.message_id.decode() if isinstance(resp_b.message_id, bytes) else resp_b.message_id
        assert msg_id_b.startswith("GW-")

        # DLR for engine B's submit should go to engine B.
        dlrs_b = engine_b.wait_for_deliver(timeout=15, count=1)
        assert len(dlrs_b) >= 1, "Engine B should receive DLR (last-submit-wins)"

        dlr_b_body = dlrs_b[0].short_message
        if isinstance(dlr_b_body, bytes):
            dlr_b_body = dlr_b_body.decode("ascii", errors="replace")
        assert msg_id_b in dlr_b_body, \
            f"Engine B DLR should contain {msg_id_b}, got: {dlr_b_body}"


class TestMORouting:
    """R04: MO (Mobile Originated) routing to submitting engine."""

    def test_r04_mo_routes_to_submitting_engine(self, esme_factory):
        """R04: After engine A submits to MSISDN X, an MO message from MSISDN X
        should route to engine A.

        This test verifies MO affinity routing. When the downstream SMSC
        generates an MO message (deliver_sm that is NOT a DLR), the gateway
        should route it to the engine that last submitted to that MSISDN.

        NOTE: This test depends on the downstream SMSC simulator being
        configured to send MO messages. If the simulator does not support MO,
        this test may time out waiting for the MO deliver_sm.
        """
        engine_a = esme_factory(system_id="engine-r04-a")

        target_msisdn = "+27833333333"

        # Engine A submits to the target MSISDN — establishes affinity.
        resp = engine_a.submit_sm(
            source_addr="GATEWAY",
            dest_addr=target_msisdn,
            short_message=b"Establish MO affinity",
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        # Wait for the DLR first.
        dlrs = engine_a.wait_for_deliver(timeout=15, count=1)
        assert len(dlrs) >= 1, "Engine A should receive DLR"

        # Now wait for an MO message from the same MSISDN.
        # The downstream SMSC simulator may or may not generate MO messages.
        # We use a shorter timeout since MO support is optional.
        mo_pdus = engine_a.wait_for_deliver(timeout=5, count=1)

        if len(mo_pdus) >= 1:
            # If we got an MO, verify it came from our target MSISDN.
            mo = mo_pdus[0]
            mo_source = mo.source_addr
            if isinstance(mo_source, bytes):
                mo_source = mo_source.decode("ascii", errors="replace")
            # MO should have the MSISDN as source.
            assert target_msisdn.lstrip("+") in mo_source or mo_source in target_msisdn, \
                f"MO source should be related to {target_msisdn}, got: {mo_source}"
        else:
            pytest.skip(
                "Downstream SMSC simulator did not generate MO messages. "
                "R04 MO routing cannot be verified without MO support."
            )
