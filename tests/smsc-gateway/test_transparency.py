"""PDU transparency tests (T01–T11).

These tests verify that the gateway forwards submit_sm bodies byte-for-byte
to the downstream SMSC. The probe SMSC captures raw submit_sm body bytes on
the southbound link. Tests build the expected body using the same SMPP 3.4
encoding and compare byte-for-byte.

Requires the probe SMSC stack:
    docker compose -f compose.base.yml -f compose.probe.yml up -d

Set PROBE_API to enable byte-level assertions:
    PROBE_API=http://127.0.0.1:8085 GW_HOST=127.0.0.1 GW_PORT=2776 pytest -v

Without PROBE_API, tests fall back to field-level checks (submit succeeds,
returns GW-* message ID).
"""

import os
import time
import pytest
import requests

from conftest import ESMEClient, GW_HOST, GW_PORT, GW_PASSWORD
from helpers import build_submit_sm_body, parse_submit_sm_body, assert_submit_body_equal

PROBE_API = os.environ.get("PROBE_API", "")


def clear_captures():
    """Clear all captures on the probe SMSC."""
    requests.post(f"{PROBE_API}/captures/clear", timeout=5)


def get_latest_capture():
    """Get the most recent capture from the probe SMSC."""
    resp = requests.get(f"{PROBE_API}/captures", timeout=5)
    resp.raise_for_status()
    captures = resp.json()
    if not captures:
        return None
    return captures[-1]


def trigger_dlr(message_id, status="DELIVRD"):
    """Trigger a DLR on the probe SMSC."""
    resp = requests.post(f"{PROBE_API}/dlr", json={
        "message_id": message_id,
        "status": status,
    }, timeout=5)
    resp.raise_for_status()


def get_captured_body_bytes(capture):
    """Extract raw body bytes from a probe capture."""
    return bytes.fromhex(capture["raw_body"])


def submit_and_capture(esme, source_addr, dest_addr, short_message, **kwargs):
    """Submit a message, retrieve the probe capture, return (resp, capture)."""
    if PROBE_API:
        clear_captures()

    resp = esme.submit_sm(
        source_addr=source_addr,
        dest_addr=dest_addr,
        short_message=short_message,
        **kwargs,
    )

    capture = None
    if PROBE_API:
        time.sleep(0.3)  # Give probe time to record
        capture = get_latest_capture()

    return resp, capture


class TestTransparency:
    """Verify submit_sm bodies pass through the gateway byte-for-byte."""

    def test_t01_gsm7_single_part(self, esme):
        """T01: GSM 7-bit single-part — byte-level transparency."""
        payload = b"Hello world, this is a GSM 7-bit test message."
        resp, capture = submit_and_capture(
            esme,
            source_addr="GATEWAY",
            dest_addr="+27830101001",
            short_message=payload,
            data_coding=0x00,
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        if capture:
            expected = build_submit_sm_body(
                "GATEWAY", "+27830101001", payload,
                data_coding=0x00,
            )
            captured = get_captured_body_bytes(capture)
            assert_submit_body_equal(expected, captured, "T01 GSM-7")

    def test_t02_ucs2_single_part(self, esme):
        """T02: UCS-2 single-part — byte-level transparency."""
        ucs2_payload = b"\x00\x48\x00\x65\x00\x6C\x00\x6C\x00\x6F"
        resp, capture = submit_and_capture(
            esme,
            source_addr="GATEWAY",
            dest_addr="+27830102001",
            short_message=ucs2_payload,
            data_coding=0x08,
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        if capture:
            expected = build_submit_sm_body(
                "GATEWAY", "+27830102001", ucs2_payload,
                data_coding=0x08,
            )
            captured = get_captured_body_bytes(capture)
            assert_submit_body_equal(expected, captured, "T02 UCS-2")

    def test_t03_binary_ota_payload(self, esme):
        """T03: Binary OTA packet — byte-level transparency."""
        binary_payload = bytes(range(0, 140))
        resp, capture = submit_and_capture(
            esme,
            source_addr="GATEWAY",
            dest_addr="+27830103001",
            short_message=binary_payload,
            data_coding=0xF6,
            esm_class=0x40,
            protocol_id=0x7F,
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        if capture:
            expected = build_submit_sm_body(
                "GATEWAY", "+27830103001", binary_payload,
                data_coding=0xF6, esm_class=0x40, protocol_id=0x7F,
            )
            captured = get_captured_body_bytes(capture)
            assert_submit_body_equal(expected, captured, "T03 binary OTA")

    def test_t04_multipart_udh_submit(self, esme):
        """T04: Multipart UDH — byte-level transparency."""
        udh = b"\x05\x00\x03\x01\x02\x01"
        payload = b"This is part 1 of a multipart message for transparency test."
        short_message = udh + payload

        resp, capture = submit_and_capture(
            esme,
            source_addr="GATEWAY",
            dest_addr="+27830104001",
            short_message=short_message,
            esm_class=0x40,
            data_coding=0x00,
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        if capture:
            expected = build_submit_sm_body(
                "GATEWAY", "+27830104001", short_message,
                esm_class=0x40, data_coding=0x00,
            )
            captured = get_captured_body_bytes(capture)
            assert_submit_body_equal(expected, captured, "T04 multipart UDH")

    def test_t06_nondefault_source_ton_npi(self, esme):
        """T06: Non-default source TON/NPI — byte-level transparency."""
        resp, capture = submit_and_capture(
            esme,
            source_addr="SHORTCODE",
            dest_addr="+27830106001",
            short_message=b"test",
            source_addr_ton=0x03,
            source_addr_npi=0x08,
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        if capture:
            expected = build_submit_sm_body(
                "SHORTCODE", "+27830106001", b"test",
                source_addr_ton=0x03, source_addr_npi=0x08,
            )
            captured = get_captured_body_bytes(capture)
            assert_submit_body_equal(expected, captured, "T06 source TON/NPI")

    def test_t07_nondefault_dest_ton_npi(self, esme):
        """T07: Non-default dest TON/NPI — byte-level transparency."""
        resp, capture = submit_and_capture(
            esme,
            source_addr="GATEWAY",
            dest_addr="27830107001",
            short_message=b"test",
            dest_addr_ton=0x02,
            dest_addr_npi=0x08,
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        if capture:
            expected = build_submit_sm_body(
                "GATEWAY", "27830107001", b"test",
                dest_addr_ton=0x02, dest_addr_npi=0x08,
            )
            captured = get_captured_body_bytes(capture)
            assert_submit_body_equal(expected, captured, "T07 dest TON/NPI")

    def test_t08_nondefault_pid(self, esme):
        """T08: Non-default PID — byte-level transparency."""
        resp, capture = submit_and_capture(
            esme,
            source_addr="GATEWAY",
            dest_addr="+27830108001",
            short_message=b"test",
            protocol_id=0x7F,
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        if capture:
            expected = build_submit_sm_body(
                "GATEWAY", "+27830108001", b"test",
                protocol_id=0x7F,
            )
            captured = get_captured_body_bytes(capture)
            assert_submit_body_equal(expected, captured, "T08 protocol_id")

    def test_t09_nondefault_dcs(self, esme):
        """T09: Non-default DCS — byte-level transparency."""
        payload = b"\x00\x48\x00\x69"
        resp, capture = submit_and_capture(
            esme,
            source_addr="GATEWAY",
            dest_addr="+27830109001",
            short_message=payload,
            data_coding=0x08,
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        if capture:
            expected = build_submit_sm_body(
                "GATEWAY", "+27830109001", payload,
                data_coding=0x08,
            )
            captured = get_captured_body_bytes(capture)
            assert_submit_body_equal(expected, captured, "T09 data_coding")

    def test_t11_registered_delivery_none(self, esme):
        """T11a: registered_delivery=0x00 — byte-level transparency."""
        resp, capture = submit_and_capture(
            esme,
            source_addr="GATEWAY",
            dest_addr="+27830111001",
            short_message=b"no DLR requested",
            registered_delivery=0x00,
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        if capture:
            expected = build_submit_sm_body(
                "GATEWAY", "+27830111001", b"no DLR requested",
                registered_delivery=0x00,
            )
            captured = get_captured_body_bytes(capture)
            assert_submit_body_equal(expected, captured, "T11a registered_delivery=0")

    def test_t11_registered_delivery_dlr(self, esme):
        """T11b: registered_delivery=0x01 — byte-level transparency."""
        resp, capture = submit_and_capture(
            esme,
            source_addr="GATEWAY",
            dest_addr="+27830111002",
            short_message=b"DLR requested",
            registered_delivery=0x01,
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        if capture:
            expected = build_submit_sm_body(
                "GATEWAY", "+27830111002", b"DLR requested",
                registered_delivery=0x01,
            )
            captured = get_captured_body_bytes(capture)
            assert_submit_body_equal(expected, captured, "T11b registered_delivery=1")


class TestTransparencyDLR:
    """Verify DLR routing works with probe-triggered DLRs."""

    @pytest.mark.skipif(not PROBE_API, reason="Requires PROBE_API")
    def test_probe_dlr_delivery(self, esme):
        """Submit via gateway, trigger DLR via probe API, verify delivery."""
        clear_captures()

        resp = esme.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27830200001",
            short_message=b"Probe DLR test",
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        time.sleep(0.3)
        capture = get_latest_capture()
        assert capture is not None, "Probe should have captured the submit_sm"

        # Trigger DLR via probe API.
        trigger_dlr(capture["message_id"])

        # Wait for DLR to arrive via gateway.
        dlrs = esme.wait_for_deliver(timeout=10, count=1)
        assert len(dlrs) >= 1, "DLR should arrive after probe trigger"

        dlr_body = dlrs[0].short_message
        if isinstance(dlr_body, bytes):
            dlr_body = dlr_body.decode("ascii", errors="replace")
        # DLR should contain the gateway message ID, not the probe message ID.
        assert msg_id in dlr_body, \
            f"DLR should contain gateway ID {msg_id}, got: {dlr_body}"


class TestTransparencyMO:
    """Verify MO routing works with probe-triggered MO deliver_sm."""

    @pytest.mark.skipif(not PROBE_API, reason="Requires PROBE_API")
    def test_probe_mo_delivery(self, esme):
        """Establish affinity, trigger MO via probe API, verify delivery."""
        clear_captures()

        # Submit to establish MSISDN affinity.
        resp = esme.submit_sm(
            source_addr="GATEWAY",
            dest_addr="+27830300001",
            short_message=b"Establish MO affinity",
        )
        msg_id = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
        assert msg_id.startswith("GW-")

        time.sleep(0.3)
        capture = get_latest_capture()
        assert capture is not None

        # Trigger DLR first (so it doesn't interfere with MO wait).
        trigger_dlr(capture["message_id"])
        dlrs = esme.wait_for_deliver(timeout=10, count=1)
        assert len(dlrs) >= 1

        # Trigger MO from the same MSISDN.
        mo_text = "MO reply from handset"
        mo_resp = requests.post(f"{PROBE_API}/mo", json={
            "source_addr": "+27830300001",
            "dest_addr": "GATEWAY",
            "text": mo_text,
        }, timeout=5)
        mo_resp.raise_for_status()

        # Wait for MO deliver_sm via gateway.
        mos = esme.wait_for_deliver(timeout=10, count=1)
        assert len(mos) >= 1, "MO should arrive after probe trigger"

        mo_body = mos[0].short_message
        if isinstance(mo_body, bytes):
            mo_body = mo_body.decode("ascii", errors="replace")
        assert mo_text in mo_body, \
            f"MO body should contain '{mo_text}', got: {mo_body}"
