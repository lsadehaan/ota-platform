"""Shared helpers for SMSC Gateway integration tests."""

import struct


def build_submit_sm_body(source_addr, dest_addr, short_message,
                         source_addr_ton=0x05, source_addr_npi=0x00,
                         dest_addr_ton=0x01, dest_addr_npi=0x01,
                         esm_class=0x00, protocol_id=0x00,
                         data_coding=0x00, registered_delivery=0x01,
                         priority_flag=0x00):
    """Build raw submit_sm body bytes matching SMPP 3.4 encoding.

    This produces the exact binary layout that smpplib.client.send_message()
    generates, so the returned bytes can be compared against what the
    probe SMSC captured on the southbound link.
    """
    buf = bytearray()

    # service_type (C-string, empty)
    buf.append(0x00)

    # source_addr_ton, source_addr_npi
    buf.append(source_addr_ton)
    buf.append(source_addr_npi)

    # source_addr (C-string)
    buf.extend(source_addr.encode("ascii"))
    buf.append(0x00)

    # dest_addr_ton, dest_addr_npi
    buf.append(dest_addr_ton)
    buf.append(dest_addr_npi)

    # destination_addr (C-string)
    buf.extend(dest_addr.encode("ascii"))
    buf.append(0x00)

    # esm_class
    buf.append(esm_class)

    # protocol_id
    buf.append(protocol_id)

    # priority_flag
    buf.append(priority_flag)

    # schedule_delivery_time (C-string, empty = immediate)
    buf.append(0x00)

    # validity_period (C-string, empty = SMSC default)
    buf.append(0x00)

    # registered_delivery
    buf.append(registered_delivery)

    # replace_if_present_flag
    buf.append(0x00)

    # data_coding
    buf.append(data_coding)

    # sm_default_msg_id
    buf.append(0x00)

    # sm_length + short_message
    if isinstance(short_message, str):
        short_message = short_message.encode("ascii")
    buf.append(len(short_message))
    buf.extend(short_message)

    return bytes(buf)


def parse_submit_sm_body(raw_body):
    """Parse raw submit_sm body bytes into a dict of fields.

    Returns a dict with all mandatory submit_sm fields extracted.
    """
    offset = 0
    result = {}

    # service_type
    result["service_type"], offset = _read_cstring(raw_body, offset)

    # source_addr_ton, source_addr_npi
    result["source_addr_ton"] = raw_body[offset]; offset += 1
    result["source_addr_npi"] = raw_body[offset]; offset += 1

    # source_addr
    result["source_addr"], offset = _read_cstring(raw_body, offset)

    # dest_addr_ton, dest_addr_npi
    result["dest_addr_ton"] = raw_body[offset]; offset += 1
    result["dest_addr_npi"] = raw_body[offset]; offset += 1

    # destination_addr
    result["destination_addr"], offset = _read_cstring(raw_body, offset)

    # esm_class
    result["esm_class"] = raw_body[offset]; offset += 1

    # protocol_id
    result["protocol_id"] = raw_body[offset]; offset += 1

    # priority_flag
    result["priority_flag"] = raw_body[offset]; offset += 1

    # schedule_delivery_time
    result["schedule_delivery_time"], offset = _read_cstring(raw_body, offset)

    # validity_period
    result["validity_period"], offset = _read_cstring(raw_body, offset)

    # registered_delivery
    result["registered_delivery"] = raw_body[offset]; offset += 1

    # replace_if_present_flag
    result["replace_if_present_flag"] = raw_body[offset]; offset += 1

    # data_coding
    result["data_coding"] = raw_body[offset]; offset += 1

    # sm_default_msg_id
    result["sm_default_msg_id"] = raw_body[offset]; offset += 1

    # sm_length
    sm_length = raw_body[offset]; offset += 1
    result["sm_length"] = sm_length

    # short_message
    if sm_length > 0:
        result["short_message"] = raw_body[offset:offset+sm_length]
        offset += sm_length
    else:
        result["short_message"] = b""

    # Remaining bytes are optional TLVs
    result["tlv_bytes"] = raw_body[offset:]

    return result


def assert_submit_body_equal(sent_body, captured_body, msg=""):
    """Assert that sent and captured submit_sm bodies are byte-identical.

    Provides detailed field-level diff on failure.
    """
    if sent_body == captured_body:
        return

    # Parse both for better error messages.
    try:
        sent = parse_submit_sm_body(sent_body)
        captured = parse_submit_sm_body(captured_body)
    except Exception:
        assert sent_body == captured_body, \
            f"Body mismatch (unparseable). {msg}\n" \
            f"  sent:     {sent_body.hex()}\n" \
            f"  captured: {captured_body.hex()}"
        return

    diffs = []
    for key in sent:
        if sent[key] != captured.get(key):
            diffs.append(f"  {key}: sent={sent[key]!r} captured={captured.get(key)!r}")

    diff_str = "\n".join(diffs) if diffs else "  (no field-level diff found)"
    assert sent_body == captured_body, \
        f"submit_sm body mismatch. {msg}\nField diffs:\n{diff_str}\n" \
        f"  sent_hex:     {sent_body.hex()}\n" \
        f"  captured_hex: {captured_body.hex()}"


def _read_cstring(data, offset):
    """Read a null-terminated C-string from data starting at offset."""
    end = offset
    while end < len(data) and data[end] != 0x00:
        end += 1
    s = data[offset:end].decode("ascii", errors="replace")
    if end < len(data):
        end += 1  # skip null terminator
    return s, end
