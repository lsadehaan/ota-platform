package smpp

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// SMPP command IDs.
const (
	CmdBindTransceiver     uint32 = 0x00000009
	CmdBindTransceiverResp uint32 = 0x80000009
	CmdSubmitSM            uint32 = 0x00000004
	CmdSubmitSMResp        uint32 = 0x80000004
	CmdDeliverSM           uint32 = 0x00000005
	CmdDeliverSMResp       uint32 = 0x80000005
	CmdEnquireLink         uint32 = 0x00000015
	CmdEnquireLinkResp     uint32 = 0x80000015
	CmdUnbind              uint32 = 0x00000006
	CmdUnbindResp          uint32 = 0x80000006
	CmdGenericNack         uint32 = 0x80000000
)

// SMPP status codes.
const (
	StatusOK          uint32 = 0x00000000
	StatusInvMsgLen   uint32 = 0x00000001
	StatusInvCmdLen   uint32 = 0x00000002
	StatusInvCmdID    uint32 = 0x00000003
	StatusInvBnd      uint32 = 0x00000004
	StatusSysErr      uint32 = 0x00000008
	StatusThrottled   uint32 = 0x00000058
	StatusMsgQFull    uint32 = 0x00000014
	StatusSubmitFail  uint32 = 0x00000045
)

// PDU header length in bytes.
const pduHeaderLen = 16

// PDU represents an SMPP Protocol Data Unit.
type PDU struct {
	CommandLength  uint32
	CommandID      uint32
	CommandStatus  uint32
	SequenceNumber uint32
	Body           []byte
}

// EncodePDU serialises a PDU into a byte slice ready for transmission.
// It sets CommandLength automatically based on the header size plus the body.
func EncodePDU(pdu *PDU) []byte {
	pdu.CommandLength = uint32(pduHeaderLen + len(pdu.Body))
	buf := make([]byte, pdu.CommandLength)
	binary.BigEndian.PutUint32(buf[0:4], pdu.CommandLength)
	binary.BigEndian.PutUint32(buf[4:8], pdu.CommandID)
	binary.BigEndian.PutUint32(buf[8:12], pdu.CommandStatus)
	binary.BigEndian.PutUint32(buf[12:16], pdu.SequenceNumber)
	copy(buf[16:], pdu.Body)
	return buf
}

// DecodePDU reads a single PDU from data. data must contain at least the full
// PDU (command_length bytes). Returns the parsed PDU or an error.
func DecodePDU(data []byte) (*PDU, error) {
	if len(data) < pduHeaderLen {
		return nil, fmt.Errorf("data too short for PDU header: got %d bytes, need %d", len(data), pduHeaderLen)
	}
	cmdLen := binary.BigEndian.Uint32(data[0:4])
	if uint32(len(data)) < cmdLen {
		return nil, fmt.Errorf("data too short for PDU: got %d bytes, header says %d", len(data), cmdLen)
	}
	pdu := &PDU{
		CommandLength:  cmdLen,
		CommandID:      binary.BigEndian.Uint32(data[4:8]),
		CommandStatus:  binary.BigEndian.Uint32(data[8:12]),
		SequenceNumber: binary.BigEndian.Uint32(data[12:16]),
	}
	if cmdLen > pduHeaderLen {
		pdu.Body = make([]byte, cmdLen-pduHeaderLen)
		copy(pdu.Body, data[pduHeaderLen:cmdLen])
	}
	return pdu, nil
}

// writeCString writes a null-terminated C-string to the buffer.
func writeCString(buf *bytes.Buffer, s string) {
	buf.WriteString(s)
	buf.WriteByte(0x00)
}

// readCString reads a null-terminated C-string from data starting at offset.
// Returns the string and the offset just past the null terminator.
func readCString(data []byte, offset int) (string, int) {
	end := offset
	for end < len(data) && data[end] != 0x00 {
		end++
	}
	s := string(data[offset:end])
	if end < len(data) {
		end++ // skip null terminator
	}
	return s, end
}

// EncodeBindTransceiver builds the body of a bind_transceiver PDU.
func EncodeBindTransceiver(systemID, password, systemType string) []byte {
	var buf bytes.Buffer
	writeCString(&buf, systemID)       // system_id
	writeCString(&buf, password)       // password
	writeCString(&buf, systemType)     // system_type
	buf.WriteByte(0x34)                // interface_version = 3.4
	buf.WriteByte(0x00)                // addr_ton
	buf.WriteByte(0x00)                // addr_npi
	writeCString(&buf, "")             // address_range
	return buf.Bytes()
}

// EncodeSubmitSM builds the body of a submit_sm PDU with OTA-appropriate fields.
func EncodeSubmitSM(sourceAddr string, sourceTON, sourceNPI byte,
	destAddr string, destTON, destNPI byte,
	esmClass, protocolID, dataCoding byte,
	registeredDelivery byte,
	shortMessage []byte) []byte {

	var buf bytes.Buffer

	writeCString(&buf, "")    // service_type (default)
	buf.WriteByte(sourceTON)  // source_addr_ton
	buf.WriteByte(sourceNPI)  // source_addr_npi
	writeCString(&buf, sourceAddr) // source_addr
	buf.WriteByte(destTON)    // dest_addr_ton
	buf.WriteByte(destNPI)    // dest_addr_npi
	writeCString(&buf, destAddr) // destination_addr
	buf.WriteByte(esmClass)   // esm_class
	buf.WriteByte(protocolID) // protocol_id
	buf.WriteByte(0x00)       // priority_flag
	writeCString(&buf, "")    // schedule_delivery_time (immediate)
	writeCString(&buf, "")    // validity_period (SMSC default)
	buf.WriteByte(registeredDelivery) // registered_delivery
	buf.WriteByte(0x00)       // replace_if_present_flag
	buf.WriteByte(dataCoding) // data_coding
	buf.WriteByte(0x00)       // sm_default_msg_id
	// sm_length + short_message
	if len(shortMessage) > 254 {
		buf.WriteByte(0x00) // sm_length = 0 when using message_payload TLV
		// Append message_payload TLV (tag=0x0424)
		binary.Write(&buf, binary.BigEndian, uint16(0x0424)) // TLV tag
		binary.Write(&buf, binary.BigEndian, uint16(len(shortMessage))) // TLV length
		buf.Write(shortMessage)
	} else {
		buf.WriteByte(byte(len(shortMessage)))
		buf.Write(shortMessage)
	}

	return buf.Bytes()
}

// ParseSubmitSMResp extracts the message_id from a submit_sm_resp body.
func ParseSubmitSMResp(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	msgID, _ := readCString(body, 0)
	return msgID
}

// ParseDeliverSM extracts fields from a deliver_sm PDU body.
// Returns the source address, destination address, ESM class, and short message payload.
func ParseDeliverSM(body []byte) (sourceAddr string, destAddr string, esmClass byte, shortMessage []byte) {
	if len(body) == 0 {
		return "", "", 0, nil
	}
	offset := 0

	// service_type (C-string)
	_, offset = readCString(body, offset)

	// source_addr_ton
	if offset >= len(body) {
		return
	}
	offset++ // source_addr_ton (skip)

	// source_addr_npi
	if offset >= len(body) {
		return
	}
	offset++ // source_addr_npi (skip)

	// source_addr
	sourceAddr, offset = readCString(body, offset)

	// dest_addr_ton
	if offset >= len(body) {
		return
	}
	offset++ // dest_addr_ton (skip)

	// dest_addr_npi
	if offset >= len(body) {
		return
	}
	offset++ // dest_addr_npi (skip)

	// destination_addr
	destAddr, offset = readCString(body, offset)

	// esm_class
	if offset >= len(body) {
		return
	}
	esmClass = body[offset]
	offset++

	// protocol_id
	if offset >= len(body) {
		return
	}
	offset++ // protocol_id (skip)

	// priority_flag
	if offset >= len(body) {
		return
	}
	offset++ // priority_flag (skip)

	// schedule_delivery_time (C-string)
	_, offset = readCString(body, offset)

	// validity_period (C-string)
	_, offset = readCString(body, offset)

	// registered_delivery
	if offset >= len(body) {
		return
	}
	offset++ // registered_delivery (skip)

	// replace_if_present_flag
	if offset >= len(body) {
		return
	}
	offset++ // replace_if_present_flag (skip)

	// data_coding
	if offset >= len(body) {
		return
	}
	offset++ // data_coding (skip)

	// sm_default_msg_id
	if offset >= len(body) {
		return
	}
	offset++ // sm_default_msg_id (skip)

	// sm_length
	if offset >= len(body) {
		return
	}
	smLen := int(body[offset])
	offset++

	// short_message
	if smLen > 0 && offset+smLen <= len(body) {
		shortMessage = make([]byte, smLen)
		copy(shortMessage, body[offset:offset+smLen])
	}

	return sourceAddr, destAddr, esmClass, shortMessage
}

// EncodeDeliverSMResp builds a complete deliver_sm_resp PDU (with header).
func EncodeDeliverSMResp(seqNum uint32) *PDU {
	// deliver_sm_resp body is just a null message_id
	body := []byte{0x00}
	return &PDU{
		CommandID:      CmdDeliverSMResp,
		CommandStatus:  StatusOK,
		SequenceNumber: seqNum,
		Body:           body,
	}
}
