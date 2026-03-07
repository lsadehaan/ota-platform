package globalplatform

import (
	"bytes"
	"testing"
)

func TestBuildDeleteAPDU(t *testing.T) {
	aid := []byte{0xA0, 0x00, 0x00, 0x00, 0x62, 0x01}

	// Without related objects
	apdu := BuildDeleteAPDU(aid, false)
	// Expected: 80 E4 00 00 08 4F 06 A0 00 00 00 62 01
	if apdu[0] != CLAGlobalPlatform {
		t.Errorf("CLA = 0x%02X, want 0x%02X", apdu[0], CLAGlobalPlatform)
	}
	if apdu[1] != INSDelete {
		t.Errorf("INS = 0x%02X, want 0x%02X", apdu[1], INSDelete)
	}
	if apdu[2] != 0x00 {
		t.Errorf("P1 = 0x%02X, want 0x00", apdu[2])
	}
	if apdu[3] != 0x00 {
		t.Errorf("P2 = 0x%02X, want 0x00", apdu[3])
	}
	// Data: 4F 06 <AID>
	dataLen := int(apdu[4])
	if dataLen != 2+len(aid) {
		t.Errorf("Lc = %d, want %d", dataLen, 2+len(aid))
	}
	if apdu[5] != 0x4F {
		t.Errorf("tag = 0x%02X, want 0x4F", apdu[5])
	}
	if apdu[6] != byte(len(aid)) {
		t.Errorf("AID length = %d, want %d", apdu[6], len(aid))
	}
	if !bytes.Equal(apdu[7:7+len(aid)], aid) {
		t.Errorf("AID mismatch")
	}

	// With related objects
	apdu2 := BuildDeleteAPDU(aid, true)
	if apdu2[3] != 0x80 {
		t.Errorf("P2 with deleteRelated = 0x%02X, want 0x80", apdu2[3])
	}
}

func TestBuildInstallForLoadAPDU(t *testing.T) {
	loadFileAID := []byte{0xA0, 0x00, 0x00, 0x00, 0x62, 0x01}
	sdAID := []byte{0xA0, 0x00, 0x00, 0x01, 0x51, 0x00}

	apdu := BuildInstallForLoadAPDU(loadFileAID, sdAID)

	if apdu[0] != CLAGlobalPlatform {
		t.Errorf("CLA = 0x%02X, want 0x%02X", apdu[0], CLAGlobalPlatform)
	}
	if apdu[1] != INSInstall {
		t.Errorf("INS = 0x%02X, want 0x%02X", apdu[1], INSInstall)
	}
	if apdu[2] != 0x02 {
		t.Errorf("P1 = 0x%02X, want 0x02", apdu[2])
	}
	if apdu[3] != 0x00 {
		t.Errorf("P2 = 0x%02X, want 0x00", apdu[3])
	}

	// Verify data structure
	data := apdu[5:]
	offset := 0

	// Load file AID
	lfAIDLen := int(data[offset])
	offset++
	if lfAIDLen != len(loadFileAID) {
		t.Fatalf("loadFileAID length = %d, want %d", lfAIDLen, len(loadFileAID))
	}
	if !bytes.Equal(data[offset:offset+lfAIDLen], loadFileAID) {
		t.Errorf("loadFileAID mismatch")
	}
	offset += lfAIDLen

	// Security domain AID
	sdAIDLen := int(data[offset])
	offset++
	if sdAIDLen != len(sdAID) {
		t.Fatalf("sdAID length = %d, want %d", sdAIDLen, len(sdAID))
	}
	if !bytes.Equal(data[offset:offset+sdAIDLen], sdAID) {
		t.Errorf("sdAID mismatch")
	}
	offset += sdAIDLen

	// Hash length, params length, token length should all be 0
	if data[offset] != 0x00 {
		t.Errorf("hash length = %d, want 0", data[offset])
	}
	offset++
	if data[offset] != 0x00 {
		t.Errorf("params length = %d, want 0", data[offset])
	}
	offset++
	if data[offset] != 0x00 {
		t.Errorf("token length = %d, want 0", data[offset])
	}
}

func TestBuildInstallForInstallAPDU(t *testing.T) {
	loadFileAID := []byte{0xA0, 0x00, 0x00, 0x00, 0x62, 0x01}
	moduleAID := []byte{0xA0, 0x00, 0x00, 0x00, 0x62, 0x01, 0x01}
	appAID := []byte{0xA0, 0x00, 0x00, 0x00, 0x62, 0x01, 0x01}
	privilege := byte(0x00)
	stkParams := BuildSTKParameters(1, []byte{0x01}, []byte{0xFF}, 0x01, 0x08, 0x20, 0x02)
	installParams := []byte{}

	apdu := BuildInstallForInstallAPDU(loadFileAID, moduleAID, appAID, privilege, stkParams, installParams)

	if apdu[0] != CLAGlobalPlatform {
		t.Errorf("CLA = 0x%02X, want 0x%02X", apdu[0], CLAGlobalPlatform)
	}
	if apdu[1] != INSInstall {
		t.Errorf("INS = 0x%02X, want 0x%02X", apdu[1], INSInstall)
	}
	if apdu[2] != 0x0C {
		t.Errorf("P1 = 0x%02X, want 0x0C", apdu[2])
	}
	if apdu[3] != 0x00 {
		t.Errorf("P2 = 0x%02X, want 0x00", apdu[3])
	}

	// Verify data structure
	data := apdu[5:]
	offset := 0

	// Load file AID
	lfLen := int(data[offset])
	offset++
	offset += lfLen

	// Module AID
	modLen := int(data[offset])
	offset++
	offset += modLen

	// App AID
	appLen := int(data[offset])
	offset++
	offset += appLen

	// Privileges: length(1) + privilege(1)
	privLen := int(data[offset])
	offset++
	if privLen != 1 {
		t.Errorf("privilege length = %d, want 1", privLen)
	}
	if data[offset] != privilege {
		t.Errorf("privilege = 0x%02X, want 0x%02X", data[offset], privilege)
	}
	offset += privLen

	// Install params field length
	paramsFieldLen := int(data[offset])
	offset++
	if paramsFieldLen > 0 {
		// Should start with C9 tag
		if data[offset] != 0xC9 {
			t.Errorf("params field tag = 0x%02X, want 0xC9", data[offset])
		}
	}
	offset += paramsFieldLen

	// Install token length = 0
	if data[offset] != 0x00 {
		t.Errorf("token length = %d, want 0", data[offset])
	}
}

func TestBuildLoadBlockAPDUs(t *testing.T) {
	// Create test data: 250 bytes
	data := make([]byte, 250)
	for i := range data {
		data[i] = byte(i)
	}

	maxBlockSize := 100
	apdus := BuildLoadBlockAPDUs(data, maxBlockSize)

	// Should need 3 blocks: 100 + 100 + 50
	if len(apdus) != 3 {
		t.Fatalf("expected 3 blocks, got %d", len(apdus))
	}

	// Check P1 flags
	for i, apdu := range apdus {
		if apdu[0] != CLAGlobalPlatform {
			t.Errorf("block %d: CLA = 0x%02X", i, apdu[0])
		}
		if apdu[1] != INSLoad {
			t.Errorf("block %d: INS = 0x%02X", i, apdu[1])
		}

		// P1: last block should be 0x80
		if i < len(apdus)-1 {
			if apdu[2] != 0x00 {
				t.Errorf("block %d: P1 = 0x%02X, want 0x00 (more blocks)", i, apdu[2])
			}
		} else {
			if apdu[2] != 0x80 {
				t.Errorf("block %d: P1 = 0x%02X, want 0x80 (last block)", i, apdu[2])
			}
		}

		// P2 = block number
		if apdu[3] != byte(i) {
			t.Errorf("block %d: P2 = %d, want %d", i, apdu[3], i)
		}

		// Lc
		blockDataLen := int(apdu[4])
		if i < 2 {
			if blockDataLen != 100 {
				t.Errorf("block %d: data len = %d, want 100", i, blockDataLen)
			}
		} else {
			if blockDataLen != 50 {
				t.Errorf("block %d: data len = %d, want 50", i, blockDataLen)
			}
		}
	}

	// Verify all data is present
	var reassembled []byte
	for _, apdu := range apdus {
		reassembled = append(reassembled, apdu[5:]...)
	}
	if !bytes.Equal(reassembled, data) {
		t.Errorf("reassembled data does not match original")
	}
}

func TestBuildSTKParameters(t *testing.T) {
	params := BuildSTKParameters(
		2,                    // maxMenuItems
		[]byte{0x01, 0x02},   // menuIDPositions
		[]byte{0xFF},         // accessDomain
		0x01,                 // priorityLevel
		0x08,                 // maxTimers
		0x20,                 // maxMenuEntryLength
		0x02,                 // maxChannels
	)

	// Should start with C9 tag
	if params[0] != 0xC9 {
		t.Errorf("tag = 0x%02X, want 0xC9", params[0])
	}

	// Length byte
	bodyLen := int(params[1])
	if len(params) != 2+bodyLen {
		t.Errorf("total length = %d, want %d", len(params), 2+bodyLen)
	}

	body := params[2:]

	// maxMenuItems
	if body[0] != 0x02 {
		t.Errorf("maxMenuItems = %d, want 2", body[0])
	}

	// menuIDPositions: length + data
	if body[1] != 0x02 {
		t.Errorf("menuIDPositions length = %d, want 2", body[1])
	}
	if body[2] != 0x01 || body[3] != 0x02 {
		t.Errorf("menuIDPositions mismatch")
	}

	// accessDomain: length + data
	if body[4] != 0x01 {
		t.Errorf("accessDomain length = %d, want 1", body[4])
	}
	if body[5] != 0xFF {
		t.Errorf("accessDomain = 0x%02X, want 0xFF", body[5])
	}

	// priorityLevel
	if body[6] != 0x01 {
		t.Errorf("priorityLevel = %d, want 1", body[6])
	}

	// maxTimers
	if body[7] != 0x08 {
		t.Errorf("maxTimers = %d, want 8", body[7])
	}

	// maxMenuEntryLength
	if body[8] != 0x20 {
		t.Errorf("maxMenuEntryLength = %d, want 0x20", body[8])
	}

	// maxChannels
	if body[9] != 0x02 {
		t.Errorf("maxChannels = %d, want 2", body[9])
	}
}
