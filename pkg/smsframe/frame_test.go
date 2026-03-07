package smsframe

import (
	"bytes"
	"testing"
)

func TestFrameOTASMS_Single(t *testing.T) {
	payload := []byte{0xAA, 0xBB, 0xCC}
	result := FrameOTASMS(payload, nil)

	// Expect UDH: 02 70 00 followed by payload
	if len(result) != 3+len(payload) {
		t.Fatalf("expected length %d, got %d", 3+len(payload), len(result))
	}
	if result[0] != 0x02 || result[1] != 0x70 || result[2] != 0x00 {
		t.Fatalf("expected UDH 02 70 00, got %02X %02X %02X", result[0], result[1], result[2])
	}
	if !bytes.Equal(result[3:], payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestFrameOTASMS_Concat(t *testing.T) {
	payload := []byte{0xDD, 0xEE}
	concat := &ConcatInfo{RefNum: 0x42, Total: 3, Part: 2}
	result := FrameOTASMS(payload, concat)

	// Expect UDH: 07 00 03 42 03 02 70 00 followed by payload
	expectedUDH := []byte{0x07, 0x00, 0x03, 0x42, 0x03, 0x02, 0x70, 0x00}
	if len(result) != 8+len(payload) {
		t.Fatalf("expected length %d, got %d", 8+len(payload), len(result))
	}
	if !bytes.Equal(result[:8], expectedUDH) {
		t.Fatalf("UDH mismatch: got %X, want %X", result[:8], expectedUDH)
	}
	if !bytes.Equal(result[8:], payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestSplitForConcat_SinglePart(t *testing.T) {
	// maxPartSize=140, single UDH=3, capacity=137
	packet := make([]byte, 100)
	for i := range packet {
		packet[i] = byte(i)
	}

	parts, err := SplitForConcat(packet, 140, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(parts))
	}

	// Should use single UDH (02 70 00)
	if parts[0][0] != 0x02 || parts[0][1] != 0x70 || parts[0][2] != 0x00 {
		t.Fatalf("expected single UDH, got %X", parts[0][:3])
	}
	if !bytes.Equal(parts[0][3:], packet) {
		t.Fatalf("payload mismatch")
	}
}

func TestSplitForConcat_MultiPart(t *testing.T) {
	// maxPartSize=50, concat UDH=8, capacity per part=42
	// packet of 120 bytes needs ceil(120/42) = 3 parts
	packet := make([]byte, 120)
	for i := range packet {
		packet[i] = byte(i % 256)
	}

	parts, err := SplitForConcat(packet, 50, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}

	// All parts should have concat UDH
	var reassembled []byte
	var refNum byte
	for i, part := range parts {
		if part[0] != 0x07 {
			t.Fatalf("part %d: expected UDH length 0x07, got 0x%02X", i, part[0])
		}
		if part[1] != 0x00 || part[2] != 0x03 {
			t.Fatalf("part %d: expected concat IEI 00 03, got %02X %02X", i, part[1], part[2])
		}
		if i == 0 {
			refNum = part[3]
		} else if part[3] != refNum {
			t.Fatalf("part %d: refNum mismatch, expected %02X got %02X", i, refNum, part[3])
		}
		if part[4] != 0x03 {
			t.Fatalf("part %d: expected total=3, got %d", i, part[4])
		}
		if part[5] != byte(i+1) {
			t.Fatalf("part %d: expected part=%d, got %d", i, i+1, part[5])
		}
		if part[6] != 0x70 || part[7] != 0x00 {
			t.Fatalf("part %d: expected SIM toolkit IEI 70 00", i)
		}
		reassembled = append(reassembled, part[8:]...)
	}

	if !bytes.Equal(reassembled, packet) {
		t.Fatalf("reassembled payload does not match original")
	}
}

func TestSplitForConcat_ExceedsMax(t *testing.T) {
	// maxPartSize=50, concat UDH=8, capacity per part=42
	// packet of 120 bytes needs 3 parts, but maxParts=2
	packet := make([]byte, 120)
	_, err := SplitForConcat(packet, 50, 2)
	if err == nil {
		t.Fatalf("expected error for exceeding max parts")
	}
}

func TestReassemble_SingleSMS(t *testing.T) {
	r := NewReassembler()

	// Build a single SMS with UDH 02 70 00
	payload := []byte{0x11, 0x22, 0x33}
	raw := FrameOTASMS(payload, nil)

	result, complete := r.AddPart("123456789", raw)
	if !complete {
		t.Fatalf("expected complete=true for single SMS")
	}
	if !bytes.Equal(result, payload) {
		t.Fatalf("payload mismatch: got %X, want %X", result, payload)
	}
}

func TestReassemble_TwoParts(t *testing.T) {
	r := NewReassembler()

	payload1 := []byte{0xAA, 0xBB}
	payload2 := []byte{0xCC, 0xDD}

	raw1 := FrameOTASMS(payload1, &ConcatInfo{RefNum: 0x55, Total: 2, Part: 1})
	raw2 := FrameOTASMS(payload2, &ConcatInfo{RefNum: 0x55, Total: 2, Part: 2})

	// Add part 1
	result, complete := r.AddPart("123456789", raw1)
	if complete {
		t.Fatalf("expected complete=false after first part")
	}
	if result != nil {
		t.Fatalf("expected nil result after first part")
	}

	// Add part 2
	result, complete = r.AddPart("123456789", raw2)
	if !complete {
		t.Fatalf("expected complete=true after second part")
	}
	expected := append(payload1, payload2...)
	if !bytes.Equal(result, expected) {
		t.Fatalf("payload mismatch: got %X, want %X", result, expected)
	}
}

func TestParseUDH(t *testing.T) {
	tests := []struct {
		name        string
		raw         []byte
		wantUDHLen  int
		wantConcat  *ConcatInfo
		wantPayload []byte
	}{
		{
			name:        "single SMS UDH",
			raw:         []byte{0x02, 0x70, 0x00, 0xAA, 0xBB},
			wantUDHLen:  2,
			wantConcat:  nil,
			wantPayload: []byte{0xAA, 0xBB},
		},
		{
			name:       "concat UDH",
			raw:        []byte{0x07, 0x00, 0x03, 0x10, 0x03, 0x01, 0x70, 0x00, 0xCC},
			wantUDHLen: 7,
			wantConcat: &ConcatInfo{RefNum: 0x10, Total: 3, Part: 1},
			wantPayload: []byte{0xCC},
		},
		{
			name:        "empty input",
			raw:         []byte{},
			wantUDHLen:  0,
			wantConcat:  nil,
			wantPayload: []byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			udhLen, concat, payload := ParseUDH(tt.raw)
			if udhLen != tt.wantUDHLen {
				t.Errorf("udhLen = %d, want %d", udhLen, tt.wantUDHLen)
			}
			if tt.wantConcat == nil {
				if concat != nil {
					t.Errorf("expected nil concat, got %+v", concat)
				}
			} else {
				if concat == nil {
					t.Fatal("expected non-nil concat")
				}
				if concat.RefNum != tt.wantConcat.RefNum || concat.Total != tt.wantConcat.Total || concat.Part != tt.wantConcat.Part {
					t.Errorf("concat = %+v, want %+v", concat, tt.wantConcat)
				}
			}
			if !bytes.Equal(payload, tt.wantPayload) {
				t.Errorf("payload = %X, want %X", payload, tt.wantPayload)
			}
		})
	}
}
