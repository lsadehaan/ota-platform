package smsframe

import (
	"fmt"
	"math/rand"
)

// ConcatInfo holds concatenation metadata for multi-part SMS
type ConcatInfo struct {
	RefNum byte
	Total  byte
	Part   byte
}

// FrameOTASMS wraps a 03.48 command packet into SMS TP-UD with proper UDH.
// For single SMS: UDH = 02 70 00
// For concatenated: UDH = 07 00 03 REF TOTAL PART 70 00
func FrameOTASMS(packet []byte, concat *ConcatInfo) []byte {
	if concat == nil {
		// Single SMS: UDH length=0x02, IEI=0x70 (SIM toolkit security), IEI data len=0x00
		udh := []byte{0x02, 0x70, 0x00}
		result := make([]byte, 0, len(udh)+len(packet))
		result = append(result, udh...)
		result = append(result, packet...)
		return result
	}

	// Concatenated SMS: UDH length=0x07
	// IEI=0x00 (concatenation), len=0x03, REF, TOTAL, PART
	// IEI=0x70 (SIM toolkit security), len=0x00
	udh := []byte{
		0x07,
		0x00, 0x03, concat.RefNum, concat.Total, concat.Part,
		0x70, 0x00,
	}
	result := make([]byte, 0, len(udh)+len(packet))
	result = append(result, udh...)
	result = append(result, packet...)
	return result
}

// SplitForConcat splits a 03.48 packet into multiple SMS parts.
// maxPartSize is the max TP-UD payload per part (excluding UDH).
// maxParts is the maximum number of SMS parts allowed.
// Returns the framed parts ready to send.
// For single-part: uses 3-byte UDH (02 70 00), payload capacity = maxPartSize - 3
// For multi-part: uses 8-byte UDH (07 00 03 XX YY ZZ 70 00), payload capacity = maxPartSize - 8
func SplitForConcat(packet []byte, maxPartSize int, maxParts int) ([][]byte, error) {
	singleUDHLen := 3
	concatUDHLen := 8

	// Check if packet fits in a single SMS
	singleCapacity := maxPartSize - singleUDHLen
	if singleCapacity <= 0 {
		return nil, fmt.Errorf("maxPartSize %d is too small for even a single-part UDH", maxPartSize)
	}

	if len(packet) <= singleCapacity {
		framed := FrameOTASMS(packet, nil)
		return [][]byte{framed}, nil
	}

	// Multi-part: calculate per-part capacity
	perPartCapacity := maxPartSize - concatUDHLen
	if perPartCapacity <= 0 {
		return nil, fmt.Errorf("maxPartSize %d is too small for concatenated UDH", maxPartSize)
	}

	// Calculate total parts needed
	totalParts := (len(packet) + perPartCapacity - 1) / perPartCapacity
	if totalParts > maxParts {
		return nil, fmt.Errorf("packet requires %d parts but maximum is %d", totalParts, maxParts)
	}
	if totalParts > 255 {
		return nil, fmt.Errorf("packet requires %d parts but maximum for concatenation is 255", totalParts)
	}

	refNum := byte(rand.Intn(256))

	parts := make([][]byte, 0, totalParts)
	offset := 0
	for i := 0; i < totalParts; i++ {
		end := offset + perPartCapacity
		if end > len(packet) {
			end = len(packet)
		}
		chunk := packet[offset:end]

		concat := &ConcatInfo{
			RefNum: refNum,
			Total:  byte(totalParts),
			Part:   byte(i + 1),
		}
		framed := FrameOTASMS(chunk, concat)
		parts = append(parts, framed)
		offset = end
	}

	return parts, nil
}
