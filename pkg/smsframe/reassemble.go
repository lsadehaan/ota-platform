package smsframe

import "sync"

// Reassembler collects concatenated SMS parts and reassembles them.
type Reassembler struct {
	mu    sync.Mutex
	parts map[string]map[byte][]byte // key = "msisdn:refnum", value = map[partNum]payload
	total map[string]byte            // key -> total parts expected
}

// NewReassembler creates a new Reassembler instance.
func NewReassembler() *Reassembler {
	return &Reassembler{
		parts: make(map[string]map[byte][]byte),
		total: make(map[string]byte),
	}
}

// AddPart processes an incoming SMS payload. Parses the UDH to detect concatenation.
// Returns (reassembled_payload, complete) where complete=true when all parts received.
// For non-concatenated SMS, returns the payload (after UDH) immediately with complete=true.
//
// UDH parsing:
// - First byte = UDH length
// - Look for IEI 0x00 (concatenation): 00 03 REF TOTAL PART
// - Look for IEI 0x70 or 0x71 (SIM toolkit security headers)
// - Strip all UDH, return just the payload data
func (r *Reassembler) AddPart(msisdn string, raw []byte) ([]byte, bool) {
	_, concat, payload := ParseUDH(raw)

	// Non-concatenated: return payload immediately
	if concat == nil {
		return payload, true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := msisdn + ":" + string(rune(concat.RefNum))

	// Initialize storage for this concat reference if needed
	if _, exists := r.parts[key]; !exists {
		r.parts[key] = make(map[byte][]byte)
		r.total[key] = concat.Total
	}

	// Store this part (make a copy of the payload)
	partData := make([]byte, len(payload))
	copy(partData, payload)
	r.parts[key][concat.Part] = partData

	// Check if all parts received
	if byte(len(r.parts[key])) < r.total[key] {
		return nil, false
	}

	// Reassemble in order
	totalParts := r.total[key]
	totalLen := 0
	for i := byte(1); i <= totalParts; i++ {
		totalLen += len(r.parts[key][i])
	}

	assembled := make([]byte, 0, totalLen)
	for i := byte(1); i <= totalParts; i++ {
		assembled = append(assembled, r.parts[key][i]...)
	}

	// Clean up
	delete(r.parts, key)
	delete(r.total, key)

	return assembled, true
}

// ParseUDH extracts UDH information elements from raw SMS data.
// Returns: udhLen, concat info (nil if not concatenated), payload after UDH
func ParseUDH(raw []byte) (int, *ConcatInfo, []byte) {
	if len(raw) == 0 {
		return 0, nil, raw
	}

	udhLen := int(raw[0])

	// Validate that we have enough data for the UDH
	if len(raw) < udhLen+1 {
		return 0, nil, raw
	}

	payload := raw[udhLen+1:]
	var concat *ConcatInfo

	// Parse IEs within the UDH
	pos := 1 // skip UDH length byte
	endUDH := udhLen + 1
	for pos < endUDH {
		if pos+1 >= endUDH {
			break
		}
		iei := raw[pos]
		ieLen := int(raw[pos+1])
		pos += 2

		if pos+ieLen > endUDH {
			break
		}

		switch iei {
		case 0x00: // Concatenation IE (8-bit reference)
			if ieLen >= 3 {
				concat = &ConcatInfo{
					RefNum: raw[pos],
					Total:  raw[pos+1],
					Part:   raw[pos+2],
				}
			}
		case 0x70, 0x71:
			// SIM toolkit security headers - no data to extract, just skip
		}

		pos += ieLen
	}

	return udhLen, concat, payload
}
