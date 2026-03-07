package hexutil

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// Encode converts a byte slice to an uppercase hex string.
func Encode(data []byte) string {
	return strings.ToUpper(hex.EncodeToString(data))
}

// Decode converts a hex string (case-insensitive) to a byte slice.
func Decode(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("hexutil: odd-length hex string: %q", s)
	}
	return hex.DecodeString(s)
}

// MustDecode converts a hex string to bytes, panicking on error.
func MustDecode(s string) []byte {
	b, err := Decode(s)
	if err != nil {
		panic(err)
	}
	return b
}

// BERLength encodes an integer as a BER-TLV length field.
//   - If length < 0x80: single byte
//   - If length < 0x100: 0x81 followed by one byte
//   - Otherwise: 0x82 followed by two bytes (big-endian)
func BERLength(length int) []byte {
	if length < 0x80 {
		return []byte{byte(length)}
	}
	if length < 0x100 {
		return []byte{0x81, byte(length)}
	}
	return []byte{0x82, byte(length >> 8), byte(length & 0xFF)}
}
