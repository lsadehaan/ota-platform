package gsm0348

import (
	"crypto/cipher"
	"crypto/des"
	"fmt"
)

// ParseResponsePacket parses a raw GSM 03.48 response packet and returns the
// decoded fields. If PoRCiphered is set in the security profile, the data
// portion is decrypted using the provided cipherKey.
//
// Response packet structure:
//
//	RPL(2) + RHL(1) + TAR(3) + CNTR(5) + PCNTR(1) + StatusCode(1) + Signature(variable) + Data(variable)
func (sp *SecurityProfile) ParseResponsePacket(raw []byte, cipherKey, signKey []byte) (*ResponsePacket, error) {
	if len(raw) < 2 {
		return nil, fmt.Errorf("gsm0348: response packet too short for RPL")
	}

	// RPL: 2 bytes, total length of everything after RPL.
	rpl := int(raw[0])<<8 | int(raw[1])
	if len(raw) < 2+rpl {
		return nil, fmt.Errorf("gsm0348: response packet truncated: RPL=%d but only %d bytes remain", rpl, len(raw)-2)
	}

	pos := 2

	// RHL: 1 byte, response header length (excludes RHL byte itself).
	if pos >= len(raw) {
		return nil, fmt.Errorf("gsm0348: response packet too short for RHL")
	}
	rhl := int(raw[pos])
	pos++

	headerStart := pos

	// TAR: 3 bytes.
	if pos+3 > len(raw) {
		return nil, fmt.Errorf("gsm0348: response packet too short for TAR")
	}
	var tar [3]byte
	copy(tar[:], raw[pos:pos+3])
	pos += 3

	// CNTR: 5 bytes.
	if pos+5 > len(raw) {
		return nil, fmt.Errorf("gsm0348: response packet too short for CNTR")
	}
	var counter [5]byte
	copy(counter[:], raw[pos:pos+5])
	pos += 5

	// PCNTR: 1 byte.
	if pos >= len(raw) {
		return nil, fmt.Errorf("gsm0348: response packet too short for PCNTR")
	}
	pcntr := raw[pos]
	pos++

	// StatusCode: 1 byte.
	if pos >= len(raw) {
		return nil, fmt.Errorf("gsm0348: response packet too short for StatusCode")
	}
	statusCode := raw[pos]
	pos++

	// Signature: variable length, determined from RHL.
	// RHL covers: TAR(3) + CNTR(5) + PCNTR(1) + StatusCode(1) + Signature(sigLen)
	// So sigLen = RHL - 10
	sigLen := rhl - 10
	if sigLen < 0 {
		sigLen = 0
	}

	var signature []byte
	if sigLen > 0 {
		if pos+sigLen > len(raw) {
			return nil, fmt.Errorf("gsm0348: response packet too short for signature")
		}
		signature = make([]byte, sigLen)
		copy(signature, raw[pos:pos+sigLen])
		pos += sigLen
	}

	_ = headerStart // consumed for clarity

	// Data: everything remaining after the header.
	dataEnd := 2 + rpl
	var data []byte
	if pos < dataEnd {
		data = make([]byte, dataEnd-pos)
		copy(data, raw[pos:dataEnd])
	}

	// Decrypt if PoRCiphered is set.
	if sp.PoRCiphered && len(data) > 0 {
		decrypted, err := decryptData(sp.KIcMode, cipherKey, data)
		if err != nil {
			return nil, fmt.Errorf("gsm0348: decrypt response data: %w", err)
		}
		data = decrypted
	}

	// Remove PCNTR padding bytes from end of data.
	if int(pcntr) > 0 && len(data) >= int(pcntr) {
		data = data[:len(data)-int(pcntr)]
	}

	return &ResponsePacket{
		TAR:        tar,
		Counter:    counter,
		PCNTR:      pcntr,
		StatusCode: statusCode,
		Signature:  signature,
		Data:       data,
	}, nil
}

// ParseResponsePacketWithProvider parses a response packet when using a CryptoProvider.
// If PoRCiphered is set, returns an error since HSM-based response decryption is not yet supported.
// For non-ciphered responses (the common case), this works identically to ParseResponsePacket.
func (sp *SecurityProfile) ParseResponsePacketWithProvider(raw []byte, cardID string, provider CryptoProvider) (*ResponsePacket, error) {
	if sp.PoRCiphered {
		return nil, fmt.Errorf("gsm0348: response decryption with CryptoProvider not yet supported; use ParseResponsePacket with raw keys")
	}
	// When PoR is not ciphered, no keys are needed for parsing.
	return sp.ParseResponsePacket(raw, nil, nil)
}

// decryptData decrypts data using the specified cipher mode with an all-zero IV.
// For 3DES-2-key, the 16-byte key is expanded to 24 bytes (K1|K2|K1).
func decryptData(mode CipherMode, key []byte, data []byte) ([]byte, error) {
	var block cipher.Block
	var err error

	switch mode {
	case CipherDES_CBC:
		if len(key) < 8 {
			return nil, fmt.Errorf("gsm0348: DES key must be at least 8 bytes")
		}
		block, err = des.NewCipher(key[:8])
	case Cipher3DES_CBC_2Keys:
		if len(key) < 16 {
			return nil, fmt.Errorf("gsm0348: 3DES-2-key requires 16-byte key, got %d", len(key))
		}
		expandedKey := make([]byte, 24)
		copy(expandedKey[:16], key[:16])
		copy(expandedKey[16:], key[:8])
		block, err = des.NewTripleDESCipher(expandedKey)
	case Cipher3DES_CBC_3Keys:
		if len(key) < 24 {
			return nil, fmt.Errorf("gsm0348: 3DES-3-key requires 24-byte key, got %d", len(key))
		}
		block, err = des.NewTripleDESCipher(key[:24])
	default:
		return nil, fmt.Errorf("gsm0348: unsupported decryption cipher mode: %d", mode)
	}

	if err != nil {
		return nil, err
	}

	blockSize := block.BlockSize()
	if len(data)%blockSize != 0 {
		return nil, fmt.Errorf("gsm0348: encrypted data length %d is not a multiple of block size %d", len(data), blockSize)
	}

	iv := make([]byte, blockSize)
	cbc := cipher.NewCBCDecrypter(block, iv)

	decrypted := make([]byte, len(data))
	cbc.CryptBlocks(decrypted, data)
	return decrypted, nil
}
