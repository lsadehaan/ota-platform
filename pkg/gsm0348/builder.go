package gsm0348

import (
	"context"
	"crypto/cipher"
	"crypto/des"
	"fmt"
)

// BuildCommandPacket constructs a complete GSM 03.48 command packet from the
// given input using the security profile's parameters.
func (sp *SecurityProfile) BuildCommandPacket(input *CommandPacketInput) ([]byte, error) {
	spi1 := sp.encodeSPI1()
	spi2 := sp.encodeSPI2()
	kic := sp.encodeKIc()
	kid := sp.encodeKID()
	sigLen := sp.signatureLength()

	// CHL = 13 + signature length
	// CHL covers: SPI(2) + KIc(1) + KID(1) + TAR(3) + CNTR(5) + PCNTR(1) = 13
	chl := byte(13 + sigLen)

	// Pad user data to block boundary if ciphering is enabled.
	var paddedData []byte
	var pcntr byte
	if sp.Ciphered {
		blockSize := sp.cipherBlockSize()
		paddedData, pcntr = padData(input.UserData, blockSize)
	} else {
		paddedData = make([]byte, len(input.UserData))
		copy(paddedData, input.UserData)
		pcntr = 0
	}

	// Build the header portion: SPI(2) + KIc(1) + KID(1) + TAR(3) + CNTR(5) + PCNTR(1)
	header := make([]byte, 0, 13)
	header = append(header, spi1, spi2)
	header = append(header, kic)
	header = append(header, kid)
	header = append(header, input.TAR[:]...)
	header = append(header, input.Counter[:]...)
	header = append(header, pcntr)

	// Compute signature (RC, CC, or DS).
	signature := make([]byte, sigLen)
	switch sp.CertMode {
	case CertNone:
		// No signature, leave as zero-length (sigLen == 0).
	case CertRC:
		// Redundancy Check (CRC32)
		crc := computeCRC(header, paddedData, sigLen)
		copy(signature, crc)
	case CertCC:
		// Cryptographic Checksum (MAC)
		var mac []byte
		var err error
		if input.CryptoProvider != nil {
			macInput := append(header, paddedData...)
			if len(macInput)%8 != 0 {
				padLen := 8 - (len(macInput) % 8)
				macInput = append(macInput, make([]byte, padLen)...)
			}
			mac, err = input.CryptoProvider.ComputeMAC(context.Background(), input.CardID, algoName(sp.KIDAlgo), cipherModeName(sp.KIDMode), macInput)
		} else {
			mac, err = computeMAC(sp.KIDMode, input.SigningKey, header, paddedData)
		}
		if err != nil {
			return nil, fmt.Errorf("gsm0348: compute MAC: %w", err)
		}
		copy(signature, mac[:sigLen])
	case CertDS:
		return nil, fmt.Errorf("gsm0348: digital signature (DS) mode not implemented")
	}

	// Build the secured data portion: signature + padded data.
	securedData := append(signature, paddedData...)

	// Encrypt the secured data if ciphering is enabled.
	if sp.Ciphered {
		var encrypted []byte
		var err error
		if input.CryptoProvider != nil {
			encrypted, err = input.CryptoProvider.Encrypt(context.Background(), input.CardID, algoName(sp.KIcAlgo), cipherModeName(sp.KIcMode), securedData)
		} else {
			encrypted, err = encryptData(sp.KIcMode, input.CipheringKey, input.Counter[:], securedData)
		}
		if err != nil {
			return nil, fmt.Errorf("gsm0348: encrypt data: %w", err)
		}
		securedData = encrypted
	}

	// CPL = length of everything after CPL itself.
	// That is: CHL(1) + header(13) + securedData
	cpl := 1 + len(header) + len(securedData)

	// Assemble final packet: CPL(2) + CHL(1) + header(13) + securedData
	packet := make([]byte, 0, 2+1+len(header)+len(securedData))
	packet = append(packet, byte(cpl>>8), byte(cpl&0xFF))
	packet = append(packet, chl)
	packet = append(packet, header...)
	packet = append(packet, securedData...)

	return packet, nil
}

// encodeSPI1 encodes Security Parameter Indicator byte 1.
// Bits 1-2: certification mode, bit 3: ciphered, bits 4-5: counter mode.
func (sp *SecurityProfile) encodeSPI1() byte {
	var b byte
	b |= byte(sp.CertMode) & 0x03        // bits 0-1
	if sp.Ciphered {                      // bit 2
		b |= 0x04
	}
	b |= (byte(sp.CounterMode) & 0x03) << 3 // bits 3-4
	return b
}

// encodeSPI2 encodes Security Parameter Indicator byte 2.
// Bits 1-2: PoR mode, bits 3-4: PoR cert mode, bit 5: PoR ciphered,
// bits 6-7: PoR protocol.
func (sp *SecurityProfile) encodeSPI2() byte {
	var b byte
	b |= byte(sp.PoRMode) & 0x03             // bits 0-1
	b |= (byte(sp.PoRCertMode) & 0x03) << 2  // bits 2-3
	if sp.PoRCiphered {                       // bit 4
		b |= 0x10
	}
	b |= (sp.PoRProtocol & 0x03) << 5 // bits 5-6
	return b
}

// encodeKIc encodes the Key Identifier for Ciphering byte.
// Bits 1-2: algorithm, bits 3-4: cipher mode, bits 5-7: keyset ID.
func (sp *SecurityProfile) encodeKIc() byte {
	var b byte
	b |= sp.KIcAlgo & 0x03              // bits 0-1
	b |= (byte(sp.KIcMode) & 0x03) << 2 // bits 2-3
	b |= (sp.KIcKeysetID & 0x07) << 4   // bits 4-6
	return b
}

// encodeKID encodes the Key Identifier for RC/CC/DS byte.
// Same structure as KIc.
func (sp *SecurityProfile) encodeKID() byte {
	var b byte
	b |= sp.KIDAlgo & 0x03              // bits 0-1
	b |= (byte(sp.KIDMode) & 0x03) << 2 // bits 2-3
	b |= (sp.KIDKeysetID & 0x07) << 4   // bits 4-6
	return b
}

// signatureLength returns the length of the signature field in bytes,
// based on the certification mode.
func (sp *SecurityProfile) signatureLength() int {
	switch sp.CertMode {
	case CertNone:
		return 0
	case CertRC:
		return 4
	case CertCC:
		return 8
	case CertDS:
		return 0 // Not implemented; would vary by algorithm.
	default:
		return 0
	}
}

// cipherBlockSize returns the cipher block size for the configured KIc mode.
func (sp *SecurityProfile) cipherBlockSize() int {
	switch sp.KIcMode {
	case CipherAES_CBC:
		return 16
	default:
		return 8 // DES and 3DES all use 8-byte blocks.
	}
}

// padData pads data to a multiple of blockSize using zero-padding.
// Returns the padded data and PCNTR (number of padding bytes added).
func padData(data []byte, blockSize int) ([]byte, byte) {
	if len(data) == 0 {
		padded := make([]byte, blockSize)
		return padded, byte(blockSize)
	}
	remainder := len(data) % blockSize
	if remainder == 0 {
		out := make([]byte, len(data))
		copy(out, data)
		return out, 0
	}
	padLen := blockSize - remainder
	padded := make([]byte, len(data)+padLen)
	copy(padded, data)
	// Remaining bytes are already zero.
	return padded, byte(padLen)
}

// computeMAC computes a MAC using ISO 9797-1 Algorithm 3 (Retail MAC).
// For 3DES-CBC-2-keys with a 16-byte key (K1 || K2):
//  1. Single DES CBC-MAC with K1 over all blocks
//  2. Decrypt the final block with K2
//  3. Encrypt the result with K1
//
// Returns an 8-byte MAC.
func computeMAC(mode CipherMode, key []byte, header, data []byte) ([]byte, error) {
	// Combine header and data for MAC computation.
	macInput := append(header, data...)

	// Pad to 8-byte boundary if needed.
	if len(macInput)%8 != 0 {
		padLen := 8 - (len(macInput) % 8)
		macInput = append(macInput, make([]byte, padLen)...)
	}

	switch mode {
	case CipherDES_CBC:
		return computeRetailMACDES(key, macInput)
	case Cipher3DES_CBC_2Keys:
		return computeRetailMAC3DES2Keys(key, macInput)
	case Cipher3DES_CBC_3Keys:
		return computeRetailMAC3DES3Keys(key, macInput)
	default:
		return nil, fmt.Errorf("gsm0348: unsupported MAC cipher mode: %d", mode)
	}
}

// computeRetailMACDES computes a simple DES CBC-MAC.
func computeRetailMACDES(key, data []byte) ([]byte, error) {
	if len(key) < 8 {
		return nil, fmt.Errorf("gsm0348: DES key must be at least 8 bytes, got %d", len(key))
	}
	block, err := des.NewCipher(key[:8])
	if err != nil {
		return nil, err
	}

	mac := make([]byte, 8)
	for i := 0; i < len(data); i += 8 {
		xorBytes(mac, data[i:i+8])
		block.Encrypt(mac, mac)
	}
	return mac, nil
}

// computeRetailMAC3DES2Keys implements ISO 9797-1 Algorithm 3 with 2 DES keys.
func computeRetailMAC3DES2Keys(key []byte, data []byte) ([]byte, error) {
	if len(key) < 16 {
		return nil, fmt.Errorf("gsm0348: 3DES-2-key MAC requires 16-byte key, got %d", len(key))
	}

	k1 := key[:8]
	k2 := key[8:16]

	// Step 1: Single DES CBC-MAC with K1 over all blocks.
	block1, err := des.NewCipher(k1)
	if err != nil {
		return nil, err
	}

	mac := make([]byte, 8)
	for i := 0; i < len(data); i += 8 {
		xorBytes(mac, data[i:i+8])
		block1.Encrypt(mac, mac)
	}

	// Step 2: Decrypt last block result with K2.
	block2, err := des.NewCipher(k2)
	if err != nil {
		return nil, err
	}
	block2.Decrypt(mac, mac)

	// Step 3: Encrypt result with K1.
	block1.Encrypt(mac, mac)

	return mac, nil
}

// computeRetailMAC3DES3Keys implements ISO 9797-1 Algorithm 3 with 3 DES keys.
func computeRetailMAC3DES3Keys(key []byte, data []byte) ([]byte, error) {
	if len(key) < 24 {
		return nil, fmt.Errorf("gsm0348: 3DES-3-key MAC requires 24-byte key, got %d", len(key))
	}

	k1 := key[:8]
	k2 := key[8:16]
	k3 := key[16:24]

	// Step 1: Single DES CBC-MAC with K1 over all blocks.
	block1, err := des.NewCipher(k1)
	if err != nil {
		return nil, err
	}

	mac := make([]byte, 8)
	for i := 0; i < len(data); i += 8 {
		xorBytes(mac, data[i:i+8])
		block1.Encrypt(mac, mac)
	}

	// Step 2: Decrypt with K2.
	block2, err := des.NewCipher(k2)
	if err != nil {
		return nil, err
	}
	block2.Decrypt(mac, mac)

	// Step 3: Encrypt with K3.
	block3, err := des.NewCipher(k3)
	if err != nil {
		return nil, err
	}
	block3.Encrypt(mac, mac)

	return mac, nil
}

// encryptData encrypts data using the specified cipher mode.
// IV is all zeros. For 3DES-2-key, the 16-byte key is expanded to 24 bytes (K1|K2|K1).
func encryptData(mode CipherMode, key []byte, counter []byte, data []byte) ([]byte, error) {
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
		// Expand K1|K2 to K1|K2|K1.
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
		return nil, fmt.Errorf("gsm0348: unsupported encryption cipher mode: %d", mode)
	}

	if err != nil {
		return nil, err
	}

	blockSize := block.BlockSize()
	if len(data)%blockSize != 0 {
		return nil, fmt.Errorf("gsm0348: data length %d is not a multiple of block size %d", len(data), blockSize)
	}

	// IV is all zeros.
	iv := make([]byte, blockSize)
	cbc := cipher.NewCBCEncrypter(block, iv)

	encrypted := make([]byte, len(data))
	cbc.CryptBlocks(encrypted, data)
	return encrypted, nil
}

// computeCRC computes a simple CRC (XOR-based redundancy check) over
// the header and data, returning a slice of the requested length.
func computeCRC(header, data []byte, length int) []byte {
	combined := append(header, data...)
	crc := make([]byte, length)
	for i, b := range combined {
		crc[i%length] ^= b
	}
	return crc
}

// xorBytes XORs src into dst in place: dst[i] ^= src[i].
func xorBytes(dst, src []byte) {
	for i := 0; i < len(dst) && i < len(src); i++ {
		dst[i] ^= src[i]
	}
}

// algoName returns a string name for the algorithm byte.
func algoName(algo byte) string {
	switch algo {
	case 0x02:
		return "AES"
	default:
		return "DES"
	}
}

// cipherModeName returns a string name for a CipherMode.
func cipherModeName(mode CipherMode) string {
	switch mode {
	case CipherDES_CBC:
		return "DES_CBC"
	case Cipher3DES_CBC_2Keys:
		return "TRIPLE_DES_CBC_2_KEYS"
	case Cipher3DES_CBC_3Keys:
		return "TRIPLE_DES_CBC_3_KEYS"
	case CipherAES_CBC:
		return "AES_CBC"
	default:
		return "DES_CBC"
	}
}
