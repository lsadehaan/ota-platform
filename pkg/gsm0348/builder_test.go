package gsm0348

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// testVector represents a single test vector loaded from JSON.
type testVector struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`

	// Security profile fields (using Java-style enum names for compatibility).
	CertMode    string `json:"certMode"`
	Ciphered    bool   `json:"ciphered"`
	CounterMode string `json:"counterMode"`
	PoRMode     string `json:"porMode"`
	PoRCertMode string `json:"porCertMode"`
	PoRCiphered bool   `json:"porCiphered"`
	PoRProtocol int    `json:"porProtocol"`

	KIcAlgo     int    `json:"kicAlgo"`
	KIcMode     string `json:"kicMode"`
	KIcKeysetID int    `json:"kicKeysetID"`
	KIDAlgo     int    `json:"kidAlgo"`
	KIDMode     string `json:"kidMode"`
	KIDKeysetID int    `json:"kidKeysetID"`

	// Input fields (hex-encoded).
	TAR          string `json:"tar"`
	Counter      string `json:"counter"`
	CipheringKey string `json:"cipheringKey"`
	SigningKey   string `json:"signingKey"`
	UserData     string `json:"userData"`

	// Expected output (hex-encoded).
	Expected string `json:"expected"`
}

type testVectorFile struct {
	Vectors []testVector `json:"vectors"`
}

// mapCertMode converts a Java-style enum name to a Go CertificationMode.
func mapCertMode(s string) CertificationMode {
	switch strings.ToUpper(s) {
	case "NO_SECURITY", "NONE", "":
		return CertNone
	case "RC", "REDUNDANCY_CHECK":
		return CertRC
	case "CC", "CRYPTOGRAPHIC_CHECKSUM":
		return CertCC
	case "DS", "DIGITAL_SIGNATURE":
		return CertDS
	default:
		return CertNone
	}
}

// mapCounterMode converts a Java-style enum name to a Go CounterMode.
func mapCounterMode(s string) CounterMode {
	switch strings.ToUpper(s) {
	case "NO_COUNTER", "NONE", "":
		return CounterNone
	case "COUNTER_NO_REPLAY_OR_CHECK", "NO_REPLAY":
		return CounterNoReplay
	case "COUNTER_MUST_BE_HIGHER", "REPLAY_CHECK":
		return CounterReplayCheck
	default:
		return CounterNone
	}
}

// mapPoRMode converts a Java-style enum name to a Go PoRMode.
func mapPoRMode(s string) PoRMode {
	switch strings.ToUpper(s) {
	case "NO_REPLY", "NONE", "":
		return PoRNone
	case "ALWAYS", "POR_REQUIRED":
		return PoRAlways
	case "ON_ERROR", "POR_ONLY_ON_ERROR":
		return PoROnError
	default:
		return PoRNone
	}
}

// mapCipherMode converts a Java-style enum name to a Go CipherMode.
func mapCipherMode(s string) CipherMode {
	switch strings.ToUpper(s) {
	case "DES_CBC", "DES":
		return CipherDES_CBC
	case "TRIPLE_DES_CBC_2_KEYS", "3DES_CBC_2_KEYS", "3DES_2_KEYS":
		return Cipher3DES_CBC_2Keys
	case "TRIPLE_DES_CBC_3_KEYS", "3DES_CBC_3_KEYS", "3DES_3_KEYS":
		return Cipher3DES_CBC_3Keys
	case "AES_CBC", "AES":
		return CipherAES_CBC
	default:
		return CipherDES_CBC
	}
}

// mustDecodeHex decodes a hex string, stripping whitespace, or fails the test.
func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "\n", "")
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("failed to decode hex %q: %v", s, err)
	}
	return b
}

func TestBuildCommandPacket_GoldenVectors(t *testing.T) {
	const vectorPath = "../../testdata/test_vectors.json"

	data, err := os.ReadFile(vectorPath)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("test vectors file not found at %s, skipping golden vector tests", vectorPath)
			return
		}
		t.Fatalf("failed to read test vectors: %v", err)
	}

	var tvf testVectorFile
	if err := json.Unmarshal(data, &tvf); err != nil {
		t.Fatalf("failed to parse test vectors JSON: %v", err)
	}

	if len(tvf.Vectors) == 0 {
		t.Skip("no test vectors found in file")
	}

	for _, tv := range tvf.Vectors {
		t.Run(tv.Name, func(t *testing.T) {
			sp := &SecurityProfile{
				CertMode:    mapCertMode(tv.CertMode),
				Ciphered:    tv.Ciphered,
				CounterMode: mapCounterMode(tv.CounterMode),
				PoRMode:     mapPoRMode(tv.PoRMode),
				PoRCertMode: mapCertMode(tv.PoRCertMode),
				PoRCiphered: tv.PoRCiphered,
				PoRProtocol: byte(tv.PoRProtocol),
				KIcAlgo:     byte(tv.KIcAlgo),
				KIcMode:     mapCipherMode(tv.KIcMode),
				KIcKeysetID: byte(tv.KIcKeysetID),
				KIDAlgo:     byte(tv.KIDAlgo),
				KIDMode:     mapCipherMode(tv.KIDMode),
				KIDKeysetID: byte(tv.KIDKeysetID),
			}

			tarBytes := mustDecodeHex(t, tv.TAR)
			counterBytes := mustDecodeHex(t, tv.Counter)
			cipherKey := mustDecodeHex(t, tv.CipheringKey)
			signingKey := mustDecodeHex(t, tv.SigningKey)
			userData := mustDecodeHex(t, tv.UserData)
			expected := mustDecodeHex(t, tv.Expected)

			var tar [3]byte
			copy(tar[:], tarBytes)

			var counter [5]byte
			copy(counter[:], counterBytes)

			input := &CommandPacketInput{
				TAR:          tar,
				Counter:      counter,
				CipheringKey: cipherKey,
				SigningKey:   signingKey,
				UserData:     userData,
			}

			result, err := sp.BuildCommandPacket(input)
			if err != nil {
				t.Fatalf("BuildCommandPacket failed: %v", err)
			}

			resultHex := strings.ToUpper(hex.EncodeToString(result))
			expectedHex := strings.ToUpper(hex.EncodeToString(expected))

			if resultHex != expectedHex {
				t.Errorf("packet mismatch\n  got:  %s\n  want: %s", resultHex, expectedHex)
			}
		})
	}
}

// TestBuildCommandPacket_Basic performs a basic smoke test of packet construction
// without needing external test vector files.
func TestBuildCommandPacket_Basic(t *testing.T) {
	sp := &SecurityProfile{
		CertMode:    CertNone,
		Ciphered:    false,
		CounterMode: CounterNone,
		PoRMode:     PoRNone,
		PoRCertMode: CertNone,
		PoRCiphered: false,
		PoRProtocol: 0x00,
		KIcAlgo:     0x01,
		KIcMode:     Cipher3DES_CBC_2Keys,
		KIcKeysetID: 0x01,
		KIDAlgo:     0x01,
		KIDMode:     Cipher3DES_CBC_2Keys,
		KIDKeysetID: 0x01,
	}

	input := &CommandPacketInput{
		TAR:     [3]byte{0xB0, 0x00, 0x10},
		Counter: [5]byte{0x00, 0x00, 0x00, 0x00, 0x01},
	}

	result, err := sp.BuildCommandPacket(input)
	if err != nil {
		t.Fatalf("BuildCommandPacket failed: %v", err)
	}

	// Verify basic structure.
	if len(result) < 16 {
		t.Fatalf("packet too short: %d bytes", len(result))
	}

	// CPL is first 2 bytes.
	cpl := int(result[0])<<8 | int(result[1])
	if cpl != len(result)-2 {
		t.Errorf("CPL mismatch: CPL=%d, but packet has %d bytes after CPL", cpl, len(result)-2)
	}

	// CHL should be 13 (no signature).
	chl := int(result[2])
	if chl != 13 {
		t.Errorf("CHL mismatch: got %d, want 13", chl)
	}

	// TAR at offset 7 (after CPL(2) + CHL(1) + SPI(2) + KIc(1) + KID(1)).
	if result[7] != 0xB0 || result[8] != 0x00 || result[9] != 0x10 {
		t.Errorf("TAR mismatch: got %02X%02X%02X, want B00010", result[7], result[8], result[9])
	}
}

// TestBuildCommandPacket_WithCC tests packet construction with cryptographic checksum.
func TestBuildCommandPacket_WithCC(t *testing.T) {
	sp := &SecurityProfile{
		CertMode:    CertCC,
		Ciphered:    false,
		CounterMode: CounterNoReplay,
		PoRMode:     PoRAlways,
		PoRCertMode: CertCC,
		PoRCiphered: false,
		PoRProtocol: 0x01,
		KIcAlgo:     0x01,
		KIcMode:     Cipher3DES_CBC_2Keys,
		KIcKeysetID: 0x01,
		KIDAlgo:     0x01,
		KIDMode:     Cipher3DES_CBC_2Keys,
		KIDKeysetID: 0x01,
	}

	signingKey := make([]byte, 16)
	for i := range signingKey {
		signingKey[i] = byte(i + 1)
	}

	input := &CommandPacketInput{
		TAR:          [3]byte{0xB0, 0x00, 0x10},
		Counter:      [5]byte{0x00, 0x00, 0x00, 0x00, 0x01},
		CipheringKey: make([]byte, 16),
		SigningKey:   signingKey,
		UserData:     []byte{0xA0, 0xA4, 0x00, 0x00, 0x02, 0x3F, 0x00},
	}

	result, err := sp.BuildCommandPacket(input)
	if err != nil {
		t.Fatalf("BuildCommandPacket failed: %v", err)
	}

	// CHL should be 21 (13 + 8 for CC signature).
	chl := int(result[2])
	if chl != 21 {
		t.Errorf("CHL mismatch: got %d, want 21", chl)
	}

	// Verify CPL consistency.
	cpl := int(result[0])<<8 | int(result[1])
	if cpl != len(result)-2 {
		t.Errorf("CPL mismatch: CPL=%d, but packet has %d bytes after CPL", cpl, len(result)-2)
	}
}

// TestBuildCommandPacket_WithCipherAndCC tests packet construction with
// both encryption and cryptographic checksum.
func TestBuildCommandPacket_WithCipherAndCC(t *testing.T) {
	sp := &SecurityProfile{
		CertMode:    CertCC,
		Ciphered:    true,
		CounterMode: CounterReplayCheck,
		PoRMode:     PoRAlways,
		PoRCertMode: CertCC,
		PoRCiphered: true,
		PoRProtocol: 0x01,
		KIcAlgo:     0x01,
		KIcMode:     Cipher3DES_CBC_2Keys,
		KIcKeysetID: 0x01,
		KIDAlgo:     0x01,
		KIDMode:     Cipher3DES_CBC_2Keys,
		KIDKeysetID: 0x01,
	}

	cipherKey := make([]byte, 16)
	signingKey := make([]byte, 16)
	for i := range cipherKey {
		cipherKey[i] = byte(i + 0x10)
		signingKey[i] = byte(i + 0x20)
	}

	input := &CommandPacketInput{
		TAR:          [3]byte{0xB0, 0x00, 0x10},
		Counter:      [5]byte{0x00, 0x00, 0x00, 0x00, 0x05},
		CipheringKey: cipherKey,
		SigningKey:   signingKey,
		UserData:     []byte{0xA0, 0xA4, 0x00, 0x00, 0x02, 0x3F, 0x00},
	}

	result, err := sp.BuildCommandPacket(input)
	if err != nil {
		t.Fatalf("BuildCommandPacket failed: %v", err)
	}

	// Verify basic structure.
	cpl := int(result[0])<<8 | int(result[1])
	if cpl != len(result)-2 {
		t.Errorf("CPL mismatch: CPL=%d, but packet has %d bytes after CPL", cpl, len(result)-2)
	}

	// CHL should be 21 (13 + 8 for CC signature).
	chl := int(result[2])
	if chl != 21 {
		t.Errorf("CHL mismatch: got %d, want 21", chl)
	}

	// Encrypted portion should be a multiple of 8 bytes (DES block size).
	encryptedLen := len(result) - 2 - 1 - 13 // total - CPL - CHL - header
	if encryptedLen%8 != 0 {
		t.Errorf("encrypted data length %d is not a multiple of 8", encryptedLen)
	}
}

// TestPadData verifies padding behavior.
func TestPadData(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		blockSize int
		wantLen   int
		wantPCNTR byte
	}{
		{"empty", []byte{}, 8, 8, 8},
		{"exact block", []byte{1, 2, 3, 4, 5, 6, 7, 8}, 8, 8, 0},
		{"needs padding", []byte{1, 2, 3}, 8, 8, 5},
		{"two blocks exact", make([]byte, 16), 8, 16, 0},
		{"one byte short", make([]byte, 7), 8, 8, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			padded, pcntr := padData(tt.data, tt.blockSize)
			if len(padded) != tt.wantLen {
				t.Errorf("padded length = %d, want %d", len(padded), tt.wantLen)
			}
			if pcntr != tt.wantPCNTR {
				t.Errorf("PCNTR = %d, want %d", pcntr, tt.wantPCNTR)
			}
		})
	}
}

// TestSPIEncoding verifies SPI byte encoding.
func TestSPIEncoding(t *testing.T) {
	sp := &SecurityProfile{
		CertMode:    CertCC,       // 0x02
		Ciphered:    true,         // bit 2
		CounterMode: CounterReplayCheck, // 0x02 << 3
		PoRMode:     PoRAlways,    // 0x01
		PoRCertMode: CertCC,      // 0x02 << 2
		PoRCiphered: true,        // bit 4
		PoRProtocol: 0x01,        // 0x01 << 5
	}

	spi1 := sp.encodeSPI1()
	// CertCC=0x02 | Ciphered=0x04 | CounterReplayCheck=0x02<<3=0x10
	// = 0x02 | 0x04 | 0x10 = 0x16
	if spi1 != 0x16 {
		t.Errorf("SPI1 = 0x%02X, want 0x16", spi1)
	}

	spi2 := sp.encodeSPI2()
	// PoRAlways=0x01 | PoRCertCC=0x02<<2=0x08 | PoRCiphered=0x10 | PoRProtocol=0x01<<5=0x20
	// = 0x01 | 0x08 | 0x10 | 0x20 = 0x39
	if spi2 != 0x39 {
		t.Errorf("SPI2 = 0x%02X, want 0x39", spi2)
	}
}

// TestKIcKIDEncoding verifies KIc and KID byte encoding.
func TestKIcKIDEncoding(t *testing.T) {
	sp := &SecurityProfile{
		KIcAlgo:     0x01,
		KIcMode:     Cipher3DES_CBC_2Keys, // 0x01 << 2
		KIcKeysetID: 0x03,                 // 0x03 << 4
		KIDAlgo:     0x01,
		KIDMode:     Cipher3DES_CBC_2Keys,
		KIDKeysetID: 0x02,
	}

	kic := sp.encodeKIc()
	// algo=0x01 | mode=0x01<<2=0x04 | keyset=0x03<<4=0x30 = 0x35
	if kic != 0x35 {
		t.Errorf("KIc = 0x%02X, want 0x35", kic)
	}

	kid := sp.encodeKID()
	// algo=0x01 | mode=0x01<<2=0x04 | keyset=0x02<<4=0x20 = 0x25
	if kid != 0x25 {
		t.Errorf("KID = 0x%02X, want 0x25", kid)
	}
}

// TestBuildCommandPacket_WithCryptoProvider tests that the builder delegates
// MAC and encryption to a CryptoProvider when one is provided.
func TestBuildCommandPacket_WithCryptoProvider(t *testing.T) {
	sp := &SecurityProfile{
		CertMode:    CertCC,
		Ciphered:    true,
		CounterMode: CounterReplayCheck,
		PoRMode:     PoRAlways,
		PoRCertMode: CertCC,
		PoRCiphered: false,
		PoRProtocol: 0x01,
		KIcAlgo:     0x01,
		KIcMode:     Cipher3DES_CBC_2Keys,
		KIcKeysetID: 0x01,
		KIDAlgo:     0x01,
		KIDMode:     Cipher3DES_CBC_2Keys,
		KIDKeysetID: 0x01,
	}

	cipherKey := make([]byte, 16)
	signingKey := make([]byte, 16)
	for i := range cipherKey {
		cipherKey[i] = byte(i + 0x10)
		signingKey[i] = byte(i + 0x20)
	}

	// Build with raw keys (baseline).
	inputRaw := &CommandPacketInput{
		TAR:          [3]byte{0xB0, 0x00, 0x10},
		Counter:      [5]byte{0x00, 0x00, 0x00, 0x00, 0x05},
		CipheringKey: cipherKey,
		SigningKey:   signingKey,
		UserData:     []byte{0xA0, 0xA4, 0x00, 0x00, 0x02, 0x3F, 0x00},
	}

	resultRaw, err := sp.BuildCommandPacket(inputRaw)
	if err != nil {
		t.Fatalf("raw BuildCommandPacket failed: %v", err)
	}

	// Build with CryptoProvider that uses the same keys internally.
	provider := &testCryptoProvider{cipherKey: cipherKey, signingKey: signingKey}
	inputCP := &CommandPacketInput{
		TAR:            [3]byte{0xB0, 0x00, 0x10},
		Counter:        [5]byte{0x00, 0x00, 0x00, 0x00, 0x05},
		CryptoProvider: provider,
		CardID:         "test-card-id",
		UserData:       []byte{0xA0, 0xA4, 0x00, 0x00, 0x02, 0x3F, 0x00},
	}

	resultCP, err := sp.BuildCommandPacket(inputCP)
	if err != nil {
		t.Fatalf("CryptoProvider BuildCommandPacket failed: %v", err)
	}

	// Both should produce identical output.
	if hex.EncodeToString(resultRaw) != hex.EncodeToString(resultCP) {
		t.Errorf("CryptoProvider result differs from raw key result\n  raw: %s\n  cp:  %s",
			hex.EncodeToString(resultRaw), hex.EncodeToString(resultCP))
	}

	if !provider.macCalled {
		t.Error("CryptoProvider.ComputeMAC was not called")
	}
	if !provider.encryptCalled {
		t.Error("CryptoProvider.Encrypt was not called")
	}
}

// testCryptoProvider delegates to the existing software crypto using the provided keys.
type testCryptoProvider struct {
	cipherKey     []byte
	signingKey    []byte
	macCalled     bool
	encryptCalled bool
}

func (p *testCryptoProvider) ComputeMAC(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error) {
	p.macCalled = true
	cipherMode := mapCipherMode(mode)
	return computeMAC(cipherMode, p.signingKey, nil, data)
}

func (p *testCryptoProvider) Encrypt(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error) {
	p.encryptCalled = true
	cipherMode := mapCipherMode(mode)
	return encryptData(cipherMode, p.cipherKey, nil, data)
}
