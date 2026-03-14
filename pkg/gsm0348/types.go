package gsm0348

import "context"

// CertificationMode defines the cryptographic checksum mode used in SPI1.
type CertificationMode byte

const (
	CertNone CertificationMode = 0x00
	CertRC   CertificationMode = 0x01
	CertCC   CertificationMode = 0x02
	CertDS   CertificationMode = 0x03
)

// CounterMode defines the counter replay protection mode used in SPI1.
type CounterMode byte

const (
	CounterNone        CounterMode = 0x00
	CounterNoReplay    CounterMode = 0x01
	CounterReplayCheck CounterMode = 0x02
)

// PoRMode defines the Proof of Receipt mode used in SPI2.
type PoRMode byte

const (
	PoRNone    PoRMode = 0x00
	PoRAlways  PoRMode = 0x01
	PoROnError PoRMode = 0x02
)

// CipherMode defines the cipher algorithm mode for KIc / KID.
type CipherMode byte

const (
	CipherDES_CBC        CipherMode = 0x00
	Cipher3DES_CBC_2Keys CipherMode = 0x01
	Cipher3DES_CBC_3Keys CipherMode = 0x02
	CipherAES_CBC        CipherMode = 0x03
)

// Response status codes returned in GSM 03.48 response packets.
const (
	RespOK                     = 0x00
	RespRCCCDSFailed           = 0x01
	RespCNTRLow                = 0x02
	RespCNTRHighNoUpdate       = 0x03
	RespCNTRHighUpdated        = 0x04
	RespCNTRBlocked            = 0x05
	RespCryptError             = 0x06
	RespInsNotSupported        = 0x07
	RespCNTRIncreaseNotAllowed = 0x08
	RespTARUnknown             = 0x09
	RespInsufficientMemory     = 0x0A
)

// SecurityProfile holds all the security parameters needed to build
// GSM 03.48 command packets and parse response packets.
type SecurityProfile struct {
	CertMode    CertificationMode
	Ciphered    bool
	CounterMode CounterMode
	PoRMode     PoRMode
	PoRCertMode CertificationMode
	PoRCiphered bool
	PoRProtocol byte // 0x01 = SMS-DELIVER-REPORT, 0x02 = SMS-SUBMIT

	KIcAlgo     byte
	KIcMode     CipherMode
	KIcKeysetID byte

	KIDAlgo     byte
	KIDMode     CipherMode
	KIDKeysetID byte

	// SecurityBytesWithLengthsAndUDHL controls whether the CPL field includes
	// the UDHL and surrounding length bytes in the total count.
	SecurityBytesWithLengthsAndUDHL bool
}

// CryptoProvider performs MAC and encryption without exposing raw key material.
type CryptoProvider interface {
	ComputeMAC(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error)
	Encrypt(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error)
}

// CommandPacketInput provides all variable data needed to build a single
// GSM 03.48 command packet.
type CommandPacketInput struct {
	TAR          [3]byte
	Counter      [5]byte
	CipheringKey []byte // Used when CryptoProvider is nil
	SigningKey    []byte // Used when CryptoProvider is nil
	UserData     []byte

	// CryptoProvider, when set, is used for MAC and encryption operations
	// instead of the raw CipheringKey/SigningKey fields above.
	CryptoProvider CryptoProvider
	// CardID is required when CryptoProvider is set, to identify which
	// card's keys to use for the crypto operations.
	CardID string
}

// ResponsePacket holds the parsed fields of a GSM 03.48 response packet.
type ResponsePacket struct {
	TAR        [3]byte
	Counter    [5]byte
	PCNTR      byte
	StatusCode byte
	Signature  []byte
	Data       []byte
}
