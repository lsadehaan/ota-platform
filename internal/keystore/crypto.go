package keystore

import "context"

// CryptoProvider performs MAC computation and encryption without exposing
// raw key material. When available, the GSM 03.48 builder delegates crypto
// operations to this interface instead of using software crypto with raw keys.
type CryptoProvider interface {
	ComputeMAC(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error)
	Encrypt(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error)
}
