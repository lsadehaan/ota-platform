package keystore

import "context"

// CardKeyMaterial holds the cryptographic keys and metadata for a SIM card.
type CardKeyMaterial struct {
	EncKey    []byte // KIC — ciphering key
	AuthKey   []byte // KID — signing/MAC key
	KEK       []byte // Key encryption key (for key provisioning to card)
	ProfileID string
	MSISDN    string
}

// KeyStore retrieves key material for a card.
type KeyStore interface {
	GetKeys(ctx context.Context, cardID string) (*CardKeyMaterial, error)
}
