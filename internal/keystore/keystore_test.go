package keystore

import (
	"context"
	"testing"
)

func TestCardKeyMaterial_HasRequiredFields(t *testing.T) {
	m := &CardKeyMaterial{
		EncKey:    []byte{0x01, 0x02},
		AuthKey:   []byte{0x03, 0x04},
		KEK:       []byte{0x05, 0x06},
		ProfileID: "test-profile",
		MSISDN:    "+1234567890",
	}

	if len(m.EncKey) != 2 {
		t.Errorf("EncKey length = %d, want 2", len(m.EncKey))
	}
	if m.ProfileID != "test-profile" {
		t.Errorf("ProfileID = %q, want %q", m.ProfileID, "test-profile")
	}
	if m.MSISDN != "+1234567890" {
		t.Errorf("MSISDN = %q, want %q", m.MSISDN, "+1234567890")
	}
}

func TestCryptoProvider_InterfaceSatisfied(t *testing.T) {
	var _ CryptoProvider = &mockCryptoProvider{}
}

type mockCryptoProvider struct{}

func (m *mockCryptoProvider) ComputeMAC(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error) {
	return []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, nil
}

func (m *mockCryptoProvider) Encrypt(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error) {
	return data, nil
}
