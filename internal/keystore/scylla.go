package keystore

import "context"

// ScyllaKeyReader abstracts the ScyllaDB card key read operation.
// Implemented by a thin adapter wrapping scylla.CardKeyStore (created at wiring time)
// to avoid an import cycle between keystore and scylla packages.
type ScyllaKeyReader interface {
	GetCardKeys(ctx context.Context, cardID string) (*ScyllaCardKeyRecord, error)
}

// ScyllaCardKeyRecord mirrors scylla.CardKeyRecord to avoid import cycles.
type ScyllaCardKeyRecord struct {
	CardID    string
	EncKey    []byte
	AuthKey   []byte
	KEK       []byte
	ProfileID string
	MSISDN    string
}

// ScyllaKeyStore implements KeyStore by reading card keys from ScyllaDB.
type ScyllaKeyStore struct {
	reader ScyllaKeyReader
}

// NewScyllaKeyStore creates a ScyllaKeyStore backed by the given reader.
func NewScyllaKeyStore(reader ScyllaKeyReader) *ScyllaKeyStore {
	return &ScyllaKeyStore{reader: reader}
}

// GetKeys retrieves card key material from ScyllaDB.
func (s *ScyllaKeyStore) GetKeys(ctx context.Context, cardID string) (*CardKeyMaterial, error) {
	record, err := s.reader.GetCardKeys(ctx, cardID)
	if err != nil {
		return nil, err
	}
	return &CardKeyMaterial{
		EncKey:    record.EncKey,
		AuthKey:   record.AuthKey,
		KEK:       record.KEK,
		ProfileID: record.ProfileID,
		MSISDN:    record.MSISDN,
	}, nil
}
