package pgstore

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"gorm.io/gorm"

	"ota-platform/internal/db"
)

// MSISDNResolver implements transport.MSISDNResolver backed by Postgres
// with a process-local sync.Map cache.
type MSISDNResolver struct {
	db    *gorm.DB
	cache sync.Map // msisdn -> cardID (string)
}

// NewMSISDNResolver creates a new Postgres-backed MSISDN resolver.
// Automatically preloads all MSISDN → cardID mappings into RAM at startup.
func NewMSISDNResolver(database *gorm.DB) *MSISDNResolver {
	r := &MSISDNResolver{db: database}
	r.preloadAll()
	return r
}

// preloadAll bulk-loads all MSISDN → cardID mappings from the cards table.
// With 200k cards this is ~11 MB in RAM — trivially small.
func (r *MSISDNResolver) preloadAll() {
	type row struct {
		ID     string `gorm:"column:id"`
		MSISDN string `gorm:"column:msisdn"`
	}
	var rows []row
	if err := r.db.Table("cards").Select("id, msisdn").Where("msisdn IS NOT NULL AND msisdn != ''").Find(&rows).Error; err != nil {
		return // non-fatal — lazy lookups will fill cache on demand
	}
	for _, r2 := range rows {
		r.cache.Store(r2.MSISDN, r2.ID)
	}
}

// PreloadMSISDNs populates the cache with MSISDN→cardID mappings.
// Called by the batch preloader after loading card keys (which include MSISDN).
func (r *MSISDNResolver) PreloadMSISDNs(mappings map[string]string) {
	for msisdn, cardID := range mappings {
		r.cache.Store(msisdn, cardID)
	}
}

// LookupCardByMSISDN resolves an MSISDN to a card ID. Returns ("", nil) if
// no card is found.
func (r *MSISDNResolver) LookupCardByMSISDN(ctx context.Context, msisdn string) (string, error) {
	if v, ok := r.cache.Load(msisdn); ok {
		return v.(string), nil
	}

	var card db.Card
	err := r.db.WithContext(ctx).Select("id").Where("msisdn = ?", msisdn).First(&card).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("lookup card by msisdn: %w", err)
	}

	cardID := card.ID.String()
	r.cache.Store(msisdn, cardID)
	return cardID, nil
}
