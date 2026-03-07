package scylla

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"ota-platform/internal/db"
	kafkapkg "ota-platform/internal/kafka"
)

type MessageLogStore struct {
	client *Client
}

type metricDelta struct {
	HourBucket       time.Time
	CampaignID       *gocql.UUID
	CampaignBucket   int
	Total            int64
	MT               int64
	MO               int64
	Delivered        int64
	Undelivered      int64
	ErrorCountDeltas []errorCounterDelta
}

func NewMessageLogStore(client *Client) *MessageLogStore {
	return &MessageLogStore{client: client}
}

func (s *MessageLogStore) CreateBatch(ctx context.Context, logs []db.MessageLog, _ int) error {
	batch := s.client.Session().NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	counterBatch := s.client.Session().NewBatch(gocql.CounterBatch).WithContext(ctx)
	for _, log := range logs {
		cardID, err := gocql.ParseUUID(log.CardID.String())
		if err != nil {
			return fmt.Errorf("parse card_id: %w", err)
		}
		msgID, err := gocql.ParseUUID(log.ID.String())
		if err != nil {
			return fmt.Errorf("parse msg_id: %w", err)
		}
		var campaignID *gocql.UUID
		if log.CampaignID != nil {
			parsed, err := gocql.ParseUUID(log.CampaignID.String())
			if err != nil {
				return fmt.Errorf("parse campaign_id: %w", err)
			}
			campaignID = &parsed
		}

		cardBucket := s.client.CardBucket(log.CardID.String())
		timeBucket := s.client.TimeBucket(log.CreatedAt)
		globalBucket := s.client.GlobalBucket(log.ID.String())
		campaignBucket := 0
		if log.CampaignID != nil {
			campaignBucket = s.client.CampaignBucket(log.CampaignID.String(), log.CardID.String())
		}

		batch.Query(`INSERT INTO message_by_id (
			msg_id, card_id, card_bucket, campaign_id, campaign_bucket, time_bucket, created_at,
			updated_at, direction, status, smpp_message_id, dlr_status, counter_hex, por_status_code, error_code,
			raw_payload, secured_payload, por_data
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			msgID, cardID, cardBucket, campaignID, campaignBucket, timeBucket, log.CreatedAt.UTC(),
			log.CreatedAt.UTC(), log.Direction, log.Status, log.SMPPMessageID, log.DLRStatus, log.CounterHex, log.PORStatusCode, nil,
			log.RawPayload, log.SecuredPayload, log.PORData,
		)

		batch.Query(`INSERT INTO message_by_card_time (
			card_bucket, card_id, time_bucket, created_at, msg_id, campaign_id, updated_at,
			direction, status, smpp_message_id, dlr_status, counter_hex, por_status_code, error_code,
			raw_payload, secured_payload, por_data
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			cardBucket, cardID, timeBucket, log.CreatedAt.UTC(), msgID, campaignID,
			log.CreatedAt.UTC(), log.Direction, log.Status, log.SMPPMessageID, log.DLRStatus, log.CounterHex, log.PORStatusCode, nil,
			log.RawPayload, log.SecuredPayload, log.PORData,
		)

		if campaignID != nil {
			batch.Query(`INSERT INTO message_by_campaign_bucket_time (
				campaign_id, campaign_bucket, time_bucket, created_at, card_id, msg_id, updated_at,
				direction, status, smpp_message_id, dlr_status, por_status_code, error_code
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				*campaignID, campaignBucket, timeBucket, log.CreatedAt.UTC(), cardID, msgID,
				log.CreatedAt.UTC(), log.Direction, log.Status, log.SMPPMessageID, log.DLRStatus, log.PORStatusCode, nil,
			)
		}

		batch.Query(`INSERT INTO message_by_time_bucket (
			time_bucket, global_bucket, created_at, msg_id, card_id, campaign_id, updated_at,
			direction, status, smpp_message_id, dlr_status, por_status_code, error_code
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			timeBucket, globalBucket, log.CreatedAt.UTC(), msgID, cardID, campaignID, log.CreatedAt.UTC(),
			log.Direction, log.Status, log.SMPPMessageID, log.DLRStatus, log.PORStatusCode, nil,
		)

		batch.Query(`INSERT INTO message_by_direction_time_bucket (
			direction, time_bucket, global_bucket, created_at, msg_id, card_id, campaign_id, updated_at,
			status, smpp_message_id, dlr_status, por_status_code, error_code
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			log.Direction, timeBucket, globalBucket, log.CreatedAt.UTC(), msgID, cardID, campaignID, log.CreatedAt.UTC(),
			log.Status, log.SMPPMessageID, log.DLRStatus, log.PORStatusCode, nil,
		)
		if log.Status != "" {
			batch.Query(`INSERT INTO message_by_status_time_bucket (
				status, time_bucket, global_bucket, created_at, msg_id, card_id, campaign_id, updated_at,
				direction, smpp_message_id, dlr_status, por_status_code, error_code
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				log.Status, timeBucket, globalBucket, log.CreatedAt.UTC(), msgID, cardID, campaignID, log.CreatedAt.UTC(),
				log.Direction, log.SMPPMessageID, log.DLRStatus, log.PORStatusCode, nil,
			)
		}
		if log.DLRStatus != nil && *log.DLRStatus != "" {
			batch.Query(`INSERT INTO message_by_dlr_status_time_bucket (
				dlr_status, time_bucket, global_bucket, created_at, msg_id, card_id, campaign_id, updated_at,
				direction, status, smpp_message_id, por_status_code, error_code
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				*log.DLRStatus, timeBucket, globalBucket, log.CreatedAt.UTC(), msgID, cardID, campaignID, log.CreatedAt.UTC(),
				log.Direction, log.Status, log.SMPPMessageID, log.PORStatusCode, nil,
			)
		}

		hourBucket := log.CreatedAt.UTC().Truncate(time.Hour)
		mtDelta, moDelta := directionCounters(log.Direction)
		deliveredDelta, undeliveredDelta := dlrCounters(log.DLRStatus)
		addMetricCounterQueries(counterBatch, metricDelta{
			HourBucket:       hourBucket,
			CampaignID:       campaignID,
			CampaignBucket:   campaignBucket,
			Total:            1,
			MT:               mtDelta,
			MO:               moDelta,
			Delivered:        deliveredDelta,
			Undelivered:      undeliveredDelta,
			ErrorCountDeltas: errorCounterDeltas(nil, log.Status, nil, log.DLRStatus, nil, log.PORStatusCode),
		})
	}
	if len(batch.Entries) > 0 {
		if err := s.client.Session().ExecuteBatch(batch); err != nil {
			return err
		}
	}
	if len(counterBatch.Entries) > 0 {
		if err := s.client.Session().ExecuteBatch(counterBatch); err != nil {
			return err
		}
	}
	return nil
}

func (s *MessageLogStore) ApplyUpdates(ctx context.Context, updates []kafkapkg.MessageLogAction) error {
	for _, update := range updates {
		if update.ID == "" || update.Updates == nil {
			continue
		}
		if err := s.applyUpdate(ctx, update); err != nil {
			return err
		}
	}
	return nil
}

func (s *MessageLogStore) applyUpdate(ctx context.Context, update kafkapkg.MessageLogAction) error {
	msgID, err := gocql.ParseUUID(update.ID)
	if err != nil {
		return fmt.Errorf("parse update message id: %w", err)
	}

	var (
		cardID         gocql.UUID
		cardBucket     int
		campaignID     gocql.UUID
		campaignBucket int
		globalBucket   int
		timeBucket     string
		createdAt      time.Time
		direction      string
		status         string
		smppMessageID  *string
		dlrStatus      *string
		porStatusCode  *int16
		errorCode      *string
	)
	if err := s.client.Session().Query(
		`SELECT card_id, card_bucket, campaign_id, campaign_bucket, time_bucket, created_at, direction, status, smpp_message_id, dlr_status, por_status_code, error_code FROM message_by_id WHERE msg_id = ?`,
		msgID,
	).WithContext(ctx).Consistency(gocql.One).Scan(&cardID, &cardBucket, &campaignID, &campaignBucket, &timeBucket, &createdAt, &direction, &status, &smppMessageID, &dlrStatus, &porStatusCode, &errorCode); err != nil {
		if err == gocql.ErrNotFound {
			return nil
		}
		return fmt.Errorf("load message lookup: %w", err)
	}
	globalBucket = s.client.GlobalBucket(update.ID)
	updatedAt := time.Now().UTC()

	setByID, valuesByID := buildMessageUpdateSet(update.Updates, updatedAt, map[string]string{
		"status":          "status",
		"smpp_message_id": "smpp_message_id",
		"dlr_status":      "dlr_status",
		"counter_hex":     "counter_hex",
		"por_status_code": "por_status_code",
		"por_data":        "por_data",
		"error_code":      "error_code",
	})
	setByCard, valuesByCard := buildMessageUpdateSet(update.Updates, updatedAt, map[string]string{
		"status":          "status",
		"smpp_message_id": "smpp_message_id",
		"dlr_status":      "dlr_status",
		"counter_hex":     "counter_hex",
		"por_status_code": "por_status_code",
		"por_data":        "por_data",
		"error_code":      "error_code",
	})
	setByCampaign, valuesByCampaign := buildMessageUpdateSet(update.Updates, updatedAt, map[string]string{
		"status":          "status",
		"smpp_message_id": "smpp_message_id",
		"dlr_status":      "dlr_status",
		"por_status_code": "por_status_code",
		"error_code":      "error_code",
	})
	setByTimeBucket, valuesByTimeBucket := buildMessageUpdateSet(update.Updates, updatedAt, map[string]string{
		"status":          "status",
		"smpp_message_id": "smpp_message_id",
		"dlr_status":      "dlr_status",
		"por_status_code": "por_status_code",
		"error_code":      "error_code",
	})
	setByDirection, valuesByDirection := buildMessageUpdateSet(update.Updates, updatedAt, map[string]string{
		"status":          "status",
		"smpp_message_id": "smpp_message_id",
		"dlr_status":      "dlr_status",
		"por_status_code": "por_status_code",
		"error_code":      "error_code",
	})
	if setByID == "" && setByCard == "" && setByCampaign == "" && setByTimeBucket == "" && setByDirection == "" {
		return nil
	}

	batch := s.client.Session().NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	counterBatch := s.client.Session().NewBatch(gocql.CounterBatch).WithContext(ctx)
	if setByID != "" {
		batch.Query(
			fmt.Sprintf("UPDATE message_by_id SET %s WHERE msg_id = ?", setByID),
			append(valuesByID, msgID)...,
		)
	}
	if setByCard != "" {
		batch.Query(
			fmt.Sprintf("UPDATE message_by_card_time SET %s WHERE card_bucket = ? AND card_id = ? AND time_bucket = ? AND created_at = ? AND msg_id = ?", setByCard),
			append(valuesByCard, cardBucket, cardID, timeBucket, createdAt, msgID)...,
		)
	}
	if campaignID != (gocql.UUID{}) && setByCampaign != "" {
		batch.Query(
			fmt.Sprintf("UPDATE message_by_campaign_bucket_time SET %s WHERE campaign_id = ? AND campaign_bucket = ? AND time_bucket = ? AND created_at = ? AND card_id = ? AND msg_id = ?", setByCampaign),
			append(valuesByCampaign, campaignID, campaignBucket, timeBucket, createdAt, cardID, msgID)...,
		)
	}
	if setByTimeBucket != "" {
		batch.Query(
			fmt.Sprintf("UPDATE message_by_time_bucket SET %s WHERE time_bucket = ? AND global_bucket = ? AND created_at = ? AND msg_id = ?", setByTimeBucket),
			append(valuesByTimeBucket, timeBucket, globalBucket, createdAt, msgID)...,
		)
	}
	if setByDirection != "" {
		batch.Query(
			fmt.Sprintf("UPDATE message_by_direction_time_bucket SET %s WHERE direction = ? AND time_bucket = ? AND global_bucket = ? AND created_at = ? AND msg_id = ?", setByDirection),
			append(valuesByDirection, direction, timeBucket, globalBucket, createdAt, msgID)...,
		)
	}
	nextStatus, nextDlrStatus, nextPORStatusCode, nextErrorCode, nextSMPPMessageID := status, dlrStatus, porStatusCode, errorCode, smppMessageID
	if v, ok := update.Updates["status"].(string); ok {
		nextStatus = v
	}
	if raw, ok := update.Updates["dlr_status"]; ok {
		switch v := raw.(type) {
		case string:
			nextDlrStatus = &v
		case *string:
			nextDlrStatus = v
		case nil:
			nextDlrStatus = nil
		}
	}
	if raw, ok := update.Updates["por_status_code"]; ok {
		switch v := raw.(type) {
		case int16:
			nextPORStatusCode = &v
		case *int16:
			nextPORStatusCode = v
		case nil:
			nextPORStatusCode = nil
		}
	}
	if raw, ok := update.Updates["error_code"]; ok {
		switch v := raw.(type) {
		case string:
			nextErrorCode = &v
		case *string:
			nextErrorCode = v
		case nil:
			nextErrorCode = nil
		}
	}
	if raw, ok := update.Updates["smpp_message_id"]; ok {
		switch v := raw.(type) {
		case string:
			nextSMPPMessageID = &v
		case *string:
			nextSMPPMessageID = v
		case nil:
			nextSMPPMessageID = nil
		}
	}

	batch.Query(
		`DELETE FROM message_by_status_time_bucket WHERE status = ? AND time_bucket = ? AND global_bucket = ? AND created_at = ? AND msg_id = ?`,
		status, timeBucket, globalBucket, createdAt, msgID,
	)
	if nextStatus != "" {
		batch.Query(`INSERT INTO message_by_status_time_bucket (
			status, time_bucket, global_bucket, created_at, msg_id, card_id, campaign_id, updated_at,
			direction, smpp_message_id, dlr_status, por_status_code, error_code
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			nextStatus, timeBucket, globalBucket, createdAt, msgID, cardID, nullableUUID(campaignID), updatedAt,
			direction, nextSMPPMessageID, nextDlrStatus, nextPORStatusCode, nextErrorCode,
		)
	}
	if dlrStatus != nil && *dlrStatus != "" {
		batch.Query(
			`DELETE FROM message_by_dlr_status_time_bucket WHERE dlr_status = ? AND time_bucket = ? AND global_bucket = ? AND created_at = ? AND msg_id = ?`,
			*dlrStatus, timeBucket, globalBucket, createdAt, msgID,
		)
	}
	if nextDlrStatus != nil && *nextDlrStatus != "" {
		batch.Query(`INSERT INTO message_by_dlr_status_time_bucket (
			dlr_status, time_bucket, global_bucket, created_at, msg_id, card_id, campaign_id, updated_at,
			direction, status, smpp_message_id, por_status_code, error_code
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			*nextDlrStatus, timeBucket, globalBucket, createdAt, msgID, cardID, nullableUUID(campaignID), updatedAt,
			direction, nextStatus, nextSMPPMessageID, nextPORStatusCode, nextErrorCode,
		)
	}
	oldDelivered, oldUndelivered := dlrCounters(dlrStatus)
	newDelivered, newUndelivered := dlrCounters(nextDlrStatus)
	hourBucket := createdAt.UTC().Truncate(time.Hour)
	if oldDelivered != newDelivered || oldUndelivered != newUndelivered {
		addMetricCounterQueries(counterBatch, metricDelta{
			HourBucket:     hourBucket,
			CampaignID:     nullableUUIDPtr(campaignID),
			CampaignBucket: campaignBucket,
			Delivered:      newDelivered - oldDelivered,
			Undelivered:    newUndelivered - oldUndelivered,
		})
	}
	addMetricCounterQueries(counterBatch, metricDelta{
		HourBucket:       hourBucket,
		CampaignID:       nullableUUIDPtr(campaignID),
		CampaignBucket:   campaignBucket,
		ErrorCountDeltas: errorCounterDeltas(&status, nextStatus, dlrStatus, nextDlrStatus, porStatusCode, nextPORStatusCode),
	})
	if err := s.client.Session().ExecuteBatch(batch); err != nil {
		return fmt.Errorf("batch update message stores: %w", err)
	}
	if len(counterBatch.Entries) > 0 {
		if err := s.client.Session().ExecuteBatch(counterBatch); err != nil {
			return fmt.Errorf("batch update message counters: %w", err)
		}
	}

	return nil
}

func nullableUUID(id gocql.UUID) interface{} {
	if id == (gocql.UUID{}) {
		return nil
	}
	return id
}

func nullableUUIDPtr(id gocql.UUID) *gocql.UUID {
	if id == (gocql.UUID{}) {
		return nil
	}
	out := id
	return &out
}

func directionCounters(direction string) (mt int64, mo int64) {
	switch direction {
	case "MT":
		return 1, 0
	case "MO":
		return 0, 1
	default:
		return 0, 0
	}
}

func dlrCounters(status *string) (delivered int64, undelivered int64) {
	if status == nil {
		return 0, 0
	}
	if *status == "DELIVRD" {
		return 1, 0
	}
	if IsFailedDLR(*status) {
		return 0, 1
	}
	return 0, 0
}

type errorCounterDelta struct {
	Kind  string
	Key   string
	Delta int64
}

func errorCounterDeltas(oldStatus *string, newStatus string, oldDLRStatus, newDLRStatus *string, oldPORStatus, newPORStatus *int16) []errorCounterDelta {
	deltas := make(map[string]int64)
	adjustCounterDelta(deltas, "status", errorStatusKey(oldStatus), -1)
	adjustCounterDelta(deltas, "status", errorStatusKey(stringPtr(newStatus)), 1)
	adjustCounterDelta(deltas, "dlr", errorDLRKey(oldDLRStatus), -1)
	adjustCounterDelta(deltas, "dlr", errorDLRKey(newDLRStatus), 1)
	adjustCounterDelta(deltas, "por", errorPORKey(oldPORStatus), -1)
	adjustCounterDelta(deltas, "por", errorPORKey(newPORStatus), 1)

	out := make([]errorCounterDelta, 0, len(deltas))
	for composite, delta := range deltas {
		if delta == 0 {
			continue
		}
		parts := strings.SplitN(composite, "\x00", 2)
		if len(parts) != 2 {
			continue
		}
		out = append(out, errorCounterDelta{Kind: parts[0], Key: parts[1], Delta: delta})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind == out[j].Kind {
			return out[i].Key < out[j].Key
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

func addErrorCounterQueries(batch *gocql.Batch, campaignID *gocql.UUID, campaignBucket int, hourBucket time.Time, deltas []errorCounterDelta) {
	for _, delta := range deltas {
		batch.Query(
			`UPDATE message_error_counts_by_hour
			 SET error_count = error_count + ?
			 WHERE hour_bucket = ? AND error_kind = ? AND error_key = ?`,
			delta.Delta, hourBucket, delta.Kind, delta.Key,
		)
		if campaignID != nil {
			batch.Query(
				`UPDATE campaign_message_error_counts_by_hour
				 SET error_count = error_count + ?
				 WHERE campaign_id = ? AND campaign_bucket = ? AND hour_bucket = ? AND error_kind = ? AND error_key = ?`,
				delta.Delta, *campaignID, campaignBucket, hourBucket, delta.Kind, delta.Key,
			)
		}
	}
}

func addMetricCounterQueries(batch *gocql.Batch, delta metricDelta) {
	if delta.Total != 0 || delta.MT != 0 || delta.MO != 0 || delta.Delivered != 0 || delta.Undelivered != 0 {
		batch.Query(
			`UPDATE message_metrics_by_hour
			 SET total_messages = total_messages + ?,
			     mt_messages = mt_messages + ?,
			     mo_messages = mo_messages + ?,
			     delivered_messages = delivered_messages + ?,
			     undelivered_messages = undelivered_messages + ?
			 WHERE hour_bucket = ?`,
			delta.Total, delta.MT, delta.MO, delta.Delivered, delta.Undelivered, delta.HourBucket,
		)
		if delta.CampaignID != nil {
			batch.Query(
				`UPDATE campaign_message_metrics_by_hour
				 SET total_messages = total_messages + ?,
				     mt_messages = mt_messages + ?,
				     mo_messages = mo_messages + ?,
				     delivered_messages = delivered_messages + ?,
				     undelivered_messages = undelivered_messages + ?
				 WHERE campaign_id = ? AND campaign_bucket = ? AND hour_bucket = ?`,
				delta.Total, delta.MT, delta.MO, delta.Delivered, delta.Undelivered, *delta.CampaignID, delta.CampaignBucket, delta.HourBucket,
			)
		}
	}
	addErrorCounterQueries(batch, delta.CampaignID, delta.CampaignBucket, delta.HourBucket, delta.ErrorCountDeltas)
}

func adjustCounterDelta(deltas map[string]int64, kind, key string, delta int64) {
	if key == "" || delta == 0 {
		return
	}
	deltas[kind+"\x00"+key] += delta
}

func errorStatusKey(status *string) string {
	if status == nil {
		return ""
	}
	switch *status {
	case "send_failed", "failed":
		return *status
	default:
		return ""
	}
}

func errorDLRKey(status *string) string {
	if status == nil || !IsFailedDLR(*status) {
		return ""
	}
	return *status
}

func errorPORKey(code *int16) string {
	if code == nil || *code == 0 {
		return ""
	}
	return fmt.Sprintf("%d", *code)
}

func stringPtr(v string) *string {
	if v == "" {
		return nil
	}
	out := v
	return &out
}

func buildMessageUpdateSet(updates map[string]interface{}, updatedAt time.Time, allowed map[string]string) (string, []interface{}) {
	fields := make([]string, 0, len(updates))
	for field := range updates {
		if _, ok := allowed[field]; ok {
			fields = append(fields, field)
		}
	}
	if len(fields) == 0 {
		return "", nil
	}
	sort.Strings(fields)

	assignments := make([]string, 0, len(fields)+1)
	values := make([]interface{}, 0, len(fields)+1)
	for _, field := range fields {
		assignments = append(assignments, fmt.Sprintf("%s = ?", allowed[field]))
		values = append(values, normalizeMessageUpdateValue(field, updates[field]))
	}
	assignments = append(assignments, "updated_at = ?")
	values = append(values, updatedAt)
	return strings.Join(assignments, ", "), values
}

func normalizeMessageUpdateValue(field string, value interface{}) interface{} {
	switch field {
	case "por_status_code":
		switch v := value.(type) {
		case int16:
			return v
		case int:
			return int16(v)
		case int32:
			return int16(v)
		case int64:
			return int16(v)
		case float64:
			return int16(v)
		case json.Number:
			if i, err := v.Int64(); err == nil {
				return int16(i)
			}
		}
	case "por_data":
		switch v := value.(type) {
		case string:
			return []byte(v)
		}
	}
	return value
}
