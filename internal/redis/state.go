package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func (c *Client) encryptData(plaintext []byte) ([]byte, error) {
	if c.aead == nil {
		return plaintext, nil
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return c.aead.Seal(nonce, nonce, plaintext, nil), nil
}

func (c *Client) decryptData(ciphertext []byte) ([]byte, error) {
	if c.aead == nil {
		return ciphertext, nil
	}
	nonceSize := c.aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return c.aead.Open(nil, nonce, ciphertext, nil)
}

// ---------------------------------------------------------------------------
// Card State
// ---------------------------------------------------------------------------

// CardState represents the current processing state of a card within a campaign.
type CardState struct {
	CampaignID  string `json:"campaign_id"`
	CurrentStep int    `json:"current_step"`
	Status      string `json:"status"` // pending, sending, awaiting_dlr, awaiting_mo, completed, failed
	RetryCount  int    `json:"retry_count"`
	LastMsgID   string `json:"last_msg_id"`
	LastEventID string `json:"last_event_id"`
}

func cardStateKey(cardID string) string {
	return "card:" + cardID + ":state"
}

// GetCardState retrieves the card state hash from Redis.
// Returns (nil, nil) if the key does not exist.
func (c *Client) GetCardState(ctx context.Context, cardID string) (*CardState, error) {
	key := cardStateKey(cardID)
	result, err := c.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		c.logger.Warn("redis: failed to get card state", zap.String("card_id", cardID), zap.Error(err))
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}

	currentStep, _ := strconv.Atoi(result["current_step"])
	retryCount, _ := strconv.Atoi(result["retry_count"])

	return &CardState{
		CampaignID:  result["campaign_id"],
		CurrentStep: currentStep,
		Status:      result["status"],
		RetryCount:  retryCount,
		LastMsgID:   result["last_msg_id"],
		LastEventID: result["last_event_id"],
	}, nil
}

// SetCardState stores the card state as a Redis hash with a 24-hour TTL.
func (c *Client) SetCardState(ctx context.Context, cardID string, state *CardState) error {
	key := cardStateKey(cardID)
	fields := map[string]interface{}{
		"campaign_id":   state.CampaignID,
		"current_step":  state.CurrentStep,
		"status":        state.Status,
		"retry_count":   state.RetryCount,
		"last_msg_id":   state.LastMsgID,
		"last_event_id": state.LastEventID,
	}

	pipe := c.rdb.Pipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, 24*time.Hour)
	_, err := pipe.Exec(ctx)
	if err != nil {
		c.logger.Warn("redis: failed to set card state", zap.String("card_id", cardID), zap.Error(err))
		return err
	}
	return nil
}

// DeleteCardState removes the card state key from Redis.
func (c *Client) DeleteCardState(ctx context.Context, cardID string) error {
	key := cardStateKey(cardID)
	err := c.rdb.Del(ctx, key).Err()
	if err != nil {
		c.logger.Warn("redis: failed to delete card state", zap.String("card_id", cardID), zap.Error(err))
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Card Keys Cache
// ---------------------------------------------------------------------------

// CardKeys holds the cryptographic keys and metadata for a SIM card.
type CardKeys struct {
	EncKey    []byte `json:"enc_key"`
	AuthKey   []byte `json:"auth_key"`
	KEK       []byte `json:"kek,omitempty"`
	ProfileID string `json:"profile_id"`
	MSISDN    string `json:"msisdn"`
}

// cardKeysJSON is the serialization form that stores byte fields as hex strings.
type cardKeysJSON struct {
	EncKey    string `json:"enc_key"`
	AuthKey   string `json:"auth_key"`
	KEK       string `json:"kek,omitempty"`
	ProfileID string `json:"profile_id"`
	MSISDN    string `json:"msisdn"`
}

func cardKeysKey(cardID string) string {
	return "card:" + cardID + ":keys"
}

// CacheCardKeys stores the card keys as a JSON string with a 1-hour TTL.
// Byte fields are hex-encoded before storage.
func (c *Client) CacheCardKeys(ctx context.Context, cardID string, keys *CardKeys) error {
	j := cardKeysJSON{
		EncKey:    hex.EncodeToString(keys.EncKey),
		AuthKey:   hex.EncodeToString(keys.AuthKey),
		KEK:       hex.EncodeToString(keys.KEK),
		ProfileID: keys.ProfileID,
		MSISDN:    keys.MSISDN,
	}
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal card keys: %w", err)
	}

	if c.aead != nil {
		data, err = c.encryptData(data)
		if err != nil {
			return fmt.Errorf("encrypt card keys: %w", err)
		}
	}

	key := cardKeysKey(cardID)
	if err := c.rdb.Set(ctx, key, data, 1*time.Hour).Err(); err != nil {
		c.logger.Warn("redis: failed to cache card keys", zap.String("card_id", cardID), zap.Error(err))
		return err
	}

	// Also store the MSISDN→card_id mapping for MO correlation.
	if keys.MSISDN != "" {
		if err := c.StoreMSISDNMapping(ctx, keys.MSISDN, cardID); err != nil {
			c.logger.Warn("redis: failed to store MSISDN mapping", zap.String("card_id", cardID), zap.String("msisdn", keys.MSISDN), zap.Error(err))
		}
	}

	return nil
}

// GetCardKeys retrieves cached card keys. Returns (nil, nil) on cache miss.
func (c *Client) GetCardKeys(ctx context.Context, cardID string) (*CardKeys, error) {
	key := cardKeysKey(cardID)
	data, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		c.logger.Warn("redis: failed to get card keys", zap.String("card_id", cardID), zap.Error(err))
		return nil, err
	}

	if c.aead != nil {
		data, err = c.decryptData(data)
		if err != nil {
			return nil, fmt.Errorf("decrypt card keys: %w", err)
		}
	}

	var j cardKeysJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("unmarshal card keys: %w", err)
	}

	encKey, err := hex.DecodeString(j.EncKey)
	if err != nil {
		return nil, fmt.Errorf("decode enc_key hex: %w", err)
	}
	authKey, err := hex.DecodeString(j.AuthKey)
	if err != nil {
		return nil, fmt.Errorf("decode auth_key hex: %w", err)
	}
	var kek []byte
	if j.KEK != "" {
		kek, err = hex.DecodeString(j.KEK)
		if err != nil {
			return nil, fmt.Errorf("decode kek hex: %w", err)
		}
	}

	return &CardKeys{
		EncKey:    encKey,
		AuthKey:   authKey,
		KEK:       kek,
		ProfileID: j.ProfileID,
		MSISDN:    j.MSISDN,
	}, nil
}

// ---------------------------------------------------------------------------
// Campaign Commands Cache
// ---------------------------------------------------------------------------

// CampaignCommandCache holds a single command within a campaign, including
// the security profile fields from the associated Application.
type CampaignCommandCache struct {
	Sequence       int    `json:"sequence"`
	ApplicationID  string `json:"application_id"`
	Script         []byte `json:"script"`
	ExpectResponse bool   `json:"expect_response"`
	TAR            []byte `json:"tar"`
	KIcAlgo        string `json:"kic_algo"`
	KIcMode        string `json:"kic_mode"`
	KIcKeysetID    int    `json:"kic_keyset_id"`
	KIdAlgo        string `json:"kid_algo"`
	KIdMode        string `json:"kid_mode"`
	KIdKeysetID    int    `json:"kid_keyset_id"`
	CertMode       string `json:"cert_mode"`
	Ciphered       bool   `json:"ciphered"`
	CounterMode    string `json:"counter_mode"`
	PORMode        string `json:"por_mode"`
	PORProtocol    string `json:"por_protocol"`
	PORCiphered    bool   `json:"por_ciphered"`
	PORCertMode    string `json:"por_cert_mode"`
}

// campaignCommandJSON mirrors CampaignCommandCache but with byte fields as hex.
type campaignCommandJSON struct {
	Sequence       int    `json:"sequence"`
	ApplicationID  string `json:"application_id"`
	Script         string `json:"script"`
	ExpectResponse bool   `json:"expect_response"`
	TAR            string `json:"tar"`
	KIcAlgo        string `json:"kic_algo"`
	KIcMode        string `json:"kic_mode"`
	KIcKeysetID    int    `json:"kic_keyset_id"`
	KIdAlgo        string `json:"kid_algo"`
	KIdMode        string `json:"kid_mode"`
	KIdKeysetID    int    `json:"kid_keyset_id"`
	CertMode       string `json:"cert_mode"`
	Ciphered       bool   `json:"ciphered"`
	CounterMode    string `json:"counter_mode"`
	PORMode        string `json:"por_mode"`
	PORProtocol    string `json:"por_protocol"`
	PORCiphered    bool   `json:"por_ciphered"`
	PORCertMode    string `json:"por_cert_mode"`
}

func campaignCommandsKey(campaignID string) string {
	return "campaign:" + campaignID + ":commands"
}

// CacheCampaignCommands stores the campaign commands as a JSON array with a 12-hour TTL.
func (c *Client) CacheCampaignCommands(ctx context.Context, campaignID string, cmds []CampaignCommandCache) error {
	jsonCmds := make([]campaignCommandJSON, len(cmds))
	for i, cmd := range cmds {
		jsonCmds[i] = campaignCommandJSON{
			Sequence:       cmd.Sequence,
			ApplicationID:  cmd.ApplicationID,
			Script:         hex.EncodeToString(cmd.Script),
			ExpectResponse: cmd.ExpectResponse,
			TAR:            hex.EncodeToString(cmd.TAR),
			KIcAlgo:        cmd.KIcAlgo,
			KIcMode:        cmd.KIcMode,
			KIcKeysetID:    cmd.KIcKeysetID,
			KIdAlgo:        cmd.KIdAlgo,
			KIdMode:        cmd.KIdMode,
			KIdKeysetID:    cmd.KIdKeysetID,
			CertMode:       cmd.CertMode,
			Ciphered:       cmd.Ciphered,
			CounterMode:    cmd.CounterMode,
			PORMode:        cmd.PORMode,
			PORProtocol:    cmd.PORProtocol,
			PORCiphered:    cmd.PORCiphered,
			PORCertMode:    cmd.PORCertMode,
		}
	}

	data, err := json.Marshal(jsonCmds)
	if err != nil {
		return fmt.Errorf("marshal campaign commands: %w", err)
	}

	key := campaignCommandsKey(campaignID)
	if err := c.rdb.Set(ctx, key, data, 12*time.Hour).Err(); err != nil {
		c.logger.Warn("redis: failed to cache campaign commands", zap.String("campaign_id", campaignID), zap.Error(err))
		return err
	}
	return nil
}

// GetCampaignCommands retrieves cached campaign commands. Returns (nil, nil) on cache miss.
func (c *Client) GetCampaignCommands(ctx context.Context, campaignID string) ([]CampaignCommandCache, error) {
	key := campaignCommandsKey(campaignID)
	data, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		c.logger.Warn("redis: failed to get campaign commands", zap.String("campaign_id", campaignID), zap.Error(err))
		return nil, err
	}

	var jsonCmds []campaignCommandJSON
	if err := json.Unmarshal(data, &jsonCmds); err != nil {
		return nil, fmt.Errorf("unmarshal campaign commands: %w", err)
	}

	cmds := make([]CampaignCommandCache, len(jsonCmds))
	for i, jc := range jsonCmds {
		script, err := hex.DecodeString(jc.Script)
		if err != nil {
			return nil, fmt.Errorf("decode script hex at index %d: %w", i, err)
		}
		tar, err := hex.DecodeString(jc.TAR)
		if err != nil {
			return nil, fmt.Errorf("decode tar hex at index %d: %w", i, err)
		}
		cmds[i] = CampaignCommandCache{
			Sequence:       jc.Sequence,
			ApplicationID:  jc.ApplicationID,
			Script:         script,
			ExpectResponse: jc.ExpectResponse,
			TAR:            tar,
			KIcAlgo:        jc.KIcAlgo,
			KIcMode:        jc.KIcMode,
			KIcKeysetID:    jc.KIcKeysetID,
			KIdAlgo:        jc.KIdAlgo,
			KIdMode:        jc.KIdMode,
			KIdKeysetID:    jc.KIdKeysetID,
			CertMode:       jc.CertMode,
			Ciphered:       jc.Ciphered,
			CounterMode:    jc.CounterMode,
			PORMode:        jc.PORMode,
			PORProtocol:    jc.PORProtocol,
			PORCiphered:    jc.PORCiphered,
			PORCertMode:    jc.PORCertMode,
		}
	}
	return cmds, nil
}

// ---------------------------------------------------------------------------
// Campaign Status
// ---------------------------------------------------------------------------

func campaignStatusKey(campaignID string) string {
	return "campaign:" + campaignID + ":status"
}

// SetCampaignStatus stores the campaign status string with a 24-hour TTL.
func (c *Client) SetCampaignStatus(ctx context.Context, campaignID, status string) error {
	key := campaignStatusKey(campaignID)
	if err := c.rdb.Set(ctx, key, status, 24*time.Hour).Err(); err != nil {
		c.logger.Warn("redis: failed to set campaign status", zap.String("campaign_id", campaignID), zap.Error(err))
		return err
	}
	return nil
}

// GetCampaignStatus retrieves the campaign status. Returns ("", nil) on miss.
func (c *Client) GetCampaignStatus(ctx context.Context, campaignID string) (string, error) {
	key := campaignStatusKey(campaignID)
	status, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		c.logger.Warn("redis: failed to get campaign status", zap.String("campaign_id", campaignID), zap.Error(err))
		return "", err
	}
	return status, nil
}

// ---------------------------------------------------------------------------
// Counter (Atomic)
// ---------------------------------------------------------------------------

// IncrCounter atomically increments the counter for a given card and application.
// Counters are permanent and monotonic (no TTL).
func (c *Client) IncrCounter(ctx context.Context, cardID, appID string) (int64, error) {
	key := "counter:" + cardID + ":" + appID
	val, err := c.rdb.Incr(ctx, key).Result()
	if err != nil {
		c.logger.Warn("redis: failed to increment counter",
			zap.String("card_id", cardID),
			zap.String("app_id", appID),
			zap.Error(err),
		)
		return 0, err
	}
	return val, nil
}

// ---------------------------------------------------------------------------
// MSISDN → Card Mapping
// ---------------------------------------------------------------------------

// StoreMSISDNMapping stores a persistent MSISDN→card_id mapping with a 24-hour TTL.
// This is set when card keys are cached, since the MSISDN is known at that point.
func (c *Client) StoreMSISDNMapping(ctx context.Context, msisdn, cardID string) error {
	key := "msisdn:" + msisdn
	return c.rdb.Set(ctx, key, cardID, 24*time.Hour).Err()
}

// LookupCardByMSISDN resolves a card_id from an MSISDN using the persistent mapping.
func (c *Client) LookupCardByMSISDN(ctx context.Context, msisdn string) (string, error) {
	key := "msisdn:" + msisdn
	return c.rdb.Get(ctx, key).Result()
}

// ---------------------------------------------------------------------------
// Multipart SMS DLR Tracking
// ---------------------------------------------------------------------------

// InitMultipartTracking initializes tracking for a multipart SMS message.
func (c *Client) InitMultipartTracking(ctx context.Context, msgID string, totalParts int) error {
	key := "multipart:" + msgID
	pipe := c.rdb.Pipeline()
	pipe.HSet(ctx, key, "total", totalParts, "delivered", 0, "failed", 0)
	pipe.Expire(ctx, key, 10*time.Minute)
	_, err := pipe.Exec(ctx)
	return err
}

// RecordPartDLR records a DLR for one part of a multipart SMS and returns
// whether all parts have been resolved and whether any part failed.
func (c *Client) RecordPartDLR(ctx context.Context, msgID string, delivered bool) (allResolved bool, anyFailed bool, err error) {
	key := "multipart:" + msgID

	// Check if multipart tracking exists first.
	exists, err := c.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, false, err
	}
	if exists == 0 {
		return false, false, fmt.Errorf("no multipart tracking for msg %s", msgID)
	}

	field := "delivered"
	if !delivered {
		field = "failed"
	}
	if _, err := c.rdb.HIncrBy(ctx, key, field, 1).Result(); err != nil {
		return false, false, err
	}
	vals, err := c.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return false, false, err
	}
	total, _ := strconv.ParseInt(vals["total"], 10, 64)
	if total <= 0 {
		return false, false, fmt.Errorf("multipart tracking for msg %s has invalid total: %d", msgID, total)
	}
	del, _ := strconv.ParseInt(vals["delivered"], 10, 64)
	fail, _ := strconv.ParseInt(vals["failed"], 10, 64)
	allResolved = (del + fail) >= total
	anyFailed = fail > 0
	return allResolved, anyFailed, nil
}

// ---------------------------------------------------------------------------
// SMPP Correlation
// ---------------------------------------------------------------------------

// SMPPMapping stores the correlation between an SMPP message ID and the
// campaign/card it belongs to.
type SMPPMapping struct {
	MsgID      string `json:"msg_id"`
	CampaignID string `json:"campaign_id"`
	CardID     string `json:"card_id"`
	MSISDN     string `json:"msisdn"`
}

func smppKey(smppMsgID string) string {
	return "smpp:" + smppMsgID
}

// StoreSMPPCorrelation stores an SMPP correlation mapping with a 5-minute TTL.
func (c *Client) StoreSMPPCorrelation(ctx context.Context, smppMsgID string, mapping *SMPPMapping) error {
	data, err := json.Marshal(mapping)
	if err != nil {
		return fmt.Errorf("marshal smpp mapping: %w", err)
	}

	key := smppKey(smppMsgID)
	if err := c.rdb.Set(ctx, key, data, 5*time.Minute).Err(); err != nil {
		c.logger.Warn("redis: failed to store smpp correlation", zap.String("smpp_msg_id", smppMsgID), zap.Error(err))
		return err
	}
	return nil
}

// LoadSMPPCorrelation retrieves and deletes the SMPP correlation mapping.
// Returns (nil, nil) on cache miss.
func (c *Client) LoadSMPPCorrelation(ctx context.Context, smppMsgID string) (*SMPPMapping, error) {
	key := smppKey(smppMsgID)

	pipe := c.rdb.Pipeline()
	getCmd := pipe.Get(ctx, key)
	pipe.Del(ctx, key)
	_, err := pipe.Exec(ctx)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		c.logger.Warn("redis: failed to load smpp correlation", zap.String("smpp_msg_id", smppMsgID), zap.Error(err))
		return nil, err
	}

	data, err := getCmd.Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	var mapping SMPPMapping
	if err := json.Unmarshal(data, &mapping); err != nil {
		return nil, fmt.Errorf("unmarshal smpp mapping: %w", err)
	}
	return &mapping, nil
}

// ---------------------------------------------------------------------------
// Dedupe
// ---------------------------------------------------------------------------

// CheckAndSetDedupe attempts to set a deduplication key with a 10-minute TTL.
// Returns true if the key was newly set (event is NOT a duplicate).
// Returns false if the key already existed (event IS a duplicate).
func (c *Client) CheckAndSetDedupe(ctx context.Context, eventID string) (bool, error) {
	key := "dedupe:" + eventID
	ok, err := c.rdb.SetNX(ctx, key, 1, 10*time.Minute).Result()
	if err != nil {
		c.logger.Warn("redis: failed to check dedupe", zap.String("event_id", eventID), zap.Error(err))
		return false, err
	}
	return ok, nil
}

// ---------------------------------------------------------------------------
// Throttle
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Profile Cache
// ---------------------------------------------------------------------------

func profileCacheKey(profileID string) string {
	return "profile:" + profileID
}

// CacheProfile stores a profile as JSON with a 1-hour TTL.
func (c *Client) CacheProfile(ctx context.Context, profileID string, profile interface{}) error {
	data, err := json.Marshal(profile)
	if err != nil {
		return fmt.Errorf("marshal profile: %w", err)
	}
	key := profileCacheKey(profileID)
	return c.rdb.Set(ctx, key, data, 1*time.Hour).Err()
}

// GetCachedProfile retrieves a cached profile. Returns redis.Nil on miss.
func (c *Client) GetCachedProfile(ctx context.Context, profileID string, dest interface{}) error {
	key := profileCacheKey(profileID)
	data, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return redis.Nil
		}
		return err
	}
	return json.Unmarshal(data, dest)
}

// ---------------------------------------------------------------------------
// Campaign Params Cache
// ---------------------------------------------------------------------------

// CampaignParams holds the campaign-level parameters needed during card processing.
type CampaignParams struct {
	MaxConcatOverride *int `json:"max_concat_override,omitempty"`
	ThrottleSMSPerSec *int `json:"throttle_sms_per_sec,omitempty"`
	MaxRetries        int  `json:"max_retries"`
}

func campaignParamsKey(campaignID string) string {
	return "campaign_params:" + campaignID
}

// CacheCampaignParams stores campaign parameters with a 1-hour TTL.
func (c *Client) CacheCampaignParams(ctx context.Context, campaignID string, params *CampaignParams) error {
	data, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("marshal campaign params: %w", err)
	}
	key := campaignParamsKey(campaignID)
	return c.rdb.Set(ctx, key, data, 1*time.Hour).Err()
}

// GetCachedCampaignParams retrieves cached campaign parameters. Returns (nil, nil) on miss.
func (c *Client) GetCachedCampaignParams(ctx context.Context, campaignID string) (*CampaignParams, error) {
	key := campaignParamsKey(campaignID)
	data, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	var params CampaignParams
	if err := json.Unmarshal(data, &params); err != nil {
		return nil, fmt.Errorf("unmarshal campaign params: %w", err)
	}
	return &params, nil
}

// AcquireThrottle checks whether a campaign is under its per-second rate limit
// using a sliding window (INCR + EXPIRE) approach.
// Returns true if the request is allowed, false if the rate is exceeded.
func (c *Client) AcquireThrottle(ctx context.Context, campaignID string, ratePerSec int) (bool, error) {
	now := time.Now().Unix()
	key := "throttle:" + campaignID + ":" + strconv.FormatInt(now, 10)

	pipe := c.rdb.Pipeline()
	incrCmd := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 2*time.Second) // expire shortly after the second passes
	_, err := pipe.Exec(ctx)
	if err != nil {
		c.logger.Warn("redis: failed to acquire throttle", zap.String("campaign_id", campaignID), zap.Error(err))
		return false, err
	}

	count := incrCmd.Val()
	return count <= int64(ratePerSec), nil
}
