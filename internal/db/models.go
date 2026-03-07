package db

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Profile represents an OTA card profile with transport-level settings.
type Profile struct {
	ID                uuid.UUID     `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	Name              string        `gorm:"uniqueIndex;not null" json:"name"`
	MaxConcatSMS      int           `gorm:"not null;default:7" json:"max_concat_sms"`
	BufferSize        int           `gorm:"not null;default:180" json:"buffer_size"`
	PID               int16         `gorm:"column:pid;not null;default:0" json:"pid"`
	DCS               int16         `gorm:"not null;default:0" json:"dcs"`
	SecurityBytesType string        `gorm:"not null;default:'WITH_LENGTHS_AND_UDHL'" json:"security_bytes_type"`
	CreatedAt         time.Time     `json:"created_at"`
	Applications      []Application `gorm:"foreignKey:ProfileID" json:"applications,omitempty"`
	Cards             []Card        `gorm:"foreignKey:ProfileID" json:"cards,omitempty"`
}

// Application represents a TAR-addressed application within a profile, with its security configuration.
type Application struct {
	ID                uuid.UUID `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	ProfileID         uuid.UUID `gorm:"type:uuid;not null;index" json:"profile_id"`
	Name              string    `gorm:"not null" json:"name"`
	TAR               []byte    `gorm:"type:bytea;not null" json:"tar"`
	KIcAlgo           string    `gorm:"column:kic_algo;not null;default:'DES'" json:"kic_algo"`
	KIcMode           string    `gorm:"column:kic_mode;not null;default:'TRIPLE_DES_CBC_2_KEYS'" json:"kic_mode"`
	KIcKeysetID       int16     `gorm:"column:kic_keyset_id;not null;default:1" json:"kic_keyset_id"`
	KIdAlgo           string    `gorm:"column:kid_algo;not null;default:'DES'" json:"kid_algo"`
	KIdMode           string    `gorm:"column:kid_mode;not null;default:'TRIPLE_DES_CBC_2_KEYS'" json:"kid_mode"`
	KIdKeysetID       int16     `gorm:"column:kid_keyset_id;not null;default:1" json:"kid_keyset_id"`
	CertificationMode string    `gorm:"not null;default:'CC'" json:"certification_mode"`
	Ciphered          bool      `gorm:"not null;default:true" json:"ciphered"`
	CounterMode       string    `gorm:"not null;default:'COUNTER_REPLAY_OR_CHECK'" json:"counter_mode"`
	PORMode           string    `gorm:"column:por_mode;not null;default:'REPLY_ALWAYS'" json:"por_mode"`
	PORProtocol       string    `gorm:"column:por_protocol;not null;default:'SMS_SUBMIT'" json:"por_protocol"`
	PORCiphered       bool      `gorm:"column:por_ciphered;not null;default:false" json:"por_ciphered"`
	PORCertMode       string    `gorm:"column:por_cert_mode;not null;default:'NO_SECURITY'" json:"por_cert_mode"`
	Profile           Profile   `gorm:"foreignKey:ProfileID" json:"profile,omitempty"`
}

// Card represents a SIM card with its identity, keys, and profile association.
type Card struct {
	ID        uuid.UUID `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	ICCID     string    `gorm:"column:iccid;uniqueIndex;not null" json:"iccid"`
	IMSI      string    `gorm:"uniqueIndex;not null" json:"imsi"`
	MSISDN    string    `gorm:"uniqueIndex;not null" json:"msisdn"`
	ProfileID uuid.UUID `gorm:"type:uuid;not null;index" json:"profile_id"`
	EncKey    []byte    `gorm:"type:bytea;not null" json:"-"`
	AuthKey   []byte    `gorm:"type:bytea;not null" json:"-"`
	KEK       []byte    `gorm:"type:bytea" json:"-"`
	Status    string    `gorm:"not null;default:'active'" json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Profile   Profile   `gorm:"foreignKey:ProfileID" json:"profile,omitempty"`
}

// CardCounter tracks the OTA counter for a specific card-application pair.
type CardCounter struct {
	CardID        uuid.UUID   `gorm:"type:uuid;primaryKey" json:"card_id"`
	ApplicationID uuid.UUID   `gorm:"type:uuid;primaryKey" json:"application_id"`
	CounterValue  int64       `gorm:"not null;default:0" json:"counter_value"`
	Card          Card        `gorm:"foreignKey:CardID" json:"card,omitempty"`
	Application   Application `gorm:"foreignKey:ApplicationID" json:"application,omitempty"`
}

// Campaign represents a batch operation targeting a set of cards.
type Campaign struct {
	ID                uuid.UUID         `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	Name              string            `gorm:"not null" json:"name"`
	Status            string            `gorm:"not null;default:'pending'" json:"status"`
	CampaignType      string            `gorm:"column:campaign_type;not null;default:'script'" json:"campaign_type"`
	CAPFileID         *uuid.UUID        `gorm:"type:uuid;column:cap_file_id" json:"cap_file_id,omitempty"`
	ScriptID          *uuid.UUID        `gorm:"type:uuid;column:script_id" json:"script_id,omitempty"`
	ScheduledAt       *time.Time        `json:"scheduled_at,omitempty"`
	MaxRetries        int               `gorm:"not null;default:3" json:"max_retries"`
	ThrottleSMSPerSec *int              `gorm:"column:throttle_sms_per_sec" json:"throttle_sms_per_sec,omitempty"`
	MaxConcatOverride *int              `json:"max_concat_override,omitempty"`
	StartedAt         *time.Time        `json:"started_at,omitempty"`
	CompletedAt       *time.Time        `json:"completed_at,omitempty"`
	CreatedBy         *string           `json:"created_by,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
	CAPFile           *CAPFile          `gorm:"foreignKey:CAPFileID" json:"cap_file,omitempty"`
	Script            *Script           `gorm:"foreignKey:ScriptID" json:"script,omitempty"`
	CampaignCommands  []CampaignCommand `gorm:"foreignKey:CampaignID" json:"campaign_commands,omitempty"`
	CampaignTargets   []CampaignTarget  `gorm:"foreignKey:CampaignID" json:"campaign_targets,omitempty"`
}

// CampaignCommand represents a single command step within a campaign.
type CampaignCommand struct {
	ID             uuid.UUID   `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	CampaignID     uuid.UUID   `gorm:"type:uuid;not null;index" json:"campaign_id"`
	Sequence       int         `gorm:"not null" json:"sequence"`
	ApplicationID  uuid.UUID   `gorm:"type:uuid;not null" json:"application_id"`
	Script         []byte      `gorm:"type:bytea;not null" json:"script"`
	ExpectResponse bool        `gorm:"not null;default:true" json:"expect_response"`
	Campaign       Campaign    `gorm:"foreignKey:CampaignID" json:"campaign,omitempty"`
	Application    Application `gorm:"foreignKey:ApplicationID" json:"application,omitempty"`
}

// CampaignTarget records static campaign targeting membership.
type CampaignTarget struct {
	CampaignID uuid.UUID `gorm:"type:uuid;primaryKey" json:"campaign_id"`
	CardID     uuid.UUID `gorm:"type:uuid;primaryKey" json:"card_id"`
	CreatedAt  time.Time `json:"created_at"`
	Campaign   Campaign  `gorm:"foreignKey:CampaignID" json:"campaign,omitempty"`
	Card       Card      `gorm:"foreignKey:CardID" json:"card,omitempty"`
}

// CampaignShard is a planner-owned shard manifest for card activation events.
type CampaignShard struct {
	ID          uuid.UUID       `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	CampaignID  uuid.UUID       `gorm:"type:uuid;not null;index" json:"campaign_id"`
	Sequence    int             `gorm:"not null" json:"sequence"`
	Status      string          `gorm:"not null;default:'pending'" json:"status"`
	ItemCount   int             `gorm:"not null;default:0" json:"item_count"`
	Items       json.RawMessage `gorm:"type:jsonb;not null;default:'[]'" json:"items"`
	ClaimedAt   *time.Time      `gorm:"column:claimed_at" json:"claimed_at,omitempty"`
	PublishedAt *time.Time      `gorm:"column:published_at" json:"published_at,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	Campaign    Campaign        `gorm:"foreignKey:CampaignID" json:"campaign,omitempty"`
}

// MessageLog is the normalized message record shape used by the projector and Scylla stores.
type MessageLog struct {
	ID             uuid.UUID  `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	CampaignID     *uuid.UUID `gorm:"type:uuid;index" json:"campaign_id,omitempty"`
	CardID         uuid.UUID  `gorm:"type:uuid;not null" json:"card_id"`
	Direction      string     `gorm:"not null" json:"direction"`
	RawPayload     []byte     `gorm:"type:bytea;not null" json:"raw_payload"`
	SecuredPayload []byte     `gorm:"type:bytea" json:"secured_payload,omitempty"`
	Status         string     `gorm:"not null;default:'created'" json:"status"`
	SMPPMessageID  *string    `gorm:"column:smpp_message_id" json:"smpp_message_id,omitempty"`
	CounterHex     *string    `gorm:"column:counter_hex" json:"counter_hex,omitempty"`
	DLRStatus      *string    `gorm:"column:dlr_status" json:"dlr_status,omitempty"`
	PORStatusCode  *int16     `gorm:"column:por_status_code" json:"por_status_code,omitempty"`
	PORData        []byte     `gorm:"type:bytea;column:por_data" json:"por_data,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	Campaign       *Campaign  `gorm:"foreignKey:CampaignID" json:"campaign,omitempty"`
	Card           Card       `gorm:"foreignKey:CardID" json:"card,omitempty"`
}

// CAPFile stores an uploaded CAP file with its parsed metadata.
type CAPFile struct {
	ID          uuid.UUID       `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	Filename    string          `gorm:"not null" json:"filename"`
	AID         string          `gorm:"not null" json:"aid"`
	FileData    []byte          `gorm:"type:bytea;not null" json:"-"`
	FileSize    int             `gorm:"not null" json:"file_size"`
	SHA256Hash  string          `gorm:"column:sha256_hash;not null" json:"sha256_hash"`
	LoadFileHex string          `gorm:"not null" json:"load_file_hex"`
	Sections    json.RawMessage `gorm:"type:jsonb;not null;default:'[]'" json:"sections"`
	UploadedBy  *string         `json:"uploaded_by,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

// Script represents a reusable OTA command script.
type Script struct {
	ID           uuid.UUID `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	Name         string    `gorm:"not null" json:"name"`
	Description  *string   `json:"description,omitempty"`
	TargetTAR    string    `gorm:"column:target_tar;not null" json:"target_tar"`
	Commands     string    `gorm:"not null" json:"commands"`
	CommandCount int       `gorm:"not null;default:0" json:"command_count"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// CardGroup represents a named collection of cards for targeting campaigns.
type CardGroup struct {
	ID          uuid.UUID         `gorm:"type:uuid;primaryKey;default:gen_random_uuid()" json:"id"`
	Name        string            `gorm:"uniqueIndex;not null" json:"name"`
	Description *string           `json:"description,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	Members     []CardGroupMember `gorm:"foreignKey:CardGroupID" json:"members,omitempty"`
}

// CardGroupMember associates a card with a card group.
type CardGroupMember struct {
	CardGroupID uuid.UUID `gorm:"type:uuid;primaryKey" json:"card_group_id"`
	CardID      uuid.UUID `gorm:"type:uuid;primaryKey" json:"card_id"`
	CardGroup   CardGroup `gorm:"foreignKey:CardGroupID" json:"card_group,omitempty"`
	Card        Card      `gorm:"foreignKey:CardID" json:"card,omitempty"`
}

// TableName overrides for GORM table name resolution.

func (Profile) TableName() string         { return "profiles" }
func (Application) TableName() string     { return "applications" }
func (Card) TableName() string            { return "cards" }
func (CardCounter) TableName() string     { return "card_counters" }
func (Campaign) TableName() string        { return "campaigns" }
func (CampaignCommand) TableName() string { return "campaign_commands" }
func (CampaignTarget) TableName() string  { return "campaign_targets" }
func (CampaignShard) TableName() string   { return "campaign_shards" }
func (CAPFile) TableName() string         { return "cap_files" }
func (Script) TableName() string          { return "scripts" }
func (CardGroup) TableName() string       { return "card_groups" }
func (CardGroupMember) TableName() string { return "card_group_members" }
