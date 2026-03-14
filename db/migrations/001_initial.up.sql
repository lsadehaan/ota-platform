CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

CREATE TABLE profiles (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL UNIQUE,
    max_concat_sms  INT NOT NULL DEFAULT 7,
    buffer_size     INT NOT NULL DEFAULT 180,
    pid             SMALLINT NOT NULL DEFAULT 0,
    dcs             SMALLINT NOT NULL DEFAULT 0,
    security_bytes_type TEXT NOT NULL DEFAULT 'WITH_LENGTHS_AND_UDHL',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE applications (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    profile_id      UUID NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    tar             BYTEA NOT NULL,
    kic_algo        TEXT NOT NULL DEFAULT 'DES',
    kic_mode        TEXT NOT NULL DEFAULT 'TRIPLE_DES_CBC_2_KEYS',
    kic_keyset_id   SMALLINT NOT NULL DEFAULT 1,
    kid_algo        TEXT NOT NULL DEFAULT 'DES',
    kid_mode        TEXT NOT NULL DEFAULT 'TRIPLE_DES_CBC_2_KEYS',
    kid_keyset_id   SMALLINT NOT NULL DEFAULT 1,
    certification_mode TEXT NOT NULL DEFAULT 'CC',
    ciphered        BOOLEAN NOT NULL DEFAULT true,
    counter_mode    TEXT NOT NULL DEFAULT 'COUNTER_REPLAY_OR_CHECK',
    por_mode        TEXT NOT NULL DEFAULT 'REPLY_ALWAYS',
    por_protocol    TEXT NOT NULL DEFAULT 'SMS_SUBMIT',
    por_ciphered    BOOLEAN NOT NULL DEFAULT false,
    por_cert_mode   TEXT NOT NULL DEFAULT 'NO_SECURITY',
    UNIQUE(profile_id, tar)
);

CREATE TABLE cards (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    iccid           TEXT NOT NULL UNIQUE,
    imsi            TEXT NOT NULL UNIQUE,
    msisdn          TEXT NOT NULL UNIQUE,
    profile_id      UUID NOT NULL REFERENCES profiles(id),
    enc_key         BYTEA NOT NULL,
    auth_key        BYTEA NOT NULL,
    kek             BYTEA,
    status          TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','inactive','blocked')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE card_counters (
    card_id         UUID NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
    application_id  UUID NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    counter_value   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (card_id, application_id)
);

CREATE TABLE campaigns (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','running','paused','completed','completed_with_errors','failed','aborted')),
    campaign_type   TEXT NOT NULL DEFAULT 'script'
                    CHECK (campaign_type IN ('cap_load','applet_install','applet_delete','script',
                                             'install_applet','delete_applet','update_applet','send_script','custom_apdu')),
    cap_file_id     UUID,
    script_id       UUID,
    scheduled_at    TIMESTAMPTZ,
    max_retries     INT NOT NULL DEFAULT 3,
    throttle_sms_per_sec INT,
    max_concat_override INT,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_by      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE campaign_commands (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id     UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    sequence        INT NOT NULL,
    application_id  UUID NOT NULL REFERENCES applications(id),
    script          BYTEA NOT NULL,
    expect_response BOOLEAN NOT NULL DEFAULT true,
    UNIQUE(campaign_id, sequence)
);

CREATE TABLE campaign_targets (
    campaign_id     UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    card_id         UUID NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (campaign_id, card_id)
);

-- UI / asset tables

CREATE TABLE cap_files (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename        TEXT NOT NULL,
    aid             TEXT NOT NULL,
    file_data       BYTEA NOT NULL,
    file_size       INT NOT NULL,
    sha256_hash     TEXT NOT NULL,
    load_file_hex   TEXT NOT NULL,
    sections        JSONB NOT NULL DEFAULT '[]'::jsonb,
    uploaded_by     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE scripts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    description     TEXT,
    target_tar      TEXT NOT NULL,
    commands        TEXT NOT NULL,
    command_count   INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE card_groups (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL UNIQUE,
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE card_group_members (
    card_group_id   UUID NOT NULL REFERENCES card_groups(id) ON DELETE CASCADE,
    card_id         UUID NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
    PRIMARY KEY (card_group_id, card_id)
);

ALTER TABLE campaigns
    ADD CONSTRAINT fk_campaigns_cap_file
    FOREIGN KEY (cap_file_id) REFERENCES cap_files(id) ON DELETE SET NULL;

ALTER TABLE campaigns
    ADD CONSTRAINT fk_campaigns_script
    FOREIGN KEY (script_id) REFERENCES scripts(id) ON DELETE SET NULL;

-- Planner-owned shard manifests for campaign activation fanout

CREATE TABLE campaign_shards (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id UUID NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    sequence    INT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending','publishing','published')),
    item_count  INT NOT NULL DEFAULT 0,
    items       JSONB NOT NULL DEFAULT '[]'::jsonb,
    claimed_at  TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(campaign_id, sequence)
);

CREATE INDEX idx_campaign_shards_pending
    ON campaign_shards(created_at)
    WHERE status = 'pending' AND claimed_at IS NULL;
CREATE INDEX idx_campaign_shards_campaign_status
    ON campaign_shards(campaign_id, status, sequence);

-- Indexes

CREATE INDEX idx_cards_msisdn ON cards(msisdn);
CREATE INDEX idx_cards_iccid ON cards(iccid);
CREATE INDEX idx_cards_profile_status ON cards(profile_id, status);
CREATE INDEX idx_campaign_targets_campaign_card ON campaign_targets(campaign_id, card_id);
