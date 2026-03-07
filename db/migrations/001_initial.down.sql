DROP INDEX IF EXISTS idx_campaign_shards_pending;
DROP INDEX IF EXISTS idx_campaign_shards_campaign_status;
DROP INDEX IF EXISTS idx_campaign_targets_campaign_card;
DROP INDEX IF EXISTS idx_cards_msisdn;
DROP INDEX IF EXISTS idx_cards_iccid;
DROP INDEX IF EXISTS idx_cards_profile_status;

DROP TABLE IF EXISTS campaign_shards;

ALTER TABLE campaigns DROP CONSTRAINT IF EXISTS fk_campaigns_script;
ALTER TABLE campaigns DROP CONSTRAINT IF EXISTS fk_campaigns_cap_file;

DROP TABLE IF EXISTS card_group_members;
DROP TABLE IF EXISTS card_groups;
DROP TABLE IF EXISTS scripts;
DROP TABLE IF EXISTS cap_files;

DROP TABLE IF EXISTS campaign_targets;
DROP TABLE IF EXISTS campaign_commands;
DROP TABLE IF EXISTS campaigns;
DROP TABLE IF EXISTS card_counters;
DROP TABLE IF EXISTS cards;
DROP TABLE IF EXISTS applications;
DROP TABLE IF EXISTS profiles;
