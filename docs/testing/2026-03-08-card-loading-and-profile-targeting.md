# Card Loading And Profile Targeting

## What changed

### CSV import
- `POST /api/v1/cards/import` now imports cards in batches instead of one row at a time.
- Current batch size: `2000`
- Request size limit was raised from `10 MiB` to `100 MiB`
- Duplicate cards are ignored with `ON CONFLICT DO NOTHING`

This makes loading large card inventories practical through the real API.

### Campaign targeting by profile
- `POST /api/v1/campaigns` with `profile_id` no longer loads all target card IDs into application memory.
- Campaign targets are now materialized directly in PostgreSQL with:
  - `INSERT INTO campaign_targets ... SELECT ... FROM cards WHERE profile_id = ?`
- Explicit `card_ids`, `profile_id`, and `card_group_id` selectors can be combined.
- Target deduplication is enforced by the `campaign_targets` primary key.

This means campaigns over entire profiles now scale with the database instead of API process memory.

## Seeded dataset

A dataset of `100000` cards was loaded on `2026-03-08` using the real API.

- profiles: `10`
- cards per profile: `10000`
- prefix: `perf100k2`

The exact IDs are in:

- [perf100k-seed-manifest.json](/home/ubuntu/ota-platform/docs/testing/perf100k-seed-manifest.json)

## Profile inventory

1. `perf100k2-profile-01`
   - profile_id: `9f946e73-6c05-47ac-9f20-43a861faf1b1`
   - application_id: `1f6cb780-f657-44d9-ae68-33337979950a`
2. `perf100k2-profile-02`
   - profile_id: `9fd707f2-fdd4-41dd-9a2f-e26c6e482adb`
   - application_id: `45aabd14-cc6f-4af9-9a8f-1045c4a53e1f`
3. `perf100k2-profile-03`
   - profile_id: `5598f44b-1764-4c5c-8015-a011629a2b23`
   - application_id: `51560e9a-73d0-44bb-836a-909359954c23`
4. `perf100k2-profile-04`
   - profile_id: `5fea6494-f754-4ee6-ab4a-56db13149425`
   - application_id: `67190237-4a80-41fb-acd4-cc25788a33bd`
5. `perf100k2-profile-05`
   - profile_id: `33770eca-1552-4694-8482-6e3d5745d014`
   - application_id: `dfe1bd0c-5db2-486d-b04c-81a12d1cd242`
6. `perf100k2-profile-06`
   - profile_id: `3d88ef7e-1f3c-44e8-8f67-828aeae4fc9d`
   - application_id: `07495db2-9baa-4e7b-bbfe-118787599181`
7. `perf100k2-profile-07`
   - profile_id: `581d52ee-90df-4ca9-9e5e-4a3b6ffde59d`
   - application_id: `ebdbb07d-2fe0-4f6e-bce0-3b58af219446`
8. `perf100k2-profile-08`
   - profile_id: `3be4115b-e8a2-4bce-88ef-a75621eeb4ea`
   - application_id: `80ed69a9-47c7-4fe3-b3de-b7b940050b54`
9. `perf100k2-profile-09`
   - profile_id: `634c6a6d-801a-4fc5-a83f-6d275faa96de`
   - application_id: `2b3b3325-a91e-400f-ae2e-cb4eb1977c8a`
10. `perf100k2-profile-10`
   - profile_id: `9726cdc9-75b3-4399-b697-c9b99abafdd6`
   - application_id: `69449afd-0492-45cc-bb4c-699d0001fd83`

## Seeder command

The dataset was loaded with:

`cmd/seed-card-dataset`

Example:

```bash
CGO_ENABLED=0 GOCACHE=/tmp/go-build go build -o /tmp/seed-card-dataset ./cmd/seed-card-dataset

docker run --rm \
  --network deployments_default \
  -v /tmp/seed-card-dataset:/seed-card-dataset:ro \
  -v /home/ubuntu/ota-platform:/workspace \
  alpine:3.20 \
  sh -lc '/seed-card-dataset \
    -base-url http://ota-api:8080 \
    -prefix perf100k2 \
    -profiles 10 \
    -cards-per-profile 10000 \
    -timeout 60m \
    -out /workspace/docs/testing/perf100k-seed-manifest.json'
```

## Expected campaign usage

To target an entire profile, create a campaign with:

- `profile_id`
- command list
- `application_id` matching that profile

The API now inserts the `campaign_targets` set in the database directly, so creating a 10k-card profile campaign should not require loading 10k card IDs into the API process.
