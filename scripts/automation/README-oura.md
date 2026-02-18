# Oura Ring API Integration

Automated daily sync of Oura Ring health data to your AWS bio-optimization data lake.

## Setup Complete ✅

- **Scheduled:** Every day at 9:00 AM EST
- **Next run:** Thursday, February 19, 2026 at 9:00 AM EST
- **Destination:** `s3://bio-lakehouse-bronze-YOUR_AWS_ACCOUNT/oura/`
- **Tracking:** DynamoDB table `bio_ingestion_log`

## OAuth Credentials

**Stored in:**
- `.env.oura` - Access token and credentials
- `.oura-tokens.json` - Full OAuth token data (includes refresh token)

**Client ID:** Configured in `.env.oura` (see `.env.oura.example`)

## Data Types Synced

The automation pulls the last 7 days of data from these endpoints:

1. **Readiness** - Daily readiness scores and contributors
2. **Sleep** - Sleep stages, quality, efficiency
3. **Activity** - Steps, calories, movement
4. **Heart Rate** - Continuous HR throughout the day
5. **SpO2** - Blood oxygen saturation

## How It Works

1. **API Authentication** (OAuth 2.0):
   - Credentials stored in `.env.oura`
   - Access token refreshes automatically via refresh token

2. **Daily Sync** (`oura-sync.sh`):
   - Fetches last 7 days from each endpoint
   - Saves JSON to S3 bronze with partitioning: `year=YYYY/month=MM/day=DD/`
   - Logs to DynamoDB with metadata

3. **WhatsApp Notification**:
   - Confirms completion
   - Reports record counts per data type

## Files

- **oura-oauth.js** - OAuth flow (one-time setup, already complete)
- **oura-sync.sh** - Daily sync script
- **.env.oura** - Credentials (DO NOT COMMIT TO GIT)
- **.oura-tokens.json** - OAuth tokens

## Manual Trigger

To run outside the schedule:

```bash
cd ~/.openclaw/workspace/scripts
./oura-sync.sh
```

Or specify a date:

```bash
./oura-sync.sh 2026-02-17
```

## Test Results (Feb 18, 2026)

✅ **Readiness:** 7 records  
✅ **Sleep:** 7 records  
✅ **Activity:** 6 records  
✅ **Heart Rate:** 395 records  
✅ **SpO2:** 7 records  

All uploaded to S3 and logged to DynamoDB.

## Cron Job

View/manage the scheduled job:

```bash
# List all cron jobs
openclaw cron list

# View next run time
openclaw cron list | grep -A5 "Oura"

# Trigger now (for testing)
openclaw cron run 47560be6-1b1c-4ff8-82bd-3c4cb0c33463
```

## Data Flow

```
Oura API → JSON → S3 Bronze → (future) Silver → Gold
                        ↓
                   DynamoDB Log
                        ↓
                  WhatsApp Alert
```

## API Endpoints

- Personal Info: `/v2/usercollection/personal_info`
- Readiness: `/v2/usercollection/daily_readiness`
- Sleep: `/v2/usercollection/daily_sleep`
- Activity: `/v2/usercollection/daily_activity`
- Heart Rate: `/v2/usercollection/heartrate`
- SpO2: `/v2/usercollection/daily_spo2`

## Token Refresh

Access tokens expire after some time. The refresh token in `.oura-tokens.json` can be used to get a new access token:

```bash
# Re-run OAuth flow if token expires
node oura-oauth.js
```

(Note: Refresh token handling should be automated in future updates)

## Next Steps

Once data is flowing consistently:
1. Update silver normalizer to handle Oura JSON schema
2. Join with Peloton data in readiness aggregator
3. Build unified health dashboard (sleep + workouts + readiness)
