# Peloton Workout Sync Automation

Automated pipeline to sync Peloton workout exports to your AWS bio-optimization data lake.

## Setup Complete ✅

- **Scheduled:** Every Sunday at 8:00 AM EST
- **Destination:** `s3://bio-lakehouse-bronze-YOUR_AWS_ACCOUNT/peloton/workouts/`
- **Tracking:** DynamoDB table `bio_ingestion_log`

## How It Works

1. **Browser automation** (via Santal/OpenClaw agent):
   - Opens Peloton workouts page
   - Clicks "Download Workouts"
   - Waits for CSV download

2. **Upload script** (`peloton-sync.sh`):
   - Finds latest CSV in Downloads
   - Uploads to S3 bronze with encryption
   - Logs to DynamoDB with metadata

3. **WhatsApp notification**:
   - Confirms completion
   - Reports record count and S3 path

## Manual Trigger

To run outside the schedule:

```bash
cd ~/.openclaw/workspace/scripts
./peloton-manual-sync
```

This will:
1. Open the Peloton page in your browser
2. Wait for you to click "Download Workouts"
3. Automatically upload to S3

## Files

- **peloton-sync.sh** - Main upload script
- **peloton-manual-sync** - Interactive manual trigger
- **peloton-download.js** - Browser automation helper (for future use)

## Cron Job

View/manage the scheduled job:

```bash
# List all cron jobs
openclaw cron list

# View next run time
openclaw cron list | grep -A5 "Peloton"

# Trigger now (for testing)
openclaw cron run <job-id>
```

## Data Flow

```
Peloton → Downloads → S3 Bronze → (future) Silver → Gold
                              ↓
                         DynamoDB Log
                              ↓
                        WhatsApp Alert
```

## Next Steps

Once data is flowing consistently:
1. Update silver normalizer to handle Peloton schema
2. Add Peloton metrics to readiness aggregator
3. Build dashboards combining Oura + Peloton data
