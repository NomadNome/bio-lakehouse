# Data Ingestion Setup Guide

Complete guide to setting up automated biometric data ingestion from Oura Ring and Peloton.

## Prerequisites

- AWS Account with CLI configured
- Python 3.9+
- Node.js 16+ (for tests)
- OpenClaw installed (for Peloton automation): https://openclaw.ai
- Oura Ring account: https://cloud.ouraring.com
- Peloton account: https://members.onepeloton.com

## Part 1: Oura API Lambda Ingestion

### Step 1: Create OAuth Application

1. Go to https://cloud.ouraring.com/oauth/applications
2. Click "Create Application"
3. Fill in:
   - **Name:** Bio Lakehouse Ingestion
   - **Redirect URI:** `http://localhost:8080/callback`
   - **Scope:** Select all available scopes (daily, heartrate, personal, session, sleep, spo2, tag, workout)
4. Save **Client ID** and **Client Secret**

### Step 2: Configure Credentials

```bash
# Copy example file
cp .env.oura.example .env.oura

# Edit with your credentials
nano .env.oura
```

Paste your Client ID and Secret:
```bash
OURA_CLIENT_ID=your-client-id-here
OURA_CLIENT_SECRET=your-client-secret-here
OURA_REDIRECT_URI=http://localhost:8080/callback
OURA_ACCESS_TOKEN=  # Leave blank for now
```

### Step 3: Run OAuth Flow

```bash
# Install dependencies
npm install express open

# Run OAuth helper
node scripts/automation/oura-oauth.js
```

This will:
1. Open your browser to Oura authorization page
2. After you approve, it redirects to localhost:8080
3. The script captures the authorization code
4. Exchanges it for an access token
5. Saves tokens to `.oura-tokens.json`

**Copy the access token** into `.env.oura`:
```bash
OURA_ACCESS_TOKEN=_0XBPWQQ_abcd1234-your-token-here
```

### Step 4: Deploy CloudFormation Stack

```bash
# Set environment variables
export BRONZE_BUCKET="bio-lakehouse-bronze-$(aws sts get-caller-identity --query Account --output text)"
export OURA_ACCESS_TOKEN=$(cat .env.oura | grep OURA_ACCESS_TOKEN | cut -d= -f2)

# Deploy stack
aws cloudformation deploy \
  --template-file infrastructure/cloudformation/oura-ingest-stack.yaml \
  --stack-name bio-lakehouse-oura-ingest \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    BronzeBucket=$BRONZE_BUCKET \
    OuraAccessToken=$OURA_ACCESS_TOKEN
```

### Step 5: Test Lambda Function

```bash
# Run unit tests
node tests/unit/test-oura-lambda.js

# Run integration tests (requires deployed stack)
./tests/test-oura-lambda-integration.sh
```

Expected output:
```
🧪 Running Oura Lambda Tests
✅ S3 keys follow bronze layer convention
✅ API response transforms correctly for S3
...
📊 Results: 8 passed, 0 failed
```

### Step 6: Verify Automated Schedule

The Lambda function is scheduled to run daily at 9:00 AM EST via EventBridge.

**Check next run:**
```bash
aws events describe-rule --name bio-lakehouse-oura-daily-ingest
```

**Trigger manually for testing:**
```bash
aws lambda invoke \
  --function-name bio-lakehouse-oura-api-ingest \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  /tmp/lambda-response.json

cat /tmp/lambda-response.json | jq
```

---

## Part 2: Peloton Browser Automation (OpenClaw)

### Step 1: Install OpenClaw

```bash
# macOS
brew install openclaw

# Or via npm
npm install -g openclaw

# Verify installation
openclaw status
```

### Step 2: Configure Scripts

```bash
# Set your AWS account ID and Peloton username
export AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export BRONZE_BUCKET="bio-lakehouse-bronze-$AWS_ACCOUNT"
export PELOTON_USERNAME="YourPelotonUsername"

# Update scripts
sed -i '' "s/YOUR_AWS_ACCOUNT/$AWS_ACCOUNT/g" scripts/automation/peloton-sync.sh
sed -i '' "s/YOUR_PELOTON_USERNAME/$PELOTON_USERNAME/g" scripts/automation/peloton-sync.sh
```

### Step 3: Test Manual Sync

```bash
# Make scripts executable
chmod +x scripts/automation/peloton-sync.sh
chmod +x scripts/automation/peloton-manual-sync

# Run manual sync
cd scripts/automation
./peloton-manual-sync
```

This will:
1. Open Peloton workouts page in your browser
2. Click "Download Workouts" button automatically
3. Wait for CSV download
4. Upload to S3 with encryption
5. Log to DynamoDB

### Step 4: Schedule Cron Job

**Using OpenClaw CLI:**
```bash
# Create weekly Sunday 8 AM EST schedule
openclaw cron add \
  --name "Peloton Weekly Sync" \
  --schedule "0 8 * * 0" \
  --command "./scripts/automation/peloton-manual-sync" \
  --timezone "America/New_York"
```

**Or add via OpenClaw agent:**
```
You: Schedule peloton-manual-sync to run every Sunday at 8 AM EST
```

### Step 5: Verify Cron Job

```bash
# List all cron jobs
openclaw cron list

# Trigger manually for testing
openclaw cron run <job-id>
```

---

## Part 3: Oura Backup Script (Optional)

For redundancy, you can also run the bash-based Oura sync alongside the Lambda.

### Configure Environment

```bash
# Add to ~/.zshrc or ~/.bashrc
export OURA_ACCESS_TOKEN="$(cat ~/.openclaw/workspace/.env.oura | grep OURA_ACCESS_TOKEN | cut -d= -f2)"
export BRONZE_BUCKET="bio-lakehouse-bronze-YOUR_AWS_ACCOUNT"
```

### Schedule Cron Job

```bash
# Daily 9 AM EST (runs in parallel with Lambda)
openclaw cron add \
  --name "Oura Daily Sync (Backup)" \
  --schedule "0 9 * * *" \
  --command "~/.openclaw/workspace/scripts/oura-sync.sh" \
  --timezone "America/New_York"
```

---

## Monitoring

### Check Recent Ingestions

```bash
# DynamoDB logs
aws dynamodb scan \
  --table-name bio_ingestion_log \
  --filter-expression "#src = :source" \
  --expression-attribute-names '{"#src": "source"}' \
  --expression-attribute-values '{":source": {"S": "oura"}}' \
  --max-items 10 \
  --query 'Items[*].[data_type.S, record_count.N, upload_timestamp.N]' \
  --output table

# S3 files
aws s3 ls s3://bio-lakehouse-bronze-YOUR_AWS_ACCOUNT/oura/ --recursive | tail -20
aws s3 ls s3://bio-lakehouse-bronze-YOUR_AWS_ACCOUNT/peloton/ --recursive | tail -20
```

### CloudWatch Logs

```bash
# Lambda logs
aws logs tail /aws/lambda/bio-lakehouse-oura-api-ingest --follow

# Filter for errors
aws logs filter-pattern "ERROR" \
  --log-group-name /aws/lambda/bio-lakehouse-oura-api-ingest \
  --start-time $(date -u -d '1 hour ago' +%s)000
```

---

## Troubleshooting

### Oura Lambda: "Unauthorized" Error

**Cause:** Access token expired (typically after 30 days)

**Fix:**
1. Run OAuth flow again: `node scripts/automation/oura-oauth.js`
2. Update SSM parameter:
```bash
aws ssm put-parameter \
  --name /bio-lakehouse/oura-api-token \
  --value "YOUR_NEW_TOKEN" \
  --type SecureString \
  --overwrite
```

### Peloton: CSV Not Downloading

**Cause:** Browser automation button selector changed or Peloton requires re-login

**Fix:**
1. Log into Peloton manually: https://members.onepeloton.com
2. Verify "Download Workouts" button is visible on profile page
3. Re-run `peloton-manual-sync` and check OpenClaw logs

### S3 Upload: "Access Denied"

**Cause:** Bucket encryption policy requires SSE-AES256 header

**Fix:** Ensure all scripts use `--sse AES256` flag:
```bash
aws s3 cp file.json s3://bucket/key --sse AES256
```

### DynamoDB: Item Not Found

**Cause:** `source` is a reserved keyword in DynamoDB

**Fix:** Use expression attribute names in queries:
```bash
--expression-attribute-names '{"#src": "source"}'
```

---

## Security Checklist

- [ ] `.env.oura` added to `.gitignore`
- [ ] `.oura-tokens.json` never committed
- [ ] AWS credentials rotated regularly
- [ ] Lambda IAM role uses least-privilege permissions
- [ ] S3 bucket encryption enforced via bucket policy
- [ ] SSM parameters use SecureString type
- [ ] CloudFormation stacks use IAM capabilities flag

---

## Next Steps

Once ingestion is operational:

1. **Validate Bronze Data:** Check S3 files for correct format and completeness
2. **Deploy Silver Layer:** Run Glue normalizers to transform raw data
3. **Create Gold Views:** Build analytical views in Athena
4. **Build Insights Engine:** Deploy Claude-powered analytics dashboard
5. **Set Up Weekly Reports:** Schedule automated HTML report generation

See main README.md for complete platform setup.
