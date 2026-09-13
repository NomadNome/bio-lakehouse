# Safe Deployment Gate

The repository can be tested locally without changing AWS. Infrastructure changes must be reviewed as a CloudFormation change set, one stack and one instance at a time. Do not deploy every stack in a loop.

## Current staged changes

| Stack | Intended change | Deployment concern |
|-------|-----------------|--------------------|
| Bronze | Restore S3 event notifications, add the MFP job setting, scope Glue job access, and make ingestion timestamps numeric | A DynamoDB key-type change can be classified as table replacement. Never execute a change set that replaces a populated ingestion table. |
| Gold | Disable the legacy “any normalizer succeeded” Gold trigger by default | Confirm the EventBridge orchestrator or local pipeline owns downstream refresh before removing the legacy trigger. |
| Morning briefing | Resolve database and SSM paths from the instance prefix | The prefixed Anthropic and Oura parameters must exist before updating the function. |
| Pipeline orchestrator | Honor the local-pipeline lock, shorten bounded waits, and invoke the briefing asynchronously | Upload the matching Lambda package before updating the function configuration. |

## Required review sequence

1. Confirm the target account, region, stack, and project prefix. Capture the current stack parameters, S3 notification configuration, Lambda environment, and ingestion-table schema.
2. Run the local unit tests and template parser.
3. Detect drift on custom-named stateful resources before trusting a change-set replacement estimate. A normal change set can understate replacement risk when the live resource differs from the stack's recorded schema.
4. Create a CloudFormation change set without executing it. Reject it if it replaces or deletes a populated bucket, table, database, role, or function unexpectedly.
5. Verify required SSM parameters exist for that instance. Never copy secret values into commands, logs, source files, or change-set parameters.
6. Package each Lambda under an immutable, versioned S3 key and pass that key to the stack. Keep the prior package as the rollback target.
7. Apply one stack only. Wait for completion, then verify its health before moving to the next stack.
8. Upload one encrypted test object and confirm exactly one intended normalizer run. For the full local pipeline, confirm the S3 lock suppresses event-driven starts and that one Gold refresh occurs.
9. Verify Silver and Gold freshness, the dashboard, and the morning briefing. Retain the prior Lambda packages until the observation window is complete.

## Instance-specific DynamoDB handling

- **Primary:** The numeric live table was reconciled on 2026-09-06 using the AWS-supported retain, detach, and import sequence. An on-demand backup was taken first, all 22,154 items remained present, and resource drift was `IN_SYNC` after import.
- **Second instance:** The empty string-key table was retained and replaced with `bio_diego_ingestion_log_v2`, which uses a numeric timestamp key. Keep the legacy table through the observation window.

## Rollout record — 2026-09-06

- Primary and second-instance Bronze stacks are `UPDATE_COMPLETE`; both ingestion Lambdas use versioned packages, `.csv` and `.json` notifications, scoped Glue access, and numeric ingestion timestamps.
- Primary pipeline orchestrator and morning briefing are `UPDATE_COMPLETE`. The orchestrator lock smoke test returned 200 without starting compute.
- The Primary duplicate ANY-normalizer Gold trigger was removed after the EventBridge orchestrator was verified enabled.
- The second-instance legacy Gold trigger remains enabled until an equivalent orchestrator is deployed; removing it earlier would interrupt automatic downstream refreshes.
- Encrypted zero-byte S3 smoke objects invoked exactly the intended Lambda in both Bronze buckets and started no Glue jobs.

## Stop conditions

Stop before execution if a change set includes an unexpected replacement, required secrets are missing, another pipeline run is active, or the rollback path has not been identified. Repository changes being tested successfully does not make an infrastructure replacement safe.
