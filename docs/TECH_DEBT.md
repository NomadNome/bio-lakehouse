# Technical Debt

## Data-source lifecycle

- [ ] **Make MyFitnessPal optional and remove it from the daily critical path.**
  The manual export workflow was retired by the primary user on 2026-09-07
  because daily exports were too cumbersome. Preserve the historical MFP data,
  but add an instance-level feature switch so the ingestion runner does not
  upload the last stale export, start the MFP normalizer, or wait for that job
  when MFP is disabled. Update the dashboard and documentation to describe
  nutrition as an optional historical source, and evaluate a lower-friction
  nutrition integration only if the feature becomes valuable again.
