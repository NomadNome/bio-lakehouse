# Technical Debt

## Data-source lifecycle

- [x] **Make MyFitnessPal optional and remove it from the daily critical path.**
  The manual export workflow was retired by the primary user on 2026-09-07
  because daily exports were too cumbersome. Preserve the historical MFP data,
  An instance-level `BIO_MFP_ENABLED` feature switch now ensures the runner does not
  upload the last stale export, start the MFP normalizer, or wait for that job
  when MFP is disabled. The dashboard and documentation describe nutrition as
  an optional historical source. Evaluate a lower-friction
  nutrition integration only if the feature becomes valuable again.
