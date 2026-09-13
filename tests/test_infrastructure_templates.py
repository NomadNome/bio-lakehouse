"""Regression checks for critical CloudFormation contracts."""

from pathlib import Path


ROOT = Path(__file__).parent.parent


def template(name):
    return (ROOT / "infrastructure" / "cloudformation" / name).read_text()


def test_ingestion_timestamp_key_is_numeric():
    bronze = template("bronze-stack.yaml")
    assert "AttributeName: upload_timestamp\n          AttributeType: N" in bronze
    assert "UpdateReplacePolicy: Retain" in bronze


def test_bronze_bucket_declares_csv_and_json_notifications():
    bronze = template("bronze-stack.yaml")
    assert "NotificationConfiguration:" in bronze
    assert "Value: .csv" in bronze
    assert "Value: .json" in bronze


def test_lambda_packages_can_be_versioned_for_safe_rollout():
    bronze = template("bronze-stack.yaml")
    briefing = template("morning-briefing-stack.yaml")
    orchestrator = template("pipeline-orchestrator-stack.yaml")

    assert "IngestionLambdaCodeS3Key:" in bronze
    assert "S3Key: !Ref IngestionLambdaCodeS3Key" in bronze
    assert "MorningBriefingCodeS3Key:" in briefing
    assert "S3Key: !Ref MorningBriefingCodeS3Key" in briefing
    assert "PipelineOrchestratorCodeS3Key:" in orchestrator
    assert "S3Key: !Ref PipelineOrchestratorCodeS3Key" in orchestrator


def test_legacy_duplicate_gold_trigger_is_off_by_default():
    gold = template("gold-stack.yaml")
    assert "EnableLegacyNormalizerTrigger:" in gold
    assert "Default: 'false'" in gold
    assert "Condition: LegacyNormalizerTriggerEnabled" in gold


def test_briefing_ssm_paths_are_scoped_by_project_prefix():
    briefing = template("morning-briefing-stack.yaml")
    assert "ANTHROPIC_KEY_PARAM: !Sub '/${ProjectPrefix}/anthropic-api-key'" in briefing
    assert "OURA_TOKEN_PARAM: !Sub '/${ProjectPrefix}/oura-api-token'" in briefing
