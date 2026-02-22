"""
Experiment Tracker — Data Model & S3 Storage

Stores interventions as JSON in the Gold S3 bucket.
Uses S3 conditional writes (If-Match on ETag) for optimistic locking.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from enum import Enum
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from insights_engine.config import AWS_CONFIG


class InterventionType(str, Enum):
    SUPPLEMENT = "supplement"
    TRAINING_CHANGE = "training_change"
    DIET = "diet"
    SLEEP_PROTOCOL = "sleep_protocol"
    RECOVERY_MODALITY = "recovery_modality"


@dataclass
class Intervention:
    id: str
    name: str
    type: InterventionType
    details: str
    start_date: str  # ISO format YYYY-MM-DD
    end_date: Optional[str] = None
    washout_days: int = 3
    notes: str = ""
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    @property
    def is_active(self) -> bool:
        if self.end_date is None:
            return True
        return date.fromisoformat(self.end_date) >= date.today()

    def to_dict(self) -> dict:
        d = asdict(self)
        d["type"] = self.type.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> Intervention:
        d = d.copy()
        d["type"] = InterventionType(d["type"])
        return cls(**d)


class ExperimentStore:
    """CRUD for interventions stored as S3 JSON with optimistic locking."""

    S3_KEY = "experiments/interventions.json"

    def __init__(self, bucket: str | None = None):
        self.bucket = bucket or AWS_CONFIG.get("gold_bucket", "")
        if not self.bucket:
            self.bucket = f"bio-lakehouse-gold-069899605581"
        self.s3 = boto3.client("s3", region_name=AWS_CONFIG.get("aws_region", "us-east-1"))
        self._etag: str | None = None

    def _read_raw(self) -> tuple[list[dict], str | None]:
        """Read interventions JSON from S3. Returns (data, etag)."""
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=self.S3_KEY)
            etag = resp.get("ETag")
            data = json.loads(resp["Body"].read().decode("utf-8"))
            return data, etag
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return [], None
            raise

    def _write_raw(self, data: list[dict], expected_etag: str | None = None) -> None:
        """Write interventions JSON to S3 with optional optimistic locking."""
        body = json.dumps(data, indent=2, default=str)
        put_kwargs = {
            "Bucket": self.bucket,
            "Key": self.S3_KEY,
            "Body": body.encode("utf-8"),
            "ContentType": "application/json",
        }

        # Optimistic locking: if we have an etag, use If-Match
        if expected_etag:
            try:
                put_kwargs["IfMatch"] = expected_etag
                self.s3.put_object(**put_kwargs)
                return
            except ClientError as e:
                if e.response["Error"]["Code"] in ("PreconditionFailed", "412"):
                    # Conflict — reload and retry once
                    print("S3 write conflict detected, reloading and retrying...")
                    existing, new_etag = self._read_raw()
                    # Merge: add any new items from data not in existing
                    existing_ids = {item["id"] for item in existing}
                    for item in data:
                        if item["id"] not in existing_ids:
                            existing.append(item)
                        else:
                            # Update existing item
                            existing = [item if e["id"] == item["id"] else e for e in existing]
                    put_kwargs.pop("IfMatch", None)
                    put_kwargs["Body"] = json.dumps(existing, indent=2, default=str).encode("utf-8")
                    self.s3.put_object(**put_kwargs)
                    return
                raise

        self.s3.put_object(**put_kwargs)

    def list_interventions(self) -> list[Intervention]:
        """Return all interventions."""
        data, self._etag = self._read_raw()
        return [Intervention.from_dict(d) for d in data]

    def get_intervention(self, intervention_id: str) -> Intervention | None:
        """Get a single intervention by ID."""
        for intv in self.list_interventions():
            if intv.id == intervention_id:
                return intv
        return None

    def add_intervention(self, intervention: Intervention) -> Intervention:
        """Add a new intervention."""
        data, etag = self._read_raw()
        data.append(intervention.to_dict())
        self._write_raw(data, etag)
        return intervention

    def end_intervention(self, intervention_id: str, end_date: str | None = None) -> Intervention | None:
        """Mark an intervention as ended."""
        data, etag = self._read_raw()
        end_dt = end_date or date.today().isoformat()
        updated = None
        for item in data:
            if item["id"] == intervention_id:
                item["end_date"] = end_dt
                updated = Intervention.from_dict(item)
        if updated:
            self._write_raw(data, etag)
        return updated

    def delete_intervention(self, intervention_id: str) -> bool:
        """Delete an intervention."""
        data, etag = self._read_raw()
        new_data = [d for d in data if d["id"] != intervention_id]
        if len(new_data) == len(data):
            return False
        self._write_raw(new_data, etag)
        return True

    def active_interventions(self) -> list[Intervention]:
        """Return only active (not ended) interventions."""
        return [i for i in self.list_interventions() if i.is_active]

    def check_overlaps(self, new_start: str, new_end: str | None = None) -> list[Intervention]:
        """Check for temporal overlaps with existing interventions (confound warnings)."""
        new_start_dt = date.fromisoformat(new_start)
        new_end_dt = date.fromisoformat(new_end) if new_end else date.today()

        overlaps = []
        for intv in self.list_interventions():
            intv_start = date.fromisoformat(intv.start_date)
            intv_end = date.fromisoformat(intv.end_date) if intv.end_date else date.today()

            if intv_start <= new_end_dt and intv_end >= new_start_dt:
                overlaps.append(intv)

        return overlaps

    @staticmethod
    def new_id() -> str:
        return str(uuid.uuid4())[:8]
