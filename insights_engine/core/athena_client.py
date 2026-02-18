"""
Bio Insights Engine - Athena Client

Executes queries against Athena bio_gold database and returns pandas DataFrames.
Handles polling, result parsing, schema introspection, and basic caching.
"""

import hashlib
import time
from dataclasses import dataclass

import boto3
import pandas as pd

from insights_engine.config import AWS_CONFIG


@dataclass
class Column:
    name: str
    type: str
    comment: str = ""


class AthenaClient:
    """Manages Athena query execution and schema introspection."""

    def __init__(
        self,
        database: str = None,
        results_bucket: str = None,
    ):
        self.database = database or AWS_CONFIG["athena_database"]
        self.results_bucket = results_bucket or AWS_CONFIG["athena_results_bucket"]
        self.results_location = f"s3://{self.results_bucket}/"
        self.client = boto3.client("athena", region_name=AWS_CONFIG["aws_region"])
        self._schema_cache: dict[str, list[Column]] | None = None
        self._query_cache: dict[str, tuple[pd.DataFrame, float]] = {}
        self._cache_ttl_sec = 3600  # 1 hour

    def execute_query(self, sql: str, timeout_sec: int = 30) -> pd.DataFrame:
        """Execute SQL against Athena, return DataFrame.

        Uses a simple in-memory cache: identical SQL within cache TTL returns
        the cached DataFrame without re-querying Athena.
        """
        cache_key = hashlib.md5(sql.encode()).hexdigest()
        if cache_key in self._query_cache:
            df, cached_at = self._query_cache[cache_key]
            if time.time() - cached_at < self._cache_ttl_sec:
                return df

        # Start query
        execution = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.results_location},
            WorkGroup="primary",
        )
        execution_id = execution["QueryExecutionId"]

        # Poll for completion
        start = time.time()
        while True:
            response = self.client.get_query_execution(
                QueryExecutionId=execution_id
            )
            state = response["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                break
            elif state in ("FAILED", "CANCELLED"):
                reason = response["QueryExecution"]["Status"].get(
                    "StateChangeReason", "Unknown error"
                )
                raise RuntimeError(f"Athena query {state}: {reason}\nSQL: {sql}")

            if time.time() - start > timeout_sec:
                self.client.stop_query_execution(QueryExecutionId=execution_id)
                raise TimeoutError(
                    f"Athena query timed out after {timeout_sec}s\nSQL: {sql}"
                )
            time.sleep(1)

        # Fetch results
        df = self._fetch_results(execution_id)
        self._query_cache[cache_key] = (df, time.time())
        return df

    def _fetch_results(self, execution_id: str) -> pd.DataFrame:
        """Fetch all result pages from a completed Athena query."""
        rows = []
        columns = None
        next_token = None

        while True:
            kwargs = {"QueryExecutionId": execution_id, "MaxResults": 1000}
            if next_token:
                kwargs["NextToken"] = next_token

            response = self.client.get_query_results(**kwargs)
            result_set = response["ResultSet"]

            if columns is None:
                columns = [
                    col["Name"]
                    for col in result_set["ResultSetMetadata"]["ColumnInfo"]
                ]
                # Skip header row (first page only)
                data_rows = result_set["Rows"][1:]
            else:
                data_rows = result_set["Rows"]

            for row in data_rows:
                row_data = [datum.get("VarCharValue") for datum in row["Data"]]
                # Pad row to match column count (DESCRIBE can return sparse rows)
                while len(row_data) < len(columns):
                    row_data.append(None)
                rows.append(row_data[:len(columns)])

            next_token = response.get("NextToken")
            if not next_token:
                break

        if not columns:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=columns)
        return self._coerce_types(df)

    @staticmethod
    def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
        """Best-effort type coercion from Athena string results."""
        for col in df.columns:
            # Try numeric first
            numeric = pd.to_numeric(df[col], errors="coerce")
            if numeric.notna().sum() > 0 and numeric.notna().sum() >= df[col].notna().sum() * 0.5:
                df[col] = numeric
                continue
            # Try datetime
            if any(
                kw in col.lower()
                for kw in ["date", "timestamp", "week_start", "time"]
            ):
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception:
                    pass
        return df

    def get_schema(self, database: str = None) -> dict[str, list[Column]]:
        """Return {table_or_view_name: [Column(name, type, comment)]} for prompt context.

        Results are cached for the lifetime of the client instance.
        """
        if self._schema_cache is not None and database is None:
            return self._schema_cache

        db = database or self.database
        schema: dict[str, list[Column]] = {}

        # List tables and views
        tables_result = self.execute_query(f"SHOW TABLES IN {db}")
        if tables_result.empty:
            return schema

        table_names = tables_result.iloc[:, 0].tolist()

        # Also include the main Gold table if not in SHOW TABLES
        # (crawler-created tables may not appear in SHOW TABLES for views-only databases)
        if "daily_readiness_performance" not in table_names:
            table_names.append("daily_readiness_performance")

        for table_name in table_names:
            try:
                columns = self._get_column_metadata(db, table_name)
                schema[table_name] = columns
            except Exception as e:
                print(f"Warning: Could not describe {table_name}: {e}")

        if database is None:
            self._schema_cache = schema
        return schema

    def _get_column_metadata(self, db: str, table_name: str) -> list[Column]:
        """Get column names and Athena types using query metadata."""
        sql = f"SELECT * FROM {db}.{table_name} LIMIT 0"
        execution = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": db},
            ResultConfiguration={"OutputLocation": self.results_location},
            WorkGroup="primary",
        )
        execution_id = execution["QueryExecutionId"]

        # Poll
        for _ in range(30):
            resp = self.client.get_query_execution(QueryExecutionId=execution_id)
            state = resp["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                break
            elif state in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"Schema query failed for {table_name}")
            time.sleep(1)

        # Get metadata from results
        results = self.client.get_query_results(
            QueryExecutionId=execution_id, MaxResults=1
        )
        col_info = results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        return [
            Column(name=c["Name"], type=c["Type"])
            for c in col_info
        ]

    def get_schema_ddl(self, database: str = None) -> str:
        """Return schema as DDL-style text for injection into LLM prompts."""
        schema = self.get_schema(database)
        lines = []
        for table_name, columns in sorted(schema.items()):
            lines.append(f"-- {database or self.database}.{table_name}")
            lines.append(f"-- Columns:")
            for col in columns:
                lines.append(f"--   {col.name} {col.type}")
            lines.append("")
        return "\n".join(lines)
