"""
Bio Insights Engine - Natural Language to SQL Engine

Uses Claude Sonnet to translate health questions into Athena SQL,
then executes and formats the results.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import anthropic
import pandas as pd

from insights_engine.config import CLAUDE_CONFIG
from insights_engine.core.athena_client import AthenaClient

PROMPTS_DIR = Path(__file__).parent.parent / "prompts"


@dataclass
class NLToSQLResult:
    sql: str
    explanation: str
    assumptions: list[str] = field(default_factory=list)
    confidence: float = 0.0


@dataclass
class AnswerResult:
    question: str
    sql: str
    explanation: str
    assumptions: list[str]
    confidence: float
    data: pd.DataFrame
    answer: str
    execution_time_ms: int
    row_count: int
    error: str | None = None


class NLToSQLEngine:
    """Translates natural language questions to SQL via Claude, executes against Athena."""

    def __init__(
        self,
        athena: AthenaClient,
        model: str = None,
    ):
        self.athena = athena
        self.model = model or CLAUDE_CONFIG["nl_to_sql_model"]
        api_key = os.environ.get(CLAUDE_CONFIG["api_key_env"])
        if not api_key:
            raise ValueError(
                f"Set {CLAUDE_CONFIG['api_key_env']} environment variable"
            )
        self.client = anthropic.Anthropic(api_key=api_key)
        self._system_prompt = None
        self._examples = None

    def _load_system_prompt(self) -> str:
        """Load and hydrate the system prompt with live schema DDL."""
        if self._system_prompt is None:
            template = (PROMPTS_DIR / "nl_to_sql_system.txt").read_text()
            schema_ddl = self.athena.get_schema_ddl()
            self._system_prompt = template.replace("{schema_ddl}", schema_ddl)
        return self._system_prompt

    def _load_examples(self) -> str:
        """Load few-shot examples."""
        if self._examples is None:
            self._examples = (PROMPTS_DIR / "nl_to_sql_examples.txt").read_text()
        return self._examples

    def translate(
        self,
        question: str,
        history: list[dict] = None,
    ) -> NLToSQLResult:
        """Translate a natural language question to SQL."""
        system_prompt = self._load_system_prompt()
        examples = self._load_examples()

        messages = []

        # Add few-shot examples as a user message
        messages.append(
            {"role": "user", "content": f"Here are example translations:\n\n{examples}"}
        )
        messages.append(
            {"role": "assistant", "content": "I understand the schema and example patterns. I'm ready to translate questions to SQL."}
        )

        # Add conversation history if provided
        if history:
            for entry in history:
                messages.append({"role": "user", "content": entry["question"]})
                messages.append(
                    {"role": "assistant", "content": json.dumps(entry["result"])}
                )

        # Add the actual question
        messages.append({"role": "user", "content": question})

        response = self.client.messages.create(
            model=self.model,
            max_tokens=CLAUDE_CONFIG["max_tokens"],
            temperature=CLAUDE_CONFIG["temperature"],
            system=system_prompt,
            messages=messages,
        )

        # Parse response
        raw_text = response.content[0].text.strip()

        # Extract JSON from response (handle markdown code blocks)
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
            raw_text = raw_text.strip()

        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            # Try to find JSON in the response
            start = raw_text.find("{")
            end = raw_text.rfind("}") + 1
            if start >= 0 and end > start:
                parsed = json.loads(raw_text[start:end])
            else:
                raise ValueError(f"Could not parse Claude response as JSON: {raw_text[:200]}")

        return NLToSQLResult(
            sql=parsed.get("sql", ""),
            explanation=parsed.get("explanation", ""),
            assumptions=parsed.get("assumptions", []),
            confidence=parsed.get("confidence", 0.0),
        )

    def ask(
        self,
        question: str,
        history: list[dict] = None,
    ) -> AnswerResult:
        """End-to-end: translate question to SQL, execute, format answer."""
        start_time = time.time()

        # Step 1: Translate to SQL
        try:
            nl_result = self.translate(question, history)
        except Exception as e:
            return AnswerResult(
                question=question,
                sql="",
                explanation="",
                assumptions=[],
                confidence=0.0,
                data=pd.DataFrame(),
                answer="",
                execution_time_ms=int((time.time() - start_time) * 1000),
                row_count=0,
                error=f"Translation error: {e}",
            )

        # Step 2: Execute SQL
        try:
            df = self.athena.execute_query(nl_result.sql, timeout_sec=30)
        except Exception as e:
            return AnswerResult(
                question=question,
                sql=nl_result.sql,
                explanation=nl_result.explanation,
                assumptions=nl_result.assumptions,
                confidence=nl_result.confidence,
                data=pd.DataFrame(),
                answer="",
                execution_time_ms=int((time.time() - start_time) * 1000),
                row_count=0,
                error=f"Query execution error: {e}",
            )

        # Step 3: Format answer
        answer = self._format_answer(question, nl_result, df)
        elapsed_ms = int((time.time() - start_time) * 1000)

        return AnswerResult(
            question=question,
            sql=nl_result.sql,
            explanation=nl_result.explanation,
            assumptions=nl_result.assumptions,
            confidence=nl_result.confidence,
            data=df,
            answer=answer,
            execution_time_ms=elapsed_ms,
            row_count=len(df),
        )

    def _format_answer(
        self, question: str, nl_result: NLToSQLResult, df: pd.DataFrame
    ) -> str:
        """Use Claude to generate a natural language answer from query results."""
        if df.empty:
            return "No data found for this query."

        # Truncate data for the prompt
        data_preview = df.head(20).to_string(index=False)
        if len(df) > 20:
            data_preview += f"\n... ({len(df)} total rows)"

        prompt = f"""The user asked: "{question}"

I ran this SQL query: {nl_result.sql}

The query returned {len(df)} rows. Here are the results:

{data_preview}

Write a concise, natural language answer to the user's question based on these results. Include specific numbers. If there are statistical caveats (small sample size, missing data), mention them briefly. Keep it to 2-4 sentences."""

        response = self.client.messages.create(
            model=self.model,
            max_tokens=512,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
