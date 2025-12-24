import json
import os
import urllib.request
from typing import Any, Dict, List, Optional

from agents.prompts import (
    FINAL_ANSWER_SYSTEM_PROMPT,
    FINAL_ANSWER_USER_PROMPT_TEMPLATE,
    PLANNER_SYSTEM_PROMPT,
    PLANNER_USER_PROMPT_TEMPLATE,
)
from agents.schemes import AgentResult, QueryStep, SQLExecution
from agents.tools.db_tools import connect_to_db, safe_query


class BasicAgent:
    """A minimal multi-step text-to-SQL agent.

    The agent alternates between:
    - Asking the model for the next action (`query` or `final`).
    - Executing read-only SQL via `safe_query` when requested.
    - Producing a schema-driven `AgentResult` suitable for evaluation and UI rendering.
    """

    def __init__(
        self,
        model: str = "gpt-4o-mini",
        api_base: str = "https://api.openai.com/v1",
        request_timeout_s: int = 60,
    ):
        """Create a `BasicAgent`.

        Args:
            model: OpenAI model name.
            api_base: Base URL for the OpenAI API.
            request_timeout_s: Timeout (seconds) for the HTTP request.

        Raises:
            RuntimeError: If `OPENAI_API_KEY` is not set (environment or `.env`).
        """
        self.model = model
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.api_base = api_base.rstrip("/")
        self.request_timeout_s = request_timeout_s

        if not self.api_key:
            raise RuntimeError(
                "OPENAI_API_KEY is not set. Set it in your environment or in the project's .env file."
            )

    def _openai_chat(self, messages: List[Dict[str, str]]) -> str:
        """Call OpenAI Chat Completions and return the assistant message content.

        Args:
            messages: OpenAI chat messages.

        Returns:
            The assistant's `content` field as a string.

        Raises:
            urllib.error.URLError / HTTPError: On network failures or non-2xx responses.
            json.JSONDecodeError: If the API response is not valid JSON.
            KeyError: If the response JSON is missing expected fields.
        """
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0,
        }

        req = urllib.request.Request(
            url=f"{self.api_base}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=self.request_timeout_s) as resp:
            body = resp.read().decode("utf-8")
        parsed = json.loads(body)
        return parsed["choices"][0]["message"]["content"]

    @staticmethod
    def _extract_json(text: str) -> Dict[str, Any]:
        """Parse a JSON string returned by the model.

        Args:
            text: Raw model output expected to be valid JSON.

        Returns:
            Parsed JSON as a dict.

        Raises:
            ValueError: If the model output is not valid JSON.
        """
        try:
            return json.loads(text)
        except Exception as exc:
            raise ValueError("Model did not return valid JSON") from exc

    @staticmethod
    def _normalize_results(results: List[Any]) -> List[Any]:
        """Normalize DB results for JSON-serializable step logging.

        Converts sqlite row tuples into lists to ensure the `AgentResult` can be
        cleanly serialized (e.g., for evaluation outputs or a UI).
        """
        normalized = []
        for row in results:
            if isinstance(row, tuple):
                normalized.append(list(row))
            else:
                normalized.append(row)
        return normalized

    @staticmethod
    def _truncate_results(results: List[Any], max_rows: int = 20, max_cell_chars: int = 200) -> List[Any]:
        """Truncate results for prompts / logs.

        Args:
            results: Row-oriented results (typically list-of-lists).
            max_rows: Maximum rows to keep.
            max_cell_chars: Maximum string length per cell.

        Returns:
            A truncated version of the results safe to embed in prompts.
        """
        truncated = []
        for row in results[:max_rows]:
            if isinstance(row, list):
                new_row = []
                for cell in row:
                    s = str(cell)
                    if len(s) > max_cell_chars:
                        s = s[:max_cell_chars] + "..."
                    new_row.append(s)
                truncated.append(new_row)
            else:
                s = str(row)
                if len(s) > max_cell_chars:
                    s = s[:max_cell_chars] + "..."
                truncated.append(s)
        return truncated

    @staticmethod
    def _steps_for_prompt(steps: List[QueryStep]) -> str:
        """Serialize prior steps into a compact JSON string for prompts.

        The prompt includes limited/truncated results to control token usage.
        """
        serializable = []
        for i, s in enumerate(steps, start=1):
            serializable.append(
                {
                    "step": i,
                    "reasoning": s.reasoning,
                    "sql": s.sql,
                    "status": s.execution.status,
                    "success": s.execution.success,
                    "error": s.execution.error,
                    "results": BasicAgent._truncate_results(s.execution.results),
                }
            )
        return json.dumps(serializable, ensure_ascii=False)

    def run(
        self,
        question: str,
        db_file: str,
        msx_ms: int = 30_000,
        max_steps: int = 4,
    ) -> AgentResult | None:
        """Answer a question against a SQLite database using multi-step querying.

        The agent repeatedly asks the model for the next action:
        - `query`: execute the provided SQL via `safe_query` and append a step.
        - `final`: return a final answer immediately.

        If the agent does not produce a final answer within `max_steps`, a final
        summarization prompt is issued using all prior steps.

        Args:
            question: Natural-language user question.
            db_file: Path to the SQLite database file.
            msx_ms: Maximum milliseconds for each SQL execution in `safe_query`.
            max_steps: Maximum number of tool-using query steps.

        Returns:
            An `AgentResult` containing executed `QueryStep`s and a final answer.

        Raises:
            Exception: Propagates DB open errors, network/API errors, and JSON parsing
                errors (fail-fast by design).
        """
        conn, schema = connect_to_db(db_file)
        run_exc: Optional[BaseException] = None

        try:
            schema_text = json.dumps(schema, ensure_ascii=False)
            steps: List[QueryStep] = []
            final_answer: Optional[str] = None

            for _ in range(max_steps):
                planner_prompt = PLANNER_USER_PROMPT_TEMPLATE.format(
                    question=question,
                    schema=schema_text,
                    steps=self._steps_for_prompt(steps),
                )
                planner_resp_text = self._openai_chat(
                    [
                        {"role": "system", "content": PLANNER_SYSTEM_PROMPT},
                        {"role": "user", "content": planner_prompt},
                    ]
                )
                planner_payload = self._extract_json(planner_resp_text)
                action = str(planner_payload.get("action", "")).strip().lower()

                if action == "final":
                    final_answer = str(planner_payload.get("final_answer", "")).strip()
                    break

                if action != "query":
                    final_answer = "Agent returned an invalid action."
                    break

                reasoning = str(planner_payload.get("reasoning", ""))
                sql = str(planner_payload.get("sql", "")).strip()
                if not sql:
                    final_answer = "Agent did not provide SQL to execute."
                    break

                exec_dict = safe_query(conn, sql, msx_ms)
                execution = SQLExecution(
                    executed=True,
                    success=bool(exec_dict.get("success")),
                    status=str(exec_dict.get("status")),
                    elapsed_ms=exec_dict.get("elapsed_ms"),
                    results=self._normalize_results(exec_dict.get("results", [])),
                    error=exec_dict.get("error"),
                )
                steps.append(QueryStep(reasoning=reasoning, sql=sql, execution=execution))

            if final_answer is None:
                final_prompt = FINAL_ANSWER_USER_PROMPT_TEMPLATE.format(
                    question=question,
                    steps=self._steps_for_prompt(steps),
                )
                final_resp_text = self._openai_chat(
                    [
                        {"role": "system", "content": FINAL_ANSWER_SYSTEM_PROMPT},
                        {"role": "user", "content": final_prompt},
                    ]
                )
                final_payload = self._extract_json(final_resp_text)
                final_answer = str(final_payload.get("final_answer", "")).strip()

            return AgentResult(steps=steps, final_answer=final_answer)
        except BaseException as exc:
            run_exc = exc
            raise
        finally:
            try:
                conn.close()
            except BaseException:
                if run_exc is None:
                    raise
