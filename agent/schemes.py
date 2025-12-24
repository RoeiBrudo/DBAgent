
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class SQLExecution:
    executed: bool
    success: bool
    status: str
    elapsed_ms: Optional[float]
    results: List[Any]
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "executed": self.executed,
            "success": self.success,
            "status": self.status,
            "elapsed_ms": self.elapsed_ms,
            "results": self.results,
            "error": self.error,
        }


@dataclass(frozen=True)
class AgentStep:
    reasoning: str
    sql: Optional[str]
    execution: Optional[SQLExecution]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "reasoning": self.reasoning,
            "sql": self.sql,
            "execution": self.execution.to_dict(),
        }


@dataclass(frozen=True)
class AgentResult:
    steps: List[AgentStep]
    final_answer: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "steps": [s.to_dict() for s in self.steps],
            "final_answer": self.final_answer,
        }

