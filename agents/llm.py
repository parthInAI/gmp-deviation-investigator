"""Chat models for each role, and a cost meter fed by every model call's token usage.

LangSmith tracing switches on by itself when LANGSMITH_TRACING=true and LANGSMITH_API_KEY are set
(see .env.example); the token counts metered here are the same ones LangSmith records.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field

from langchain_core.callbacks import BaseCallbackHandler

from agents import config


class CostLimitExceeded(RuntimeError):
    pass


@dataclass
class CostMeter(BaseCallbackHandler):
    usd_to_eur: float = float(os.environ.get("GMPDI_USD_EUR", "0.92"))
    limit_eur: float = config.COST_LIMIT_EUR
    calls: list[dict] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, model: str, input_tokens: int, output_tokens: int, source: str = "llm"):
        pin, pout = config.price(model, input_tokens)
        usd = (input_tokens * pin + output_tokens * pout) / 1e6
        with self._lock:
            self.calls.append({"model": model, "in": input_tokens, "out": output_tokens, "usd": usd, "source": source})
        if self.eur > self.limit_eur:
            raise CostLimitExceeded(f"investigation cost EUR {self.eur:.3f} exceeds the EUR {self.limit_eur} limit")

    def on_llm_end(self, response, **kwargs):
        for gens in response.generations:
            for g in gens:
                msg = getattr(g, "message", None)
                usage = getattr(msg, "usage_metadata", None) or {}
                meta = getattr(msg, "response_metadata", None) or {}
                model = meta.get("model_name") or meta.get("model") or (response.llm_output or {}).get("model_name")
                if usage and model:
                    self.add(model, int(usage.get("input_tokens", 0)), int(usage.get("output_tokens", 0)))

    @property
    def usd(self) -> float:
        return sum(c["usd"] for c in self.calls)

    @property
    def eur(self) -> float:
        return self.usd * self.usd_to_eur

    def summary(self) -> dict:
        by_model: dict[str, dict] = {}
        for c in self.calls:
            m = by_model.setdefault(c["model"], {"calls": 0, "input_tokens": 0, "output_tokens": 0, "usd": 0.0})
            m["calls"] += 1
            m["input_tokens"] += c["in"]
            m["output_tokens"] += c["out"]
            m["usd"] += c["usd"]
        return {
            "usd": round(self.usd, 4),
            "eur": round(self.eur, 4),
            "usd_to_eur": self.usd_to_eur,
            "by_model": by_model,
        }


def chat(role: str, max_tokens: int = 4096):
    """The chat model for a role ('writer', 'analyst', 'reviewer'), deterministic settings."""
    model = config.MODELS[role]
    if model.startswith("mistral"):
        from langchain_mistralai import ChatMistralAI

        return ChatMistralAI(model=model, temperature=config.TEMPERATURE, max_tokens=max_tokens, max_retries=3)
    from langchain_anthropic import ChatAnthropic

    return ChatAnthropic(model=model, temperature=config.TEMPERATURE, max_tokens=max_tokens, max_retries=3)
