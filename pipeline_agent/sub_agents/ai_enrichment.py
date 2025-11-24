from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import json
import logging

import pandas as pd

logger = logging.getLogger(__name__)


GEMINI_PROMPT_TEMPLATE = """
You are part of the Pipeline Strategist Agent, a multi-agent GTM system.

Given the following lead record (in JSON), infer structured GTM insights.

Return a JSON object with the following keys:
- pain_points: list of top 2–4 likely business pains
- maturity_signals: list of buying-readiness or growth indicators
- value_drivers: list of angles that are likely to resonate
- risk_factors: list of potential blockers or disqualifiers
- narrative: 1–2 sentence summary that an SDR could reuse directly

Lead record (JSON):
{lead_json}
"""


@dataclass
class AIEnrichmentConfig:
    """
    Configuration for the AIEnrichmentAgent.

    simulate:
        If True, use a deterministic, hard-coded "fake Gemini" response.
        This keeps the pipeline fully runnable in environments where the
        real Gemini API is not available (e.g. Kaggle, CI, offline dev).

        For a production deployment you can set simulate=False and implement
        a real call in `_call_gemini_api`.
    """

    simulate: bool = True
    model: str = "gemini-1.5-flash"  # Placeholder, not used in this repo.


class AIEnrichmentAgent:
    """
    Optional AI enrichment layer that decorates each lead with AI_Notes.

    The Agent is designed to be Gemini-ready:

    - It constructs a structured prompt (see GEMINI_PROMPT_TEMPLATE).
    - It expects a JSON-like response with pain points, signals, etc.
    - In this capstone / open-source version, it defaults to a simulated
      response so the pipeline is stable without external API keys.

    The resulting insights are flattened into a human-readable `AI_Notes`
    string column that downstream agents can consume.
    """

    def __init__(self, config: AIEnrichmentConfig | None = None) -> None:
        self.config = config or AIEnrichmentConfig()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            logger.info("AIEnrichmentAgent: received empty dataframe, skipping.")
            return df

        df = df.copy()
        notes: List[str] = []

        for _, row in df.iterrows():
            lead_dict = row.to_dict()

            if self.config.simulate:
                enrichment = self._simulate_gemini_output(lead_dict)
            else:
                enrichment = self._call_gemini_api(lead_dict)

            notes.append(self._format_enrichment(enrichment))

        df["AI_Notes"] = notes
        logger.info("AIEnrichmentAgent: added AI_Notes for %d leads.", len(df))
        return df

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _call_gemini_api(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """
        Placeholder for a real Gemini API call.

        For the capstone and public repo we do NOT call external services.
        Instead, we log and fall back to a deterministic simulated response.

        A real implementation could look roughly like:

            import os
            import google.generativeai as genai

            genai.configure(api_key=os.environ["GEMINI_API_KEY"])
            model = genai.GenerativeModel(self.config.model)

            prompt = GEMINI_PROMPT_TEMPLATE.format(
                lead_json=json.dumps(lead, indent=2)
            )
            resp = model.generate_content(prompt)
            # then parse resp.text as JSON, etc.

        """
        logger.info(
            "AIEnrichmentAgent: Gemini API call disabled in this repo, "
            "falling back to simulated output."
        )
        return self._simulate_gemini_output(lead)

    def _simulate_gemini_output(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deterministic "fake Gemini" output used for the capstone and tests.

        This keeps the pipeline fully runnable without depending on
        external APIs or billing, while still demonstrating how an LLM
        would structure its reasoning.
        """
        company = (
            str(lead.get("CompanyName"))
            or str(lead.get("Company"))
            or "the company"
        )
        industry = str(lead.get("Industry") or "their industry")
        role = str(lead.get("JobTitle") or "their team")

        pain_points = [
            "manual reporting and fragmented data",
            "slow outbound cycles",
        ]
        maturity_signals = [
            "signs of growth or recent hiring",
        ]
        value_drivers = [
            "workflow automation",
            "pipeline visibility",
            "AI-assisted prioritization",
        ]
        risk_factors = [
            "unclear decision-maker",
        ]
        narrative = (
            f"{company} operates in {industry} and {role} likely deals with "
            f"manual workflows and slow pipeline visibility. A message that "
            f"focuses on automation and clearer GTM execution will resonate."
        )

        return {
            "pain_points": pain_points,
            "maturity_signals": maturity_signals,
            "value_drivers": value_drivers,
            "risk_factors": risk_factors,
            "narrative": narrative,
        }

    @staticmethod
    def _format_enrichment(enrichment: Dict[str, Any]) -> str:
        """
        Turn the structured enrichment dict into a compact string suitable
        for a single `AI_Notes` column that downstream agents and humans
        can both read.
        """
        if not enrichment:
            return ""

        sections: List[str] = []

        narrative = enrichment.get("narrative")
        if narrative:
            sections.append(str(narrative))

        def _join(label: str, key: str) -> None:
            values = enrichment.get(key) or []
            if values:
                sections.append(f"{label}: " + ", ".join(map(str, values)))

        _join("Pain points", "pain_points")
        _join("Signals", "maturity_signals")
        _join("Value drivers", "value_drivers")
        _join("Risks", "risk_factors")

        return " | ".join(sections)
