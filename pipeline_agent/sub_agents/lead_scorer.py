
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

import pandas as pd


@dataclass
class LeadScorerConfig:
    base_score: int = 40
    seniority_bonus: Dict[str, int] = None
    country_bonus: Dict[str, int] = None
    industry_bonus: Dict[str, int] = None

    def __post_init__(self) -> None:
        if self.seniority_bonus is None:
            self.seniority_bonus = {
                "c-level": 20,
                "chief": 20,
                "vp": 15,
                "director": 10,
                "head": 10,
                "manager": 5,
            }
        if self.country_bonus is None:
            self.country_bonus = {
                "Singapore": 10,
                "United States": 10,
                "United Kingdom": 8,
                "United Arab Emirates": 8,
            }
        if self.industry_bonus is None:
            self.industry_bonus = {
                "Fintech": 10,
                "Cybersecurity": 10,
                "IT Services": 8,
            }


class LeadScorerAgent:
    """
    Assigns a StrategicScore to each lead using simple weighted rules
    based on industry, country, and seniority.
    """

    def __init__(self, config: LeadScorerConfig | None = None) -> None:
        self.config = config or LeadScorerConfig()

    def _score_row(self, row: pd.Series) -> int:
        score = self.config.base_score

        seniority = str(row.get("SeniorityLevel") or "").lower()
        for key, bonus in self.config.seniority_bonus.items():
            if key in seniority:
                score += bonus
                break

        country = str(row.get("Country") or "")
        score += self.config.country_bonus.get(country, 0)

        industry = str(row.get("Industry") or "")
        score += self.config.industry_bonus.get(industry, 0)

        company_size = str(row.get("CompanySize") or "")
        if "+" in company_size:
            score += 10

        if not row.get("Email"):
            score -= 5

        score = max(0, min(100, score))
        return score

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df["StrategicScore"] = df.apply(self._score_row, axis=1)

        if "PriorityScore" not in df.columns:
            df["PriorityScore"] = df["StrategicScore"]
        else:
            df["PriorityScore"] = df["PriorityScore"].fillna(df["StrategicScore"])

        return df
