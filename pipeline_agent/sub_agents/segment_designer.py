from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import numpy as np
import pandas as pd


@dataclass
class SegmentDesignerConfig:
    high_score_threshold: int = 75
    mid_score_threshold: int = 50
    strategic_countries = {"Singapore", "United States", "United Kingdom", "United Arab Emirates"}
    strategic_industries = {"Fintech", "Cybersecurity", "IT Services"}


class SegmentDesignerAgent:
    """
    Assigns each lead to a commercial segment and returns
    both row level segments and an aggregate view.
    """

    def __init__(self, config: SegmentDesignerConfig | None = None) -> None:
        self.config = config or SegmentDesignerConfig()

    def _parse_company_size(self, value: object) -> int:
        """
        Converts textual company size like "11-50" or "1001-5000" to
        an approximate numeric mid point. Returns 0 if unknown.
        """
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return 0

        text = str(value).strip()
        if "-" in text:
            parts = text.split("-")
            try:
                low = int(parts[0])
                high = int(parts[1])
                return int((low + high) / 2)
            except ValueError:
                return 0

        try:
            return int(text.replace("+", ""))
        except ValueError:
            return 0

    def _assign_segment(self, row: pd.Series) -> str:
        score = row.get("PriorityScore", 0) or 0
        country = row.get("Country") or ""
        industry = row.get("Industry") or ""
        seniority = (row.get("SeniorityLevel") or "").lower()
        company_size_numeric = self._parse_company_size(row.get("CompanySize"))

        is_strategic_geo = country in self.config.strategic_countries
        is_strategic_industry = industry in self.config.strategic_industries
        is_senior = any(
            key in seniority
            for key in ["c-level", "chief", "vp", "director", "founder", "owner", "head"]
        )

        if (
            score >= self.config.high_score_threshold
            and is_strategic_geo
            and is_strategic_industry
            and company_size_numeric >= 50
        ):
            return "Strategic Core"

        if score >= self.config.mid_score_threshold and (is_strategic_geo or is_strategic_industry):
            return "Growth Focus"

        if score >= 30:
            return "Nurture"

        return "Low Priority"

    def run(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = df.copy()

        if "PriorityScore" not in df.columns:
            df["PriorityScore"] = 0

        df["Segment"] = df.apply(self._assign_segment, axis=1)

        # Aggregate stats per segment
        segments_df = df.groupby("Segment").agg(
            LeadCount=("LeadID", "count"),
            AvgPriorityScore=("PriorityScore", "mean"),
        ).reset_index()

        segments_df["AvgPriorityScore"] = segments_df["AvgPriorityScore"].round(1)

        return df, segments_df
