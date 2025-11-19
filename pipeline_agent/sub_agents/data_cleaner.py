from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass
class DataCleanerConfig:
    min_company_name_length: int = 2
    drop_rows_with_no_company: bool = True
    drop_duplicate_lead_ids: bool = True


class DataCleanerAgent:
    """
    Cleans and normalizes the structured leads dataframe.

    Focus areas:
      - whitespace and casing normalization
      - light standardization of country and industry
      - duplicate handling
    """

    def __init__(self, config: DataCleanerConfig | None = None) -> None:
        self.config = config or DataCleanerConfig()

        self.country_map = {
            "usa": "United States",
            "us": "United States",
            "u.s.": "United States",
            "u.s.a.": "United States",
            "uk": "United Kingdom",
            "u.k.": "United Kingdom",
            "uae": "United Arab Emirates",
        }

        self.industry_map = {
            "fin tech": "Fintech",
            "fintech": "Fintech",
            "financial technology": "Fintech",
            "cyber security": "Cybersecurity",
            "cyber-security": "Cybersecurity",
            "it services": "IT Services",
            "information technology": "IT Services",
        }

    def _normalize_string_column(self, series: pd.Series, mode: str) -> pd.Series:
        series = series.fillna("").astype(str).str.strip()

        if mode == "lower":
            series = series.str.lower()
        elif mode == "title":
            series = series.str.title()

        # Replace empty strings with None
        series = series.replace("", pd.NA)
        return series

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Basic trimming and casing
        if "FullName" in df.columns:
            df["FullName"] = self._normalize_string_column(df["FullName"], "title")
        if "CompanyName" in df.columns:
            df["CompanyName"] = self._normalize_string_column(df["CompanyName"], "title")
        if "Email" in df.columns:
            df["Email"] = self._normalize_string_column(df["Email"], "lower")
        if "Industry" in df.columns:
            df["Industry"] = self._normalize_string_column(df["Industry"], "lower")
        if "Country" in df.columns:
            df["Country"] = self._normalize_string_column(df["Country"], "title")
        if "JobTitle" in df.columns:
            df["JobTitle"] = self._normalize_string_column(df["JobTitle"], "title")

        # Standardize industry labels
        if "Industry" in df.columns:
            df["Industry"] = df["Industry"].replace(self.industry_map)
            df["Industry"] = df["Industry"].str.title()

        # Standardize country labels
        if "Country" in df.columns:
            lowered = df["Country"].str.lower()
            df["Country"] = lowered.replace(self.country_map)
            df["Country"] = df["Country"].str.title()

        # Drop rows with no meaningful company name if configured
        if self.config.drop_rows_with_no_company and "CompanyName" in df.columns:
            mask_valid_company = df["CompanyName"].notna() & (
                df["CompanyName"].str.len() >= self.config.min_company_name_length
            )
            df = df[mask_valid_company]

        # Drop fully empty rows
        df = df.dropna(how="all")

        # Drop duplicate LeadID if present
        if self.config.drop_duplicate_lead_ids and "LeadID" in df.columns:
            df = df.drop_duplicates(subset=["LeadID"])

        df = df.reset_index(drop=True)
        return df
