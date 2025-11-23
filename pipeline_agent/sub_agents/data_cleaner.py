from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence

import pandas as pd


@dataclass
class DataCleanerConfig:
    """
    Configuration for DataCleanerAgent.

    You can tune this without changing the agent logic.
    """

    # Columns that should be treated as identifiers when dropping duplicates
    dedupe_on: Sequence[str] = field(default_factory=lambda: ["Email", "LeadID"])

    # Columns that should be normalized to lower case
    lowercase_columns: Sequence[str] = field(
        default_factory=lambda: ["Email", "Country"]
    )

    # Columns that should be stripped of whitespace around text
    strip_whitespace_columns: Sequence[str] = field(
        default_factory=lambda: [
            "FullName",
            "CompanyName",
            "Email",
            "Industry",
            "Country",
            "JobTitle",
            "SeniorityLevel",
            "LeadStatus",
        ]
    )

    # If True, remove rows that have no email and no company and no full name
    drop_completely_blank_leads: bool = True

    # Required canonical columns that should always exist on the dataframe
    # Missing ones will be added as empty columns
    required_columns: Iterable[str] | None = field(
        default_factory=lambda: [
            "LeadID",
            "FullName",
            "CompanyName",
            "Email",
            "Industry",
            "CompanySize",
            "Country",
            "JobTitle",
            "SeniorityLevel",
            "LeadStatus",
        ]
    )


class DataCleanerAgent:
    """
    Cleans and normalizes the leads dataframe coming from RawIngestionAgent.

    Responsibilities:
      * Ensure required columns exist
      * Normalize text fields
      * Deduplicate on Email or LeadID
      * Drop obviously empty rows
    """

    def __init__(self, config: DataCleanerConfig | None = None) -> None:
        self.config = config or DataCleanerConfig()

    def _ensure_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.config.required_columns:
            return df

        for col in self.config.required_columns:
            if col not in df.columns:
                df[col] = pd.NA
        return df

    def _strip_whitespace(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.config.strip_whitespace_columns:
            if col in df.columns and pd.api.types.is_string_dtype(df[col]):
                df[col] = df[col].astype("string").str.strip()
        return df

    def _lowercase_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.config.lowercase_columns:
            if col in df.columns and pd.api.types.is_string_dtype(df[col]):
                df[col] = df[col].astype("string").str.lower()
        return df

    def _drop_completely_blank(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.config.drop_completely_blank_leads:
            return df

        key_cols = [
            col
            for col in ["Email", "FullName", "CompanyName"]
            if col in df.columns
        ]
        if not key_cols:
            return df

        mask_all_null = df[key_cols].isna().all(axis=1)
        mask_all_empty = df[key_cols].astype("string").fillna("").eq("").all(axis=1)
        mask_drop = mask_all_null | mask_all_empty

        return df[~mask_drop].reset_index(drop=True)

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Deduplicate based on configured columns if they exist.

        Order of precedence:
          1. Email
          2. LeadID
        """

        working = df.copy()

        for col in self.config.dedupe_on:
            if col in working.columns:
                working = working.drop_duplicates(subset=[col], keep="first")

        return working.reset_index(drop=True)

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute the cleaning pipeline.

        This method is the main entry point the orchestrator should call.
        """

        if df is None or df.empty:
            return df

        cleaned = df.copy()

        cleaned = self._ensure_required_columns(cleaned)
        cleaned = self._strip_whitespace(cleaned)
        cleaned = self._lowercase_columns(cleaned)
        cleaned = self._drop_completely_blank(cleaned)
        cleaned = self._deduplicate(cleaned)

        return cleaned
