from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd


@dataclass
class RawIngestionConfig:
    """
    Configuration for RawIngestionAgent.
    Extend later if you want to support more file types or options.
    """

    encoding: str = "utf-8"
    csv_sep: str = ","


class RawIngestionAgent:
    """
    Converts raw user input into a standard leads dataframe.

    Supported.
        * CSV files with arbitrary column names
        * (Optional) TXT files with very simple line based format, basic fallback

    Output columns  STANDARD_COLUMNS
    """

    STANDARD_COLUMNS: List[str] = [
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
        "PriorityScore",
    ]

    # Aliases so messy headers still map to our standard schema
    COLUMN_ALIASES: Dict[str, str] = {
        "fullname": "FullName",
        "full_name": "FullName",
        "name": "FullName",

        "company": "CompanyName",
        "company_name": "CompanyName",
        "business": "CompanyName",
        "biz": "CompanyName",

        "mail": "Email",
        "email_address": "Email",

        "industry_sector": "Industry",
        "sector": "Industry",

        "company_size": "CompanySize",
        "employees": "CompanySize",
        "employee_count": "CompanySize",

        "country_code": "Country",
        "location": "Country",

        "job_title": "JobTitle",
        "title": "JobTitle",

        "seniority": "SeniorityLevel",
        "seniority_level": "SeniorityLevel",

        "status": "LeadStatus",
        "lead_status": "LeadStatus",

        "priority": "PriorityScore",
        "score": "PriorityScore",
    }

    def __init__(self, config: RawIngestionConfig | None = None) -> None:
        self.config = config or RawIngestionConfig()

    # ---------- internal helpers ----------

    def _load_csv(self, path: Path) -> pd.DataFrame:
        return pd.read_csv(path, encoding=self.config.encoding, sep=self.config.csv_sep)

    def _load_txt_simple(self, path: Path) -> pd.DataFrame:
        """
        Very simple fallback for TXT input.
        Each non empty line is treated as a lead name.
        You can extend this later to parse more complex formats.
        """
        leads = []
        with path.open("r", encoding=self.config.encoding) as f:
            for idx, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                leads.append(
                    {
                        "LeadID": idx,
                        "FullName": line,
                    }
                )
        df = pd.DataFrame(leads)
        return df

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map messy headers to STANDARD_COLUMNS using COLUMN_ALIASES.
        Create any missing standard columns as empty.
        """

        if df is None or df.empty:
            # Ensure we at least have the right columns, even if empty
            return pd.DataFrame(columns=self.STANDARD_COLUMNS)

        renamed = {}
        for col in df.columns:
            key = str(col).strip().lower()
            target = self.COLUMN_ALIASES.get(key, None)
            if target:
                renamed[col] = target

        df = df.rename(columns=renamed)

        # Ensure all standard columns exist
        for col in self.STANDARD_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA

        # Keep only our standard schema and in correct order
        df = df[self.STANDARD_COLUMNS]

        return df

    # ---------- public entry point ----------

    def run(self, input_path: Path) -> pd.DataFrame:
        """
        Main entrypoint used by the orchestrator.

        Detects file type from extension, loads it, and converts to the standard schema.
        """

        suffix = input_path.suffix.lower()

        if suffix in [".csv"]:
            raw_df = self._load_csv(input_path)
        elif suffix in [".txt"]:
            raw_df = self._load_txt_simple(input_path)
        else:
            raise ValueError(f"Unsupported input file type. {suffix}")

        df_standard = self._standardize_columns(raw_df)
        return df_standard
