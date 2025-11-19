
from pathlib import Path
from typing import Dict, List

import pandas as pd


class RawIngestionAgent:
    """
    Converts raw user input into a standard leads dataframe.

    Handles:
      - CSV files with arbitrary or messy column names
      - TXT files with simple line based lead entries

    The goal is to always return a dataframe that matches the
    canonical schema expected by downstream agents.
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

    # Keys are normalized (lowercase, underscores)
    COLUMN_ALIASES: Dict[str, str] = {
        "lead_id": "LeadID",
        "id": "LeadID",
        "fullname": "FullName",
        "full_name": "FullName",
        "name": "FullName",
        "company": "CompanyName",
        "company_name": "CompanyName",
        "biz": "CompanyName",
        "business_name": "CompanyName",
        "mail": "Email",
        "email_address": "Email",
        "email": "Email",
        "industry_sector": "Industry",
        "sector": "Industry",
        "country_code": "Country",
        "location": "Country",
        "job_title": "JobTitle",
        "role": "JobTitle",
        "seniority": "SeniorityLevel",
        "status": "LeadStatus",
        "lead_status": "LeadStatus",
        "priority": "PriorityScore",
        "priority_score": "PriorityScore",
    }

    def _normalize_and_map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalizes column names and maps aliases to the canonical schema.
        """
        # Normalize original column names
        normalized = {c: c.strip().lower().replace(" ", "_") for c in df.columns}
        df = df.rename(columns=normalized)

        # Map aliases to standard names
        rename_map: Dict[str, str] = {}
        for col in df.columns:
            if col in self.COLUMN_ALIASES:
                rename_map[col] = self.COLUMN_ALIASES[col]
        df = df.rename(columns=rename_map)

        return df

    def from_csv(self, path: Path) -> pd.DataFrame:
        """
        Ingests a CSV file and returns a dataframe that matches
        the STANDARD_COLUMNS schema.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Input CSV not found at {path}")

        # Let pandas guess delimiter and encoding
        try:
            df = pd.read_csv(path)
        except UnicodeDecodeError:
            # Fallback encoding if needed
            df = pd.read_csv(path, encoding="latin-1")

        # Normalize and map columns
        df = self._normalize_and_map_columns(df)

        # Add missing required columns as empty
        for col in self.STANDARD_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Ensure LeadID exists
        if df["LeadID"].isna().all():
            df["LeadID"] = range(1, len(df) + 1)

        # Coerce PriorityScore to numeric if possible
        if "PriorityScore" in df.columns:
            df["PriorityScore"] = pd.to_numeric(
                df["PriorityScore"], errors="coerce"
            )

        # Reorder columns to canonical schema
        df = df[self.STANDARD_COLUMNS]

        return df

    def from_text(self, text: str) -> pd.DataFrame:
        """
        Converts simple line based text into a leads dataframe.

        Format:
            Each non empty line is treated as a lead.
            If the line contains commas, the pattern is:
                FullName, CompanyName, Country, JobTitle

        Missing fields are filled as None.
        PriorityScore defaults to 5 and LeadStatus defaults to "New".
        """
        rows: List[Dict[str, object]] = []

        for idx, line in enumerate(text.splitlines(), start=1):
            raw = line.strip()
            if not raw:
                continue

            full_name = None
            company_name = None
            country = None
            job_title = None

            parts = [p.strip() for p in raw.split(",") if p.strip()]
            if len(parts) >= 1:
                full_name = parts[0]
            if len(parts) >= 2:
                company_name = parts[1]
            if len(parts) >= 3:
                country = parts[2]
            if len(parts) >= 4:
                job_title = parts[3]

            rows.append(
                {
                    "LeadID": idx,
                    "FullName": full_name,
                    "CompanyName": company_name,
                    "Email": None,
                    "Industry": None,
                    "CompanySize": None,
                    "Country": country,
                    "JobTitle": job_title,
                    "SeniorityLevel": None,
                    "LeadStatus": "New",
                    "PriorityScore": 5,
                }
            )

        if not rows:
            return pd.DataFrame(columns=self.STANDARD_COLUMNS)

        df = pd.DataFrame(rows)

        for col in self.STANDARD_COLUMNS:
            if col not in df.columns:
                df[col] = None

        df = df[self.STANDARD_COLUMNS]
        return df
