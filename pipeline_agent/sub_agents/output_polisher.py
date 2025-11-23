from __future__ import annotations

from typing import Any, List

import pandas as pd


class OutputPolisherAgent:
    """
    Final presentation layer for cleaned_scored_leads.

    Responsibilities:
      - Map raw columns (id, full name, mail, etc.) into canonical schema
      - Normalize and standardize key fields
      - Create derived columns (Region, CompanySizeTier, JobFunction, DQ flags)
      - Add professional placeholder fields (Phone, LinkedInURL, etc.)
      - Drop raw / legacy columns from the original dataset
      - Reorder columns into a portfolio friendly structure
      - Sort rows by Segment (asc), StrategicScore (desc)
    """

    # Final, polished schema for cleaned_scored_leads.csv
    TARGET_COLUMN_ORDER: List[str] = [
        "LeadID",
        "FullName",
        "Email",
        "Phone",
        "LinkedInURL",
        "CompanyName",
        "Industry",
        "SubIndustry",
        "Country",
        "City",
        "Region",
        "CompanySize",
        "CompanySizeTier",
        "SeniorityLevel",
        "JobTitle",
        "JobFunction",
        "LeadStatus",
        "Segment",
        "StrategicScore",
        "DQ_MissingEmail",
        "DQ_MissingIndustry",
        "DQ_InferredFieldsCount",
        "DQ_ConfidenceLevel",
        "AI_Notes",
    ]

    # Map legacy raw column names into canonical ones
    RAW_TO_CANONICAL = {
        "id": "LeadID",
        "full name": "FullName",
        "biz": "CompanyName",
        "mail": "Email",
        "industry_sector": "Industry",
        "company size": "CompanySize",
        "country_code": "Country",
        "role": "JobTitle",
        "seniority": "SeniorityLevel",
        "status": "LeadStatus",
        "priority score": "StrategicScore",  # fallback if no proper score yet
    }

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main entry point.

        Takes the scored and segmented dataframe and returns a polished
        version ready to be exported as cleaned_scored_leads.csv.
        """
        if df.empty:
            return df

        df = df.copy()

        # ------------------------------------------------------------------
        # 0. Map legacy raw columns into canonical columns where missing
        # ------------------------------------------------------------------
        for raw_col, canon_col in self.RAW_TO_CANONICAL.items():
            if raw_col in df.columns:
                if canon_col not in df.columns:
                    df[canon_col] = pd.NA
                # Only fill canon where it is null or empty
                df[canon_col] = df[canon_col].where(
                    ~df[canon_col].isna(),  # keep existing non NA
                    df[raw_col],
                )

        # ------------------------------------------------------------------
        # 1. Ensure all expected columns exist
        # ------------------------------------------------------------------
        base_required = [
            "LeadID",
            "FullName",
            "Email",
            "CompanyName",
            "Industry",
            "Country",
            "JobTitle",
            "SeniorityLevel",
            "CompanySize",
            "LeadStatus",
            "Segment",
            "StrategicScore",
        ]

        for col in base_required:
            if col not in df.columns:
                df[col] = pd.NA

        # Ensure placeholder or derived fields exist
        for col in [
            "Phone",
            "LinkedInURL",
            "SubIndustry",
            "City",
            "Region",
            "CompanySizeTier",
            "JobFunction",
            "DQ_MissingEmail",
            "DQ_MissingIndustry",
            "DQ_InferredFieldsCount",
            "DQ_ConfidenceLevel",
            "AI_Notes",
        ]:
            if col not in df.columns:
                df[col] = pd.NA

        # ------------------------------------------------------------------
        # 2. Derive / normalize fields
        # ------------------------------------------------------------------
        df["Region"] = df["Country"].apply(self._derive_region)
        df["CompanySizeTier"] = df["CompanySize"].apply(self._derive_company_size_tier)
        df["JobFunction"] = df["JobTitle"].apply(self._derive_job_function)

        # Data quality flags
        df["DQ_MissingEmail"] = df["Email"].apply(self._flag_missing_email)
        df["DQ_MissingIndustry"] = df["Industry"].apply(self._flag_missing_industry)
        df["DQ_InferredFieldsCount"] = self._count_inferred_fields(df)
        df["DQ_ConfidenceLevel"] = df["DQ_InferredFieldsCount"].apply(
            self._derive_confidence_level
        )

        # AI notes summary
        df["AI_Notes"] = df.apply(self._build_ai_notes, axis=1)

        # ------------------------------------------------------------------
        # 3. Drop raw / legacy columns. keep only polished schema
        # ------------------------------------------------------------------
        allowed = set(self.TARGET_COLUMN_ORDER)
        df = df[[c for c in df.columns if c in allowed]]

        # ------------------------------------------------------------------
        # 4. Presentation cleanup. convert to clean strings
        # ------------------------------------------------------------------
        for col in [
            "FullName",
            "Email",
            "CompanyName",
            "Industry",
            "SubIndustry",
            "Country",
            "City",
            "JobTitle",
            "SeniorityLevel",
            "LeadStatus",
            "Segment",
            "Region",
            "CompanySizeTier",
            "JobFunction",
            "AI_Notes",
        ]:
            if col in df.columns:
                df[col] = df[col].apply(self._safe_str)

        # ------------------------------------------------------------------
        # 5. Reorder columns and sort
        # ------------------------------------------------------------------
        df = self._reorder_columns(df)

        if "Segment" in df.columns and "StrategicScore" in df.columns:
            df = df.sort_values(
                by=["Segment", "StrategicScore"],
                ascending=[True, False],
                na_position="last",
            ).reset_index(drop=True)

        return df

    # ======================================================================
    # Helper methods
    # ======================================================================

    def _safe_str(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return ""
        except TypeError:
            pass
        return str(value).strip()

    def _derive_region(self, country: Any) -> str:
        text = self._safe_str(country).lower()
        if not text:
            return ""
        if text in {"singapore"}:
            return "APAC"
        if text in {"united arab emirates", "uae", "dubai", "abu dhabi"}:
            return "EMEA"
        if text in {"united kingdom", "uk", "england", "london"}:
            return "EMEA"
        if text in {"united states", "usa", "us", "new york", "san francisco"}:
            return "AMER"
        if text in {"qatar", "saudi arabia", "saudi", "kuwait", "bahrain"}:
            return "EMEA"
        if text in {"australia", "new zealand", "malaysia", "thailand", "vietnam"}:
            return "APAC"
        return ""

    def _derive_company_size_tier(self, value: Any) -> str:
        text = self._safe_str(value).lower()
        if not text:
            return ""

        if "1000" in text or "1001" in text or "enterprise" in text or "500+" in text:
            return "Enterprise"
        if "201-500" in text or "200-500" in text:
            return "Mid Market"
        if "51-200" in text:
            return "SMB"
        if "1-10" in text or "11-50" in text or "startup" in text:
            return "Startup or Micro"

        for token in ["-", "+"]:
            if token in text:
                left = text.split(token)[0]
                try:
                    n = int(left)
                    if n >= 500:
                        return "Enterprise"
                    if n >= 200:
                        return "Mid Market"
                    if n >= 50:
                        return "SMB"
                    return "Startup or Micro"
                except ValueError:
                    break

        return ""

    def _derive_job_function(self, value: Any) -> str:
        text = self._safe_str(value).lower()
        if not text:
            return ""

        if any(k in text for k in ["ciso", "security", "infosec", "soc"]):
            return "Security"
        if any(k in text for k in ["cto", "it ", "it-", "infrastructure", "devops", "engineering"]):
            return "Technology"
        if any(k in text for k in ["finance", "cfo", "treasury", "fp&a"]):
            return "Finance"
        if any(k in text for k in ["operations", "coo", "ops manager", "service delivery"]):
            return "Operations"
        if any(k in text for k in ["product", "pm ", "product manager", "head of product"]):
            return "Product"
        if any(k in text for k in ["marketing", "growth", "demand gen"]):
            return "Marketing"
        if any(k in text for k in ["sales", "bd", "business development", "account executive"]):
            return "Sales"
        if any(k in text for k in ["founder", "ceo", "vp", "vice president", "chief", "director"]):
            return "Executive"

        return "General"

    def _flag_missing_email(self, value: Any) -> bool:
        text = self._safe_str(value)
        if not text:
            return True
        if "@" not in text or " " in text:
            return True
        return False

    def _flag_missing_industry(self, value: Any) -> bool:
        text = self._safe_str(value)
        return text == ""

    def _count_inferred_fields(self, df: pd.DataFrame) -> pd.Series:
        inferred_cols = ["Region", "CompanySizeTier", "JobFunction"]
        count = pd.Series(0, index=df.index, dtype="int64")
        for col in inferred_cols:
            if col in df.columns:
                non_empty = df[col].apply(self._safe_str) != ""
                count = count + non_empty.astype("int64")
        return count

    def _derive_confidence_level(self, inferred_count: Any) -> str:
        try:
            n = int(inferred_count)
        except (TypeError, ValueError):
            return "Medium"
        if n <= 1:
            return "High"
        if n == 2:
            return "Medium"
        return "Low"

    def _build_ai_notes(self, row: pd.Series) -> str:
        parts: List[str] = []

        icp_label = self._safe_str(row.get("ICPFitLabel"))
        icp_reason = self._safe_str(row.get("ICPFitReason"))
        risk_flag = self._safe_str(row.get("RiskFlag"))
        channel = self._safe_str(row.get("SuggestedPrimaryChannel"))

        if icp_label:
            parts.append(f"ICP fit. {icp_label}")
        if icp_reason:
            parts.append(icp_reason)
        if risk_flag:
            parts.append(f"Risk. {risk_flag}")
        if channel:
            parts.append(f"Channel. {channel}")

        return " | ".join(parts).strip()

    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Ensure all target columns exist
        for col in self.TARGET_COLUMN_ORDER:
            if col not in df.columns:
                df[col] = pd.NA
        return df[self.TARGET_COLUMN_ORDER]
