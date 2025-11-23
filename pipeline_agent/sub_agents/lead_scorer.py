from __future__ import annotations

from typing import Any

import pandas as pd


ICP_INDUSTRIES = {
    "fintech",
    "financial technology",
    "payments",
    "cybersecurity",
    "cyber security",
    "it services",
    "it consulting",
    "saas",
    "cloud",
    "software",
}

HUB_COUNTRIES = {
    "singapore",
    "united arab emirates",
    "uae",
    "dubai",
    "abu dhabi",
    "united kingdom",
    "uk",
    "england",
    "london",
    "qatar",
    "saudi arabia",
    "saudi",
}

SENIOR_TITLES = {
    "ciso",
    "cto",
    "ceo",
    "coo",
    "cfo",
    "chief",
    "founder",
    "vp",
    "vice president",
    "director",
    "head",
}


class LeadScorerAgent:
    """
    Assigns a StrategicScore between 0 and 10 to each lead.

    The score combines:
      - Data quality (email, industry presence)
      - ICP fit (industry, region, function, seniority, company size)
      - Engagement / status
      - AI-enrichment hints (ICPFitLabel, RiskFlag)

    This is designed to work hand in hand with the 5-segment GTM model:
      - A1 Strategic ICP
      - A2 Standard ICP
      - B1 Contactable Leads
      - B2 AI-Potential Leads
      - C0 Disqualified
    """

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        df["StrategicScore"] = df.apply(self._score_row, axis=1)
        return df

    # ------------------------------------------------------------------
    # Core scoring logic
    # ------------------------------------------------------------------

    def _score_row(self, row: pd.Series) -> float:
        """
        Compute a 0–10 StrategicScore for a single lead.
        """

        # ------------------------------------------
        # 0. Quick sanity helpers
        # ------------------------------------------
        email = self._safe_str(row.get("Email"))
        industry = self._safe_str(row.get("Industry"))
        country = self._safe_str(row.get("Country"))
        region = self._safe_str(row.get("Region"))
        job_fn = self._safe_str(row.get("JobFunction"))
        job_title = self._safe_str(row.get("JobTitle"))
        size_tier = self._safe_str(row.get("CompanySizeTier"))
        seniority = self._safe_str(row.get("SeniorityLevel"))
        status = self._safe_str(row.get("LeadStatus"))
        icp_label = self._safe_str(row.get("ICPFitLabel"))
        risk_flag = self._safe_str(row.get("RiskFlag"))

        # ------------------------------------------
        # 1. Baseline from data quality
        # ------------------------------------------
        score = 0.0

        email_valid = self._is_email_valid(email)
        has_industry = bool(industry)
        has_title = bool(job_title)

        if email_valid and has_industry and has_title:
            # Fully usable record
            score += 4.0
        elif (email_valid and has_industry) or (email_valid and has_title):
            # Partially usable record
            score += 3.0
        elif email_valid:
            score += 2.0
        elif has_industry or has_title:
            score += 1.0
        else:
            # Very low quality record
            score += 0.0

        # ------------------------------------------
        # 2. ICP Industry & Region
        # ------------------------------------------
        if industry in ICP_INDUSTRIES:
            score += 2.0

        if country in HUB_COUNTRIES or region in {"apac", "emea"}:
            score += 1.5

        # ------------------------------------------
        # 3. Role / Function / Seniority
        # ------------------------------------------
        if job_fn in {"security", "technology", "finance", "operations", "executive"}:
            score += 1.5

        # Check seniority by keywords in either SeniorityLevel or JobTitle
        senior_text = f"{seniority} {job_title}".lower()
        if any(keyword in senior_text for keyword in SENIOR_TITLES):
            score += 1.5

        # ------------------------------------------
        # 4. Company size (tier)
        # ------------------------------------------
        if size_tier in {"Enterprise"}:
            score += 1.5
        elif size_tier in {"Mid Market", "SMB"}:
            score += 1.0
        elif size_tier in {"Startup or Micro"}:
            score += 0.5

        # ------------------------------------------
        # 5. Engagement / lead status
        # ------------------------------------------
        if status in {"In Progress", "Meeting Scheduled", "Proposal Sent"}:
            score += 1.0
        elif status in {"New", "Contacted"}:
            score += 0.5
        # Closed Lost or similar could be a small penalty if you add those later

        # ------------------------------------------
        # 6. AI-enrichment hints (ICPFitLabel, RiskFlag)
        # ------------------------------------------
        if icp_label in {"elite", "high"}:
            score += 2.0
        elif icp_label in {"medium", "standard"}:
            score += 1.0
        elif icp_label in {"low"}:
            score -= 0.5

        if risk_flag:
            # Any risk flag should slightly reduce enthusiasm, but not auto-kill
            score -= 0.5

        # ------------------------------------------
        # 7. Hard penalties
        # ------------------------------------------
        # Seriously bad email + no industry = not worth much
        if not email_valid and not has_industry:
            score -= 1.0

        # ------------------------------------------
        # 8. Clamp to [0, 10]
        # ------------------------------------------
        if score < 0:
            score = 0.0
        if score > 10:
            score = 10.0

        return float(round(score, 2))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _safe_str(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return ""
        except TypeError:
            pass
        return str(value).strip()

    def _is_email_valid(self, email: str) -> bool:
        if not email:
            return False
        if "@" not in email:
            return False
        if " " in email:
            return False
        # You can add more nuanced rules here if you like
        return True
