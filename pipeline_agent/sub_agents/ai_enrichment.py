from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd


# Core industries and regions we care most about
CORE_INDUSTRIES = {
    "fintech",
    "cybersecurity",
    "it services",
    "saas",
}

STRATEGIC_REGIONS = {
    "singapore",
    "united arab emirates",
    "united kingdom",
    "united states",
}


class AIEnrichmentAgent:
    """
    Lightweight AI-style enrichment layer.

    This version is fully rule-based so it works offline (GitHub Codespaces,
    Kaggle, local). Later you can swap the internals to call an LLM while
    keeping the same public interface.

    It adds columns like.
      - ICPFitLabel            ('high' / 'medium' / 'low')
      - ICPFitReason           (short explanation string)
      - RiskFlag               (basic risk notes, may be empty)
      - SuggestedPrimaryChannel('email', 'linkedin', 'mixed', etc.)
      - Persona                (short human-readable profile)
    """

    def __init__(self, use_llm: bool = False) -> None:
        # Reserved for future. currently everything is rule-based
        self.use_llm = use_llm

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        enriched_rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            insights = self._enrich_rule_based(row)
            enriched_rows.append(insights)

        insights_df = pd.DataFrame(enriched_rows, index=df.index)

        # Merge, letting enrichment columns overwrite existing ones if present
        for col in insights_df.columns:
            df[col] = insights_df[col]

        return df

    # ------------------------------------------------------------------
    # Core rule-based enrichment
    # ------------------------------------------------------------------

    def _enrich_rule_based(self, row: pd.Series) -> Dict[str, Any]:
        # Pull normalized-ish values
        industry_raw = self._safe_str(row.get("Industry"))
        country_raw = self._safe_str(row.get("Country"))
        seniority_raw = self._safe_str(row.get("SeniorityLevel"))
        company_size_raw = self._safe_str(row.get("CompanySize"))
        priority_score = self._safe_int(row.get("PriorityScore"))

        industry_lc = industry_raw.lower()
        country_lc = country_raw.lower()
        seniority_lc = seniority_raw.lower()

        industry_bucket = self._get_industry_bucket(industry_lc)
        region_bucket = self._get_region_bucket(country_lc)
        company_size_tier = self._get_company_size_tier(company_size_raw)
        seniority_tier = self._get_seniority_tier(seniority_lc)

        # ------------------------------------------------------------------
        # ICP Fit scoring heuristic
        # ------------------------------------------------------------------
        icp_score = 0.0
        reasons: List[str] = []

        # Industry weight
        if industry_bucket == "core":
            icp_score += 3
            reasons.append("core industry (fintech / cyber / IT / SaaS)")
        elif industry_bucket == "adjacent":
            icp_score += 2
            reasons.append("adjacent tech-related industry")
        elif industry_bucket == "other":
            icp_score += 1
            reasons.append("non-core industry")

        # Region weight
        if region_bucket == "hub":
            icp_score += 3
            reasons.append("in strategic hub region")
        else:
            icp_score += 1
            reasons.append("non-hub region")

        # Seniority
        if seniority_tier == "executive":
            icp_score += 3
            reasons.append("executive decision maker")
        elif seniority_tier == "lead":
            icp_score += 2
            reasons.append("team or department lead")
        elif seniority_tier == "ic":
            icp_score += 1
            reasons.append("individual contributor")

        # Company size
        if company_size_tier == "enterprise":
            icp_score += 2
            reasons.append("enterprise size company")
        elif company_size_tier == "mid":
            icp_score += 1.5
            reasons.append("mid market company")
        elif company_size_tier == "smb":
            icp_score += 1
            reasons.append("SMB size company")

        # PriorityScore nudges
        if priority_score is not None:
            if priority_score >= 9:
                icp_score += 2
                reasons.append("internally marked as top priority")
            elif priority_score >= 7:
                icp_score += 1.5
                reasons.append("internally marked as high priority")
            elif priority_score <= 3:
                icp_score -= 1
                reasons.append("internally marked as low priority")

        # Clamp
        if icp_score < 0:
            icp_score = 0.0
        if icp_score > 10:
            icp_score = 10.0

        # Map to label
        if icp_score >= 7.5:
            icp_label = "high"
        elif icp_score >= 4.5:
            icp_label = "medium"
        else:
            icp_label = "low"

        icp_reason = "; ".join(reasons) if reasons else "No strong signals detected"

        # ------------------------------------------------------------------
        # Risk flags
        # ------------------------------------------------------------------
        email_val = row.get("Email")
        email_status = self._classify_email(email_val)

        risk_flag: Optional[str] = None

        if email_status in {"empty", "invalid"}:
            risk_flag = "Missing or invalid email"

        if industry_bucket == "other":
            if risk_flag:
                risk_flag += " + non core industry"
            else:
                risk_flag = "Non core industry"

        if priority_score is not None and priority_score <= 3:
            if risk_flag:
                risk_flag += " + low internal priority"
            else:
                risk_flag = "Low internal priority score"

        # ------------------------------------------------------------------
        # Suggested primary channel
        # ------------------------------------------------------------------
        suggested_channel = self._suggest_channel(
            seniority_tier=seniority_tier,
            region_bucket=region_bucket,
            email_status=email_status,
        )

        # ------------------------------------------------------------------
        # Persona text
        # ------------------------------------------------------------------
        title = self._safe_str(row.get("JobTitle"))
        industry_for_persona = industry_raw
        country_for_persona = country_raw

        persona_parts: List[str] = []
        if title:
            persona_parts.append(title)
        if industry_for_persona:
            persona_parts.append(industry_for_persona)
        if country_for_persona:
            persona_parts.append(country_for_persona)

        persona = " in ".join(persona_parts)
        if not persona:
            persona = "Prospect in target market"

        return {
            "ICPFitLabel": icp_label,
            "ICPFitReason": icp_reason,
            "RiskFlag": risk_flag,
            "SuggestedPrimaryChannel": suggested_channel,
            "Persona": persona,
        }

    # ------------------------------------------------------------------
    # Helper methods. NA-safe
    # ------------------------------------------------------------------

    def _safe_str(self, value: Any) -> str:
        """Convert value to safe stripped string, even if it is NA."""
        if value is None:
            return ""
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return ""
        except TypeError:
            # Non-scalar. ignore NA check
            pass
        return str(value).strip()

    def _safe_int(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return None
        except TypeError:
            pass
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Bucketing helpers
    # ------------------------------------------------------------------

    def _get_industry_bucket(self, value: Any) -> str:
        if value is None:
            return "unknown"
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return "unknown"
        except TypeError:
            pass

        text = str(value).strip().lower()
        if not text:
            return "unknown"

        if text in CORE_INDUSTRIES:
            return "core"
        if any(k in text for k in ["cloud", "data", "software", "bpo", "outsourcing"]):
            return "adjacent"
        return "other"

    def _get_region_bucket(self, value: Any) -> str:
        if value is None:
            return "unknown"
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return "unknown"
        except TypeError:
            pass

        text = str(value).strip().lower()
        if not text:
            return "unknown"

        if text in STRATEGIC_REGIONS:
            return "hub"
        return "other"

    def _get_company_size_tier(self, value: Any) -> str:
        """
        Very simple bucketing based on size-like text.
        """
        if value is None:
            return "unknown"
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return "unknown"
        except TypeError:
            pass

        text = str(value).strip().lower()
        if not text:
            return "unknown"

        # textual hints
        if "enterprise" in text or "1001" in text or "1000+" in text or "500+" in text:
            return "enterprise"
        if "51-200" in text or "201-500" in text:
            return "mid"
        if "1-10" in text or "11-50" in text or "small" in text:
            return "smb"

        # numeric-ish ranges like "51-200", "200-500", "50-1000+"
        for token in ["-", "+"]:
            if token in text:
                left = text.split(token)[0]
                try:
                    n = int(left)
                    if n >= 500:
                        return "enterprise"
                    if n >= 51:
                        return "mid"
                    return "smb"
                except ValueError:
                    pass

        return "unknown"

    def _get_seniority_tier(self, value: Any) -> str:
        if value is None:
            return "unknown"
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return "unknown"
        except TypeError:
            pass

        text = str(value).strip().lower()
        if not text:
            return "unknown"

        if any(k in text for k in ["vp", "cxo", "cto", "ciso", "chief", "founder", "coo", "ceo"]):
            return "executive"
        if any(k in text for k in ["head", "director", "lead", "manager"]):
            return "lead"
        return "ic"

    # ------------------------------------------------------------------
    # Email & channel helpers
    # ------------------------------------------------------------------

    def _classify_email(self, value: Any) -> str:
        """
        Return one of.
          - 'valid'
          - 'invalid'
          - 'empty'
        """
        if value is None:
            return "empty"
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return "empty"
        except TypeError:
            pass

        text = str(value).strip()
        if not text:
            return "empty"
        if "@" not in text or " " in text:
            return "invalid"
        return "valid"

    def _suggest_channel(
        self,
        seniority_tier: str,
        region_bucket: str,
        email_status: str,
    ) -> str:
        """
        Very simple channel heuristic.
        """
        # If email is bad. go LinkedIn-first
        if email_status in {"empty", "invalid"}:
            if region_bucket == "hub":
                return "linkedin-first"
            return "linkedin-only"

        # Email is valid
        if seniority_tier == "executive":
            if region_bucket == "hub":
                return "email + linkedin"
            return "email-first"
        if seniority_tier == "lead":
            return "email + linkedin"
        return "email-first"
