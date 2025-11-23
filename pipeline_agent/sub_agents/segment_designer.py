from __future__ import annotations

from typing import Any, Tuple, List, Dict

import pandas as pd


# ICP industries and hub countries tuned to your target markets
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


class SegmentDesignerAgent:
    """
    Assigns advanced GTM-oriented segments to each lead and builds a rich
    per-segment summary for analysis and portfolio reporting.

    Segment labels (Sales & GTM style):
      - A1 Strategic ICP          -> Elite ICP, high fit, high score
      - A2 Standard ICP           -> Core ICP, good fit
      - B1 Contactable Leads      -> Non-ICP but reachable with valid email
      - B2 AI-Potential Leads     -> Non-ICP but promising via AI / inferred fit
      - C0 Disqualified           -> Out of scope or poor quality

    Summary output columns (lead_segments.csv):
      - Segment
      - LeadCount
      - AvgStrategicScore
      - ICP_Percentage
      - ValidEmail_Percentage
      - AvgCompanySizeTier
      - TopIndustries
      - TopJobFunctions
      - RegionMix
      - RiskFlagsDetected
      - RecommendedChannel
      - StrategicNotes

    Inputs.
      DataFrame produced by.
        - RawIngestionAgent
        - DataCleanerAgent
        - AIEnrichmentAgent (optional)
        - LeadScorerAgent
        - OutputPolisherAgent

    Required columns.
      - StrategicScore

    Optional columns (used when present).
      - Industry
      - Country
      - Region
      - JobFunction
      - CompanySizeTier
      - ICPFitLabel
      - RiskFlag
      - Email
      - SuggestedPrimaryChannel
    """

    def run(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if df.empty:
            empty = pd.DataFrame(
                columns=[
                    "Segment",
                    "LeadCount",
                    "AvgStrategicScore",
                    "ICP_Percentage",
                    "ValidEmail_Percentage",
                    "AvgCompanySizeTier",
                    "TopIndustries",
                    "TopJobFunctions",
                    "RegionMix",
                    "RiskFlagsDetected",
                    "RecommendedChannel",
                    "StrategicNotes",
                ]
            )
            return df, empty

        df = df.copy()

        if "StrategicScore" not in df.columns:
            raise ValueError(
                "SegmentDesignerAgent requires 'StrategicScore' column. "
                "Make sure LeadScorerAgent has been run before segmentation."
            )

        # Assign segment label per lead
        df["Segment"] = df.apply(self._assign_segment, axis=1)

        # Build per-segment summary
        segments_df = self._build_segment_summary(df)

        return df, segments_df

    # ------------------------------------------------------------------
    # Core segmentation logic
    # ------------------------------------------------------------------

    def _assign_segment(self, row: pd.Series) -> str:
        score = self._safe_float(row.get("StrategicScore"))
        industry = self._safe_str(row.get("Industry"))
        country = self._safe_str(row.get("Country"))
        region = self._safe_str(row.get("Region"))
        job_fn = self._safe_str(row.get("JobFunction"))
        icp_label = self._safe_str(row.get("ICPFitLabel"))
        risk_flag = self._safe_str(row.get("RiskFlag"))
        email_val = row.get("Email")

        # 1. Hard disqualification
        if self._is_disqualified(email_val, risk_flag, score):
            return "C0 Disqualified"

        # Derived feature flags
        is_icp_industry = self._is_icp_industry(industry)
        is_hub_market = self._is_hub_market(country, region)
        is_icp_function = self._is_icp_function(job_fn)
        is_email_valid = not self._email_is_bad(email_val)
        is_high_fit_icp_label = icp_label in {"high", "elite"}
        is_mid_fit_icp_label = icp_label in {"medium", "standard"}

        # If score is missing, treat as very low
        s = score if score is not None else 0.0

        # 2. A1 Strategic ICP  (Elite fit, high score)
        if (
            s >= 7
            and is_icp_industry
            and is_icp_function
            and is_email_valid
            and (is_hub_market or is_high_fit_icp_label)
        ):
            return "A1 Strategic ICP"

        # 3. A2 Standard ICP  (Core ICP, good fit)
        if (
            s >= 4
            and is_icp_industry
            and is_email_valid
            and (is_hub_market or is_icp_function or is_mid_fit_icp_label)
        ):
            return "A2 Standard ICP"

        # 4. B1 Contactable Leads  (Non-ICP but reachable)
        if is_email_valid and (not is_icp_industry) and s >= 3:
            return "B1 Contactable Leads"

        # 5. B2 AI-Potential Leads  (Non-ICP but promising via AI)
        # Use ICPFitLabel as proxy for AI-driven potential, even if email/industry weaker
        if (is_high_fit_icp_label or is_mid_fit_icp_label) and s >= 2:
            return "B2 AI-Potential Leads"

        # Fallback.
        # If we reach here with valid email and modest score, treat as B1.
        if is_email_valid and s >= 2:
            return "B1 Contactable Leads"

        # Otherwise, put into B2 (potential) if AI suggests something,
        # else disqualified.
        if is_high_fit_icp_label or is_mid_fit_icp_label:
            return "B2 AI-Potential Leads"

        return "C0 Disqualified"

    # ------------------------------------------------------------------
    # Summary builder
    # ------------------------------------------------------------------

    def _build_segment_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build an expanded analyst-friendly summary per segment.
        """
        grouped = df.groupby("Segment")

        rows: List[Dict[str, Any]] = []

        for segment, g in grouped:
            if g.empty:
                continue

            # Lead count
            lead_count = (
                g["LeadID"].nunique()
                if "LeadID" in g.columns
                else len(g.index)
            )

            # AvgScore
            avg_score = g["StrategicScore"].mean()

            # ICP%
            icp_mask = g.get("ICPFitLabel").astype(str).str.lower().isin(
                ["high", "elite", "medium", "standard"]
            ) if "ICPFitLabel" in g.columns else pd.Series(False, index=g.index)
            icp_pct = round(100.0 * icp_mask.sum() / lead_count, 1) if lead_count else 0.0

            # Valid email %
            email_valid_mask = ~g["Email"].apply(self._email_is_bad) if "Email" in g.columns else pd.Series(False, index=g.index)
            email_pct = round(100.0 * email_valid_mask.sum() / lead_count, 1) if lead_count else 0.0

            # AvgCompanySizeTier -> most common tier
            avg_company_size_tier = ""
            if "CompanySizeTier" in g.columns:
                avg_company_size_tier = self._mode_or_blank(g["CompanySizeTier"])

            # Top industries / job functions / regions
            top_industries = ""
            if "Industry" in g.columns:
                top_industries = self._top_categories(g["Industry"])

            top_job_fns = ""
            if "JobFunction" in g.columns:
                top_job_fns = self._top_categories(g["JobFunction"])

            region_mix = ""
            if "Region" in g.columns:
                region_mix = self._top_categories(g["Region"])

            # Risk flags.
            risk_flags_detected = ""
            if "RiskFlag" in g.columns:
                rf_vals = (
                    g["RiskFlag"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .replace("", pd.NA)
                    .dropna()
                )
                unique_rf = sorted(set(rf_vals))
                risk_flags_detected = ", ".join(unique_rf[:3])

            # Recommended channel
            recommended_channel = ""
            if "SuggestedPrimaryChannel" in g.columns:
                recommended_channel = self._mode_or_blank(
                    g["SuggestedPrimaryChannel"]
                )

            # Strategic notes
            strategic_notes = self._build_segment_notes(
                segment=segment,
                lead_count=lead_count,
                avg_score=avg_score,
                icp_pct=icp_pct,
                email_pct=email_pct,
                top_industries=top_industries,
                top_job_fns=top_job_fns,
                region_mix=region_mix,
            )

            rows.append(
                {
                    "Segment": segment,
                    "LeadCount": int(lead_count),
                    "AvgStrategicScore": round(float(avg_score), 2)
                    if pd.notna(avg_score)
                    else 0.0,
                    "ICP_Percentage": icp_pct,
                    "ValidEmail_Percentage": email_pct,
                    "AvgCompanySizeTier": avg_company_size_tier,
                    "TopIndustries": top_industries,
                    "TopJobFunctions": top_job_fns,
                    "RegionMix": region_mix,
                    "RiskFlagsDetected": risk_flags_detected,
                    "RecommendedChannel": recommended_channel,
                    "StrategicNotes": strategic_notes,
                }
            )

        segments_df = pd.DataFrame(rows)

        # Sort by hierarchy of segments and score
        segment_order = [
            "A1 Strategic ICP",
            "A2 Standard ICP",
            "B1 Contactable Leads",
            "B2 AI-Potential Leads",
            "C0 Disqualified",
        ]
        segments_df["__segment_rank"] = segments_df["Segment"].apply(
            lambda s: segment_order.index(s) if s in segment_order else len(segment_order)
        )
        segments_df = segments_df.sort_values(
            by=["__segment_rank", "AvgStrategicScore"],
            ascending=[True, False],
        ).drop(columns="__segment_rank")

        return segments_df

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    def _safe_str(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return ""
        except TypeError:
            pass
        return str(value).strip().lower()

    def _safe_float(self, value: Any) -> float | None:
        if value is None:
            return None
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return None
        except TypeError:
            pass
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _email_is_bad(self, email_val: Any) -> bool:
        if email_val is None:
            return True
        try:
            if pd.isna(email_val):  # type: ignore[arg-type]
                return True
        except TypeError:
            pass

        email = str(email_val).strip()
        if not email or "@" not in email or " " in email:
            return True
        return False

    def _is_disqualified(self, email_val: Any, risk_flag: str, score: Any) -> bool:
        # Explicit risk flags
        rf = self._safe_str(risk_flag)
        if rf and ("invalid" in rf or "disqualify" in rf or "blacklist" in rf):
            return True

        # Bad email + very low score or missing score
        if self._email_is_bad(email_val):
            s = self._safe_float(score)
            if s is None or s <= 1:
                return True

        return False

    def _is_icp_industry(self, industry: str) -> bool:
        return industry in ICP_INDUSTRIES

    def _is_hub_market(self, country: str, region: str) -> bool:
        c = country or ""
        r = region or ""
        return c in HUB_COUNTRIES or r in {"apac", "emea"}

    def _is_icp_function(self, job_fn: str) -> bool:
        if not job_fn:
            return False
        if any(k in job_fn for k in ["security", "ciso", "infosec", "soc"]):
            return True
        if any(k in job_fn for k in ["cto", "technology", "engineering", "devops"]):
            return True
        if any(k in job_fn for k in ["finance", "cfo", "treasury", "fp&a"]):
            return True
        if any(k in job_fn for k in ["operations", "ops", "service delivery"]):
            return True
        if any(k in job_fn for k in ["founder", "ceo", "vp", "vice president", "chief", "director"]):
            return True
        return False

    def _mode_or_blank(self, series: pd.Series) -> str:
        s = (
            series.dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
        )
        if s.empty:
            return ""
        return s.value_counts().idxmax()

    def _top_categories(self, series: pd.Series, top_n: int = 3) -> str:
        s = (
            series.dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
        )
        if s.empty:
            return ""
        counts = s.value_counts().head(top_n)
        return ", ".join(counts.index.tolist())

    def _build_segment_notes(
        self,
        segment: str,
        lead_count: int,
        avg_score: float,
        icp_pct: float,
        email_pct: float,
        top_industries: str,
        top_job_fns: str,
        region_mix: str,
    ) -> str:
        """
        Compact strategic note for portfolio / stakeholder reporting.
        """
        base = f"{segment}: {lead_count} leads | Avg score {avg_score:.1f} | ICP {icp_pct:.1f}% | Valid email {email_pct:.1f}%."

        details_parts: List[str] = []

        if top_industries:
            details_parts.append(f"Top industries. {top_industries}.")
        if top_job_fns:
            details_parts.append(f"Key functions. {top_job_fns}.")
        if region_mix:
            details_parts.append(f"Region mix. {region_mix}.")

        details = " ".join(details_parts)

        return (base + " " + details).strip()
