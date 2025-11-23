from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd


class QualityCheckerAgent:
    """
    Runs quality checks on the final pipeline outputs and generates
    an analyst style markdown report.

    Inputs:
      - df: cleaned, scored, segmented leads dataframe
      - segments_df: segment summary from SegmentDesignerAgent
      - playbook_md: markdown string from PlaybookWriterAgent

    Output:
      - A markdown string describing data health, issues and recommendations.
    """

    def run(self, df: pd.DataFrame, segments_df: pd.DataFrame, playbook_md: str) -> str:
        sections: List[str] = []

        sections.append("# Quality Report . Pipeline Strategist Agent\n\n")

        if df.empty:
            sections.append("## Summary\n\n")
            sections.append("- Status: ❌ Blocked\n")
            sections.append("- Reason: Lead dataframe is empty. nothing to analyze.\n\n")
            return "".join(sections)

        issues: List[Dict[str, str]] = []

        # Build data for sections and collect issues as we go
        summary_block = self._summary_block(df, segments_df)
        completeness_block, completeness_issues = self._completeness_block(df)
        issues.extend(completeness_issues)

        email_block, email_issues = self._email_quality_block(df)
        issues.extend(email_issues)

        score_segment_block = self._score_and_segment_block(df, segments_df)

        playbook_block, playbook_issues = self._playbook_block(playbook_md)
        issues.extend(playbook_issues)

        overall_block = self._overall_assessment_block(df, segments_df, playbook_md, issues)

        # Summary section at the top with severity rollup
        sections.append(self._summary_section(df, issues))
        sections.append(summary_block)
        sections.append(completeness_block)
        sections.append(email_block)
        sections.append(score_segment_block)
        sections.append(playbook_block)
        sections.append(overall_block)

        return "".join(sections)

    # ---------- 0. Summary header with severities ----------

    def _summary_section(self, df: pd.DataFrame, issues: List[Dict[str, str]]) -> str:
        lines: List[str] = []
        lines.append("## 1. Status summary\n\n")

        total_leads = len(df)

        if not issues:
            status = "✅ Healthy"
            status_note = "All key checks passed. dataset is ready for analysis and outreach."
        else:
            severities = {i["severity"] for i in issues}
            if "critical" in severities or "high" in severities:
                status = "⚠️ Issues"
                status_note = "One or more high impact issues detected. address before production usage."
            else:
                status = "ℹ️ Minor issues"
                status_note = "Only medium or low impact issues detected. can be improved but not blocking."

        lines.append(f"- Status: {status}\n")
        lines.append(f"- Leads processed: **{total_leads}**\n")
        lines.append(f"- Overall assessment: {status_note}\n\n")

        if issues:
            lines.append("**Issue summary:**\n\n")
            # Group by severity for a quick view
            for sev in ["critical", "high", "medium", "low"]:
                sev_issues = [i for i in issues if i["severity"] == sev]
                if not sev_issues:
                    continue
                label = sev.capitalize()
                lines.append(f"- {label} ({len(sev_issues)}):\n")
                for item in sev_issues:
                    lines.append(f"  - {item['message']}\n")
            lines.append("\n")

        return "".join(lines)

    # ---------- 1. Snapshot block ----------

    def _summary_block(self, df: pd.DataFrame, segments_df: pd.DataFrame) -> str:
        lines: List[str] = []
        lines.append("## 2. Snapshot\n\n")

        total_leads = len(df)
        lines.append(f"- Total leads processed: **{total_leads}**\n")

        if "StrategicScore" in df.columns:
            avg_score = df["StrategicScore"].mean()
            lines.append(f"- Average StrategicScore: **{avg_score:.1f}**\n")

        if "Segment" in df.columns:
            unique_segments = sorted(df["Segment"].dropna().astype(str).unique().tolist())
            if unique_segments:
                seg_list = ", ".join(unique_segments)
                lines.append(f"- Segments present: **{seg_list}**\n")

        if not segments_df.empty and {"Segment", "LeadCount"}.issubset(segments_df.columns):
            total_from_segments = int(segments_df["LeadCount"].sum())
            lines.append(f"- Leads counted via segment summary: **{total_from_segments}**\n")

        lines.append("\n")
        return "".join(lines)

    # ---------- 2. Completeness block ----------

    def _completeness_block(self, df: pd.DataFrame) -> Tuple[str, List[Dict[str, str]]]:
        lines: List[str] = []
        issues: List[Dict[str, str]] = []
        lines.append("## 3. Column completeness\n\n")

        required_cols = [
            "LeadID",
            "FullName",
            "CompanyName",
            "Email",
            "Industry",
            "Country",
            "JobTitle",
            "SeniorityLevel",
            "CompanySize",
            "Segment",
            "StrategicScore",
        ]

        lines.append("| Column | Present | Missing values | Missing percent |\n")
        lines.append("|--------|---------|----------------|-----------------|\n")

        total_rows = len(df)

        for col in required_cols:
            present = col in df.columns
            if not present:
                lines.append(f"| {col} | No | - | - |\n")
                issues.append(
                    {
                        "severity": "critical",
                        "message": f"Required column `{col}` is missing from the leads dataframe.",
                    }
                )
                continue

            missing_count = int(df[col].isna().sum())
            missing_pct = (missing_count / total_rows * 100) if total_rows > 0 else 0.0
            lines.append(
                f"| {col} | Yes | {missing_count} | {missing_pct:.1f}% |\n"
            )

            # Heuristic thresholds
            if missing_pct > 40 and col in {"Email", "Industry", "Country"}:
                issues.append(
                    {
                        "severity": "high",
                        "message": f"More than 40 percent of values are missing in `{col}`.",
                    }
                )
            elif 10 < missing_pct <= 40 and col in {"JobTitle", "SeniorityLevel", "CompanySize"}:
                issues.append(
                    {
                        "severity": "medium",
                        "message": f"Between 10 and 40 percent of values are missing in `{col}`.",
                    }
                )

        lines.append("\n")
        return "".join(lines), issues

    # ---------- 3. Email and contact quality ----------

    def _email_quality_block(self, df: pd.DataFrame) -> Tuple[str, List[Dict[str, str]]]:
        lines: List[str] = []
        issues: List[Dict[str, str]] = []
        lines.append("## 4. Email and contact quality\n\n")

        if "Email" not in df.columns:
            lines.append("- No `Email` column found. cannot evaluate contact quality.\n\n")
            issues.append(
                {
                    "severity": "critical",
                    "message": "Email column is missing. cannot run email based outreach.",
                }
            )
            return "".join(lines), issues

        total_rows = len(df)
        valid_count = 0
        invalid_count = 0
        empty_count = 0

        for val in df["Email"]:
            status = self._classify_email(val)
            if status == "empty":
                empty_count += 1
            elif status == "invalid":
                invalid_count += 1
            elif status == "valid":
                valid_count += 1

        lines.append(f"- Valid emails: **{valid_count}**\n")
        lines.append(f"- Empty emails: **{empty_count}**\n")
        lines.append(f"- Invalid emails: **{invalid_count}**\n")

        if total_rows > 0:
            empty_pct = empty_count / total_rows * 100
            invalid_pct = invalid_count / total_rows * 100

            if empty_pct > 40:
                issues.append(
                    {
                        "severity": "high",
                        "message": "More than 40 percent of leads have no email address.",
                    }
                )
            elif empty_pct > 20:
                issues.append(
                    {
                        "severity": "medium",
                        "message": "Between 20 and 40 percent of leads have no email address.",
                    }
                )

            if invalid_pct > 15:
                issues.append(
                    {
                        "severity": "high",
                        "message": "More than 15 percent of emails look invalid.",
                    }
                )
            elif invalid_pct > 5:
                issues.append(
                    {
                        "severity": "medium",
                        "message": "Between 5 and 15 percent of emails look invalid.",
                    }
                )

        lines.append("\n")
        return "".join(lines), issues

    # ---------- 4. Score and segment distributions ----------

    def _score_and_segment_block(self, df: pd.DataFrame, segments_df: pd.DataFrame) -> str:
        lines: List[str] = []
        lines.append("## 5. Score and segment distributions\n\n")

        if "StrategicScore" in df.columns:
            score_series = df["StrategicScore"].dropna()
            if not score_series.empty:
                lines.append("**StrategicScore distribution:**\n\n")
                lines.append(f"- Min: **{score_series.min():.1f}**\n")
                lines.append(f"- Max: **{score_series.max():.1f}**\n")
                lines.append(f"- Mean: **{score_series.mean():.1f}**\n")
                lines.append(f"- Median: **{score_series.median():.1f}**\n")
                lines.append("\n")

        if "Segment" in df.columns:
            lines.append("**Leads per segment:**\n\n")
            seg_counts = (
                df["Segment"]
                .fillna("Unassigned")
                .astype(str)
                .value_counts()
            )
            for seg, count in seg_counts.items():
                lines.append(f"- {seg}: **{count}** leads\n")
            lines.append("\n")

        if not segments_df.empty and {"Segment", "AvgStrategicScore"}.issubset(
            segments_df.columns
        ):
            lines.append("**Segment level average scores:**\n\n")
            for _, row in segments_df.sort_values(
                "AvgStrategicScore", ascending=False
            ).iterrows():
                seg = str(row["Segment"])
                avg = float(row["AvgStrategicScore"])
                lines.append(f"- {seg}: average StrategicScore **{avg:.1f}**\n")
            lines.append("\n")

        return "".join(lines)

    # ---------- 5. Playbook validation ----------

    def _playbook_block(self, playbook_md: str) -> Tuple[str, List[Dict[str, str]]]:
        lines: List[str] = []
        issues: List[Dict[str, str]] = []
        lines.append("## 6. Playbook validation\n\n")

        if not playbook_md or "# Pipeline Strategist Agent" not in playbook_md:
            lines.append(
                "- Warning: playbook markdown does not contain the expected header "
                "`# Pipeline Strategist Agent`.\n\n"
            )
            issues.append(
                {
                    "severity": "medium",
                    "message": "Playbook header missing or malformed.",
                }
            )
            return "".join(lines), issues

        length = len(playbook_md.splitlines())
        if length < 15:
            lines.append(
                "- Playbook exists but is quite short. consider enriching segment level guidance.\n\n"
            )
            issues.append(
                {
                    "severity": "low",
                    "message": "Playbook content is short. could be enriched with more guidance.",
                }
            )
        else:
            lines.append("- Playbook markdown generated successfully.\n\n")

        return "".join(lines), issues

    # ---------- 6. Overall assessment and recommendations ----------

    def _overall_assessment_block(
        self,
        df: pd.DataFrame,
        segments_df: pd.DataFrame,
        playbook_md: str,
        issues: List[Dict[str, str]],
    ) -> str:
        lines: List[str] = []
        lines.append("## 7. Overall assessment and recommendations\n\n")

        if not issues:
            lines.append(
                "All core checks passed. The dataset is structurally healthy and suitable for:\n\n"
                "- running outbound campaigns\n"
                "- building dashboards in Power BI\n"
                "- including as a portfolio artifact for a Business Systems or Strategy Analyst role\n\n"
            )
            return "".join(lines)

        lines.append("The following issues were detected. grouped by severity.\n\n")

        for sev in ["critical", "high", "medium", "low"]:
            sev_issues = [i for i in issues if i["severity"] == sev]
            if not sev_issues:
                continue
            label = sev.capitalize()
            lines.append(f"### {label} issues\n\n")
            for item in sev_issues:
                lines.append(f"- {item['message']}\n")
            lines.append("\n")

        lines.append("**Next steps suggested:**\n\n")
        lines.append("- Address critical and high issues before using this dataset in live campaigns.\n")
        lines.append("- Use medium and low issues as a backlog of incremental data quality improvements.\n")
        lines.append("- Once resolved, rerun the pipeline to regenerate a fresh quality report.\n\n")

        return "".join(lines)

    # ---------- helpers ----------

    def _classify_email(self, value: Any) -> str:
        """
        Return one of:
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
