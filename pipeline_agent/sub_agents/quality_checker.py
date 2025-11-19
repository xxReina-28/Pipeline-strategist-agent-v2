
import pandas as pd


class QualityCheckerAgent:
    """Runs simple quality checks on outputs."""

    def run(self, df: pd.DataFrame, segments_df: pd.DataFrame, playbook_md: str) -> str:
        issues = []

        # Existing logical checks
        if df.empty:
            issues.append("- Lead dataframe is empty.")

        if segments_df.empty:
            issues.append("- No segments were generated.")

        if "StrategicScore" not in df.columns:
            issues.append("- StrategicScore column missing.")

        if "Segment" not in df.columns:
            issues.append("- Segment column missing.")

        if "# Pipeline Strategist Agent" not in playbook_md:
            issues.append("- Playbook does not contain the expected header.")

        required_cols = [
            "LeadID",
            "FullName",
            "CompanyName",
            "Email",
            "Industry",
            "Country",
            "JobTitle",
            "Segment",
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            issues.append(f"- Missing important columns: {missing}")

        # === New: dataset stats ===
        n_rows, n_cols = df.shape
        duplicate_rows = int(df.duplicated().sum()) if not df.empty else 0

        if not df.empty:
            missing_counts = df.isna().sum()
            missing_total = int(missing_counts.sum())
        else:
            missing_counts = None
            missing_total = 0

        # === Build markdown report ===
        report_lines: list[str] = []

        # Header
        report_lines.append("# Quality Report")
        report_lines.append("")

        # Dataset overview
        report_lines.append("## Dataset Overview")
        report_lines.append("")
        report_lines.append(f"- Total rows: **{n_rows}**")
        report_lines.append(f"- Total columns: **{n_cols}**")
        report_lines.append(f"- Duplicate rows: **{duplicate_rows}**")
        report_lines.append("")

        # Quality checks section (your existing rules)
        report_lines.append("## Quality Checks")
        report_lines.append("")
        if issues:
            report_lines.extend(issues)
        else:
            report_lines.append("- All rule-based quality checks passed.")
        report_lines.append("")

        # Missing values per column
        report_lines.append("## Missing Values by Column")
        report_lines.append("")
        if df.empty:
            report_lines.append("Dataset is empty, missing value analysis not applicable.")
        elif missing_total == 0:
            report_lines.append("No missing values detected in any column.")
        else:
            report_lines.append("| Column | Missing Count | Missing % |")
            report_lines.append("|--------|---------------|-----------|")
            for col in df.columns:
                if missing_counts[col] > 0:
                    pct = round(missing_counts[col] / n_rows * 100, 1)
                    report_lines.append(
                        f"| {col} | {int(missing_counts[col])} | {pct}% |"
                    )
        report_lines.append("")

        # Segment distribution
        report_lines.append("## Leads per Segment")
        report_lines.append("")
        if not segments_df.empty and "Segment" in segments_df.columns:
            seg_counts = segments_df["Segment"].value_counts()
            report_lines.append("| Segment | Lead Count |")
            report_lines.append("|---------|------------|")
            for seg, count in seg_counts.items():
                report_lines.append(f"| {seg} | {int(count)} |")
            report_lines.append("")
        else:
            report_lines.append("No segment information available.")
            report_lines.append("")

        # Summary
        report_lines.append("## Summary")
        report_lines.append("")
        if not issues and missing_total == 0 and duplicate_rows == 0:
            report_lines.append(
                "All basic quality checks passed. No missing values, no duplicates, "
                "and all required columns are present."
            )
        else:
            report_lines.append(
                "Some potential data quality risks were detected. "
                "Review the sections above and consider applying additional cleaning steps "
                "before moving to production."
            )

        return "\n".join(report_lines) + "\n"


import pandas as pd


class QualityCheckerAgent:
    """Runs simple quality checks on outputs."""

    def run(self, df: pd.DataFrame, segments_df: pd.DataFrame, playbook_md: str) -> str:
        issues = []

        if df.empty:
            issues.append("- Lead dataframe is empty.")

        if segments_df.empty:
            issues.append("- No segments were generated.")

        if "StrategicScore" not in df.columns:
            issues.append("- StrategicScore column missing.")

        if "Segment" not in df.columns:
            issues.append("- Segment column missing.")

        if "# Pipeline Strategist Agent" not in playbook_md:
            issues.append("- Playbook does not contain the expected header.")

        required_cols = [
            "LeadID",
            "FullName",
            "CompanyName",
            "Email",
            "Industry",
            "Country",
            "JobTitle",
            "Segment",
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            issues.append(f"- Missing important columns. {missing}")

        if not issues:
            issues.append("All basic quality checks passed.")

        report_lines = ["# Quality Report\n", "\n"]
        for item in issues:
            report_lines.append(f"{item}\n")

        return "".join(report_lines)
