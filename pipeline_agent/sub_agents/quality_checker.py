
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
