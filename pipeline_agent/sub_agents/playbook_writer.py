
import pandas as pd
from textwrap import dedent


class PlaybookWriterAgent:
    """Generates a markdown outreach playbook per segment."""

    def run(self, df: pd.DataFrame, segments_df: pd.DataFrame) -> str:
        lines = []
        lines.append("# Pipeline Strategist Agent. Outbound Playbook\n")

        segments_df_sorted = segments_df.sort_values(
            by=["AvgPriority", "LeadCount"], ascending=False
        )

        for _, seg in segments_df_sorted.iterrows():
            segment_name = seg["Segment"]
            lines.append(f"## Segment. {segment_name}\n")
            lines.append(
                f"- Leads in segment. {int(seg['LeadCount'])}\n"
                f"- Average priority. {seg['AvgPriority']:.2f}\n"
                f"- Industries. {seg['Industries']}\n"
                f"- Regions. {seg['Regions']}\n"
            )

            seg_leads = df[df["Segment"] == segment_name].copy()
            seg_leads = seg_leads.sort_values("StrategicScore", ascending=False).head(5)

            lines.append("### Recommended angle\n")
            example_industry = seg_leads["Industry"].mode().iat[0]
            lines.append(
                f"Focus on how outsourcing can reduce operational drag and unlock growth for {example_industry} companies.\n"
            )

            lines.append("### Top 5 leads to start with\n")
            lines.append("| Lead | Company | Country | Job Title | Score |\n")
            lines.append("|------|---------|---------|-----------|-------|\n")
            for _, row in seg_leads.iterrows():
                lines.append(
                    f"| {row['FullName']} | {row['CompanyName']} | {row['Country']} | "
                    f"{row['JobTitle']} | {row['StrategicScore']:.1f} |\n"
                )

            lines.append("\n---\n")

        return "".join(lines)
