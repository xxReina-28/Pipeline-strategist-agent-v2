
from __future__ import annotations

from typing import List

import pandas as pd


class PlaybookWriterAgent:
    """
    Builds a markdown outbound playbook using the scored leads
    and aggregated segment statistics.
    """

    def run(self, df_scored: pd.DataFrame, segments_df: pd.DataFrame) -> str:
        lines: List[str] = []

        total_leads = len(df_scored)
        lines.append("# Pipeline Strategist Agent. Outbound Playbook")
        lines.append("")
        lines.append(f"Total leads analyzed: **{total_leads}**")
        lines.append("")

        # Overview per segment
        lines.append("## Segment Overview")
        lines.append("")
        if not segments_df.empty:
            lines.append("| Segment | Lead Count | Avg Priority Score |")
            lines.append("|---------|------------|--------------------|")
            for row in segments_df.itertuples():
                lines.append(
                    f"| {row.Segment} | {row.LeadCount} | {row.AvgPriorityScore} |"
                )
        else:
            lines.append("No segments were generated.")
        lines.append("")

        # Strategy per segment
        lines.append("## Segment Strategies")
        lines.append("")
        for row in segments_df.itertuples():
            segment = row.Segment
            count = row.LeadCount

            lines.append(f"### {segment}")
            lines.append("")
            lines.append(f"- Approximate lead volume: **{count}**")

            if segment == "Strategic Core":
                lines.extend(
                    [
                        "- Primary goal. land multi year strategic accounts.",
                        "- Owner. senior AE plus leadership sponsor.",
                        "- Channels. warm introductions, executive email, LinkedIn, targeted events.",
                        "- Cadence. 1 to 1 highly personalized outreach, weekly touchpoints.",
                    ]
                )
            elif segment == "Growth Focus":
                lines.extend(
                    [
                        "- Primary goal. convert into ARR within 1 to 2 quarters.",
                        "- Owner. mid market AE, supported by SDR.",
                        "- Channels. outbound email sequences, LinkedIn, occasional calls.",
                        "- Cadence. 5 to 7 touch sequence over 3 to 4 weeks.",
                    ]
                )
            elif segment == "Nurture":
                lines.extend(
                    [
                        "- Primary goal. educate and stay top of mind.",
                        "- Owner. marketing automation, SDR on low touch.",
                        "- Channels. newsletters, product updates, one to many webinars.",
                        "- Cadence. monthly value emails, quarterly check in from SDR.",
                    ]
                )
            else:
                lines.extend(
                    [
                        "- Primary goal. collect more context, do not over invest.",
                        "- Owner. automated flows primarily.",
                        "- Channels. light email touch, retargeting where possible.",
                        "- Cadence. occasional newsletter only.",
                    ]
                )
            lines.append("")

        # Next best actions
        lines.append("## Next Best Actions")
        lines.append("")
        lines.append("- Export `cleaned_scored_leads.csv` into your CRM.")
        lines.append("- Map Segment and StrategicScore into lead fields.")
        lines.append("- Build campaigns that mirror the segment strategies above.")
        lines.append("- Review the quality report before going live.")
        lines.append("")

        return "\n".join(lines)
