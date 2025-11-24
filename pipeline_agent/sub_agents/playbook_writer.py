from __future__ import annotations

from typing import List

import pandas as pd


class PlaybookWriterAgent:
    """
    Generates a polished GTM outbound playbook from segmented + scored leads.

    The playbook includes:
      - Segment-level summary table (from segments_df)
      - Segment-specific strategies (A1/A2/B1/B2/C0)
      - AI-inferred pain points where available
    """

    # ---------------------------------------------------------
    # Main entry point
    # ---------------------------------------------------------

    def run(self, df: pd.DataFrame, segments_df: pd.DataFrame) -> str:
        """
        df: per-lead dataframe (cleaned + scored + segmented)
        segments_df: per-segment summary from SegmentDesignerAgent
        """
        md: List[str] = []
        md.append("# Pipeline Strategist Agent (PSA)\n")
        md.append("## Outbound GTM Playbook\n\n")

        # 1. Segment summary
        md.append("## Segment summary\n\n")
        md.append(self._build_segment_summary(segments_df))
        md.append("\n---\n\n")

        # 2. Per-segment playbooks
        # Defensive handling. If no Segment column or no values, exit gracefully with markdown.
        if "Segment" not in df.columns:
            return (
                "# Outbound GTM Playbook\n"
                "_No segments were assigned because no valid leads received segment labels. "
                "Check earlier pipeline steps such as scoring and segmentation._\n"
            )

        segments_series = df["Segment"].dropna()

        if segments_series.empty:
            return (
                "# Outbound GTM Playbook\n"
                "_Segmentation produced no assignable segments. "
                "All leads may have failed ICP or quality rules._\n"
            )

        segments = segments_series.unique().tolist()

        for segment in segments:
            subdf = df[df["Segment"] == segment]

            md.append(f"## {segment}\n\n")

            # Handle disqualified separately
            if segment == "C0 Disqualified":
                md.append(
                    "These leads do not meet minimum quality or ICP criteria.\n\n"
                    "- Invalid or missing email\n"
                    "- Non-target industry or location\n"
                    "- Missing key business fields\n\n"
                )
                md.append("\n---\n\n")
                continue

            # AI-inferred pain points (from AI_Notes if present)
            pain_points: List[str] = []
            if "AI_Notes" in subdf.columns:
                pain_points = (
                    subdf["AI_Notes"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .replace("", pd.NA)
                    .dropna()
                    .head(5)
                    .tolist()
                )

            if pain_points:
                md.append("### Common pain points (AI inferred)\n\n")
                for p in pain_points:
                    md.append(f"- {p}\n")
                md.append("\n")

            # Segment-specific playbook content
            md.append(self._segment_playbook_block(segment))
            md.append("\n---\n\n")

        # Always return a markdown string
        return "".join(md).strip()

    # ---------------------------------------------------------
    # Segment summary from segments_df
    # ---------------------------------------------------------

    def _build_segment_summary(self, segments_df: pd.DataFrame) -> str:
        """
        Build the top-level summary table from the segment summary dataframe.
        Expected columns in segments_df:
          - Segment
          - LeadCount
          - AvgStrategicScore
          - ICP_Percentage
          - ValidEmail_Percentage
        """
        if segments_df.empty:
            return "_No segments found._"

        lines: List[str] = []
        lines.append("| Segment | Count | Avg score | ICP% | Email valid% |\n")
        lines.append("|---------|-------|-----------|------|--------------|\n")

        for _, row in segments_df.iterrows():
            lines.append(
                f"| {row['Segment']} | {int(row['LeadCount'])} | "
                f"{float(row['AvgStrategicScore']):.2f} | "
                f"{float(row['ICP_Percentage']):.1f}% | "
                f"{float(row['ValidEmail_Percentage']):.1f}% |\n"
            )

        return "".join(lines)

    # ---------------------------------------------------------
    # Segment-specific blocks
    # ---------------------------------------------------------

    def _segment_playbook_block(self, segment: str) -> str:
        """
        Route to the appropriate segment block.
        """
        if segment == "A1 Strategic ICP":
            return self._block_a1()

        if segment == "A2 Standard ICP":
            return self._block_a2()

        if segment == "B1 Contactable Leads":
            return self._block_b1()

        if segment == "B2 AI-Potential Leads":
            return self._block_b2()

        # Fallback
        return "(No specific playbook is defined for this segment yet.)\n"

    def _block_a1(self) -> str:
        return """
### Ideal personas
C-level and VP-level executives in cyber, fintech, IT, and SaaS.

### Channel strategy
1. Email with executive framing
2. Signal-based LinkedIn outreach
3. Invite-only workshop or briefing as CTA

### Strategic narrative
You are speaking to owners of risk, growth, and cost.
The story focuses on:
- risk reduction
- strategic visibility
- acceleration of change

### Messaging template
Hi {{Name}},

Given your role overseeing {{Function}} at {{Company}}, I wanted to share a quick observation.

Executive teams in {{Industry}} who are scaling across {{Region}} are usually fighting two things at once:
fragmented workflows and rising risk around visibility and control.

We have been helping leaders consolidate their security and operations view
so they can make decisions on real-time data instead of stitched spreadsheets.

If you are open to it, I can send a short executive summary and, if useful,
we can turn that into a 15-minute briefing.
"""

    def _block_a2(self) -> str:
        return """
### Ideal personas
Security leaders, IT managers, Heads of Ops, regional directors.

### Recommended channels
1. Direct email
2. LinkedIn follow-up
3. Phone if warm or referred

### Strategic narrative
You are solving:
- workflow fragmentation
- data inconsistency
- operational bottlenecks

### Messaging template
Hi {{Name}},

Noticed {{Company}} is expanding its {{Industry}} footprint in {{Region}}.
Teams at this stage often face {{PainPoint1}} and {{PainPoint2}} when workflows start scaling.

We helped similar firms reduce manual work by {{X%}} and improve visibility into
pipelines, risks, and handoffs.

Would a 15-minute diagnostic call be useful to see if the same patterns apply to your team?
"""

    def _block_b1(self) -> str:
        return """
### Channel strategy
High-volume, low-friction, email-first.

### Positioning
Practical value, consolidation, less busywork for the team.

### Messaging template
Hi {{Name}},

Many {{Industry}} teams who are dealing with {{PainPoint}} are starting to modernize
how they handle their workflows and reporting.

If it would be helpful, I can share a brief insight summary that is tailored to your role.
No obligation, you can use it internally if it is useful.
"""

    def _block_b2(self) -> str:
        """
        B2 layout fallback.
        Reuses B1 for now so the pipeline runs.
        """
        return self._block_b1()
