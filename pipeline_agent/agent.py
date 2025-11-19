
from pathlib import Path

from .sub_agents.data_cleaner import DataCleanerAgent
from .sub_agents.segment_designer import SegmentDesignerAgent
from .sub_agents.lead_scorer import LeadScorerAgent
from .sub_agents.playbook_writer import PlaybookWriterAgent
from .sub_agents.quality_checker import QualityCheckerAgent
from .tools.csv_loader import load_leads_from_csv
from .tools.csv_saver import save_dataframe, save_markdown


class PipelineStrategistAgent:
    """Simple end to end implementation of the Pipeline Strategist Agent.

    This version is framework agnostic so it can run on GitHub or Kaggle
    without extra dependencies. It mimics the orchestration pattern you
    would later migrate into the Google Agents ADK.
    """

    def __init__(self):
        self.data_cleaner = DataCleanerAgent()
        self.segment_designer = SegmentDesignerAgent()
        self.lead_scorer = LeadScorerAgent()
        self.playbook_writer = PlaybookWriterAgent()
        self.quality_checker = QualityCheckerAgent()

    def run(self, input_csv: str, output_dir: str = "outputs") -> None:
        input_path = Path(input_csv)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # 1. Load
        df_raw = load_leads_from_csv(input_path)

        # 2. Clean
        df_clean = self.data_cleaner.run(df_raw)

        # 3. Segment
        df_segmented, segments_df = self.segment_designer.run(df_clean)

        # 4. Score
        df_scored = self.lead_scorer.run(df_segmented)

        # 5. Write playbook
        playbook_md = self.playbook_writer.run(df_scored, segments_df)

        # 6. Quality checks
        report = self.quality_checker.run(df_scored, segments_df, playbook_md)

        # 7. Save outputs
        save_dataframe(df_scored, output_path / "cleaned_scored_leads.csv")
        save_dataframe(segments_df, output_path / "lead_segments.csv")
        save_markdown(playbook_md, output_path / "outbound_playbook.md")
        save_markdown(report, output_path / "quality_report.md")

        print(f"Pipeline complete. Outputs saved to {output_path.resolve()}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run the Pipeline Strategist Agent.")
    parser.add_argument(
        "--input_csv",
        type=str,
        default="lead_list_100.csv",
        help="Path to the input leads CSV.",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="outputs",
        help="Directory where outputs will be written.",
    )

    args = parser.parse_args()
    agent = PipelineStrategistAgent()
    agent.run(args.input_csv, args.output_dir)


if __name__ == "__main__":
    main()
