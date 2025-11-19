
from pathlib import Path

from pipeline_agent.agent import PipelineStrategistAgent


def test_pipeline_runs():
    input_csv = Path("lead_list_100.csv")
    if not input_csv.exists():
        # Minimal smoke test. nothing to do if file missing.
        return

    agent = PipelineStrategistAgent()
    agent.run(str(input_csv), output_dir="test_outputs")


if __name__ == "__main__":
    test_pipeline_runs()
    print("Test completed.")
