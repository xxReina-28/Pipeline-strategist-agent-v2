from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from pipeline_agent.sub_agents.data_cleaner import DataCleanerAgent
from pipeline_agent.sub_agents.lead_scorer import LeadScorerAgent
from pipeline_agent.sub_agents.segment_designer import SegmentDesignerAgent
from pipeline_agent.sub_agents.playbook_writer import PlaybookWriterAgent
from pipeline_agent.sub_agents.quality_checker import QualityCheckerAgent
from pipeline_agent.sub_agents.output_polisher import OutputPolisherAgent

# Optional AI enrichment
try:
    from pipeline_agent.sub_agents.ai_enrichment import AIEnrichmentAgent
except ImportError:
    AIEnrichmentAgent = None

# Optional ingestion helper for TXT
try:
    from pipeline_agent.sub_agents.raw_ingestion import RawIngestionAgent
except ImportError:
    RawIngestionAgent = None


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def _normalize_csv_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize raw CSV headers (id, full name, mail, etc.)
    into the canonical schema used by the pipeline
    (LeadID, FullName, Email, Industry, Country, etc.).
    """

    # 1. Normalize header names to lowercase with trimmed spaces
    normalized = {c: c.strip().lower() for c in df.columns}
    df = df.rename(columns=normalized)

    # 2. Map raw column names to canonical ones
    raw_to_canonical = {
        # IDs
        "id": "LeadID",
        "leadid": "LeadID",

        # Names and company
        "full name": "FullName",
        "fullname": "FullName",
        "full_name": "FullName",
        "name": "FullName",

        "company": "CompanyName",
        "company name": "CompanyName",
        "companyname": "CompanyName",
        "biz": "CompanyName",

        # Email
        "email": "Email",
        "mail": "Email",
        "email_address": "Email",

        # Industry
        "industry": "Industry",
        "industry_sector": "Industry",

        # Company size
        "company size": "CompanySize",
        "companysize": "CompanySize",
        "size": "CompanySize",

        # Country
        "country": "Country",
        "country_code": "Country",

        # Role and seniority
        "jobtitle": "JobTitle",
        "job title": "JobTitle",
        "job_title": "JobTitle",
        "role": "JobTitle",

        "seniority": "SeniorityLevel",
        "seniority_level": "SeniorityLevel",

        # Lead status
        "status": "LeadStatus",
        "leadstatus": "LeadStatus",

        # Optional existing score
        "priority score": "PriorityScore",
        "priorityscore": "PriorityScore",
    }

    for raw, canon in raw_to_canonical.items():
        if raw in df.columns:
            if canon not in df.columns:
                df[canon] = df[raw]
            else:
                # If canonical exists but has gaps, fill from raw
                df[canon] = df[canon].where(~df[canon].isna(), df[raw])

    # 3. Ensure required canonical columns exist
    required_cols = [
        "LeadID",
        "FullName",
        "CompanyName",
        "Email",
        "Industry",
        "CompanySize",
        "Country",
        "JobTitle",
        "SeniorityLevel",
        "LeadStatus",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    return df


def setup_logging() -> None:
    """Basic console logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(message)s",
    )


def validate_paths(input_csv: str, output_dir: str) -> tuple[Path, Path]:
    """
    Validate input and output paths early so the user
    gets a clear error instead of a long traceback.
    """
    input_path = Path(input_csv)
    if not input_path.exists():
        logging.error(f"Input file not found. {input_path}")
        raise SystemExit(1)

    if input_path.suffix.lower() not in {".csv", ".txt"}:
        logging.error(
            f"Unsupported input type {input_path.suffix}. "
            "Use a .csv or .txt file."
        )
        raise SystemExit(1)

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    return input_path, out_dir


# ---------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------

def run_pipeline(
    input_path: Path,
    output_dir: Path,
    skip_ai: bool = False,
) -> dict[str, Path]:
    """
    End to end pipeline orchestrator.

    Steps.
      1. Load raw leads (CSV or TXT)
      2. Clean data
      3. Optional AI enrichment
      4. Lead scoring
      5. Segment design
      6. Output polishing
      7. Playbook generation
      8. Quality report
      9. Save all outputs
    """

    # output_dir is already created by validate_paths but keep this idempotent
    output_dir.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------
    # 1. LOAD RAW DATA
    # ---------------------------------------------------------
    suffix = input_path.suffix.lower()

    if suffix == ".csv":
        try:
            df_raw = pd.read_csv(input_path)
        except Exception as e:
            logging.error(f"Failed to read CSV {input_path}. Error. {e}")
            raise
        df_raw = _normalize_csv_columns(df_raw)

    elif suffix == ".txt":
        if RawIngestionAgent is None:
            raise RuntimeError(
                "TXT ingestion requested but RawIngestionAgent is not available."
            )
        text = input_path.read_text(encoding="utf-8")
        ingestor = RawIngestionAgent()
        df_raw = ingestor.from_text(text)

    else:
        raise ValueError(f"Unsupported file extension. {suffix}")

    if df_raw is None or df_raw.empty:
        raise ValueError("Loaded dataframe is empty after ingestion step.")

    logging.info(f"Loaded {len(df_raw)} raw rows from {input_path}")
  
    # ---------------------------------------------------------
    # 2. CLEAN DATA
    # ---------------------------------------------------------
    cleaner = DataCleanerAgent()
    df_clean = cleaner.run(df_raw)

    # Hardening. if cleaning removed everything, stop with a clear error
    if df_clean is None or df_clean.empty:
        raise ValueError(
            "No valid leads remain after cleaning. "
            "All rows failed basic quality or ICP checks."
        )


    # ---------------------------------------------------------
    # 3. OPTIONAL AI ENRICHMENT
    # ---------------------------------------------------------
    if not skip_ai and AIEnrichmentAgent is not None:
        logging.info("Running AI enrichment step")
        enricher = AIEnrichmentAgent()
        df_enriched = enricher.run(df_clean)
    else:
        if skip_ai:
            logging.info("Skipping AI enrichment as requested")
        elif AIEnrichmentAgent is None:
            logging.info("AIEnrichmentAgent not available. Skipping AI step")
        df_enriched = df_clean

    # ---------------------------------------------------------
    # 4. LEAD SCORING
    # ---------------------------------------------------------
    scorer = LeadScorerAgent()
    df_scored = scorer.run(df_enriched)

    # ---------------------------------------------------------
    # 5. SEGMENT DESIGN
    # ---------------------------------------------------------
    segmenter = SegmentDesignerAgent()
    df_segmented, segments_df = segmenter.run(df_scored)
    logging.info(f"Segments created. {segments_df.shape[0]} segment rows")

    # ---------------------------------------------------------
    # 6. OUTPUT POLISHING
    # ---------------------------------------------------------
    polisher = OutputPolisherAgent()
    df_polished = polisher.run(df_segmented)

    # ---------------------------------------------------------
    # 7. PLAYBOOK GENERATION
    # ---------------------------------------------------------
    playbook_writer = PlaybookWriterAgent()
    playbook_md = playbook_writer.run(df_polished, segments_df)

    # ---------------------------------------------------------
    # 8. QUALITY REPORT
    # ---------------------------------------------------------
    quality_checker = QualityCheckerAgent()
    quality_report_md = quality_checker.run(
        df_polished,
        segments_df,
        playbook_md,
    )

    # ---------------------------------------------------------
    # 9. SAVE OUTPUTS
    # ---------------------------------------------------------
    cleaned_scored_path = output_dir / "cleaned_scored_leads.csv"
    segments_path = output_dir / "lead_segments.csv"
    playbook_path = output_dir / "outbound_playbook.md"
    quality_path = output_dir / "quality_report.md"

    from pipeline_agent.tools.csv_saver import save_dataframe, save_markdown

    save_dataframe(df_polished, cleaned_scored_path)
    save_dataframe(segments_df, segments_path)
    save_markdown(playbook_md, playbook_path)
    save_markdown(quality_report_md, quality_path)

    logging.info("All outputs saved successfully.")

    return {
        "cleaned_scored_leads": cleaned_scored_path,
        "segments": segments_path,
        "playbook": playbook_path,
        "quality_report": quality_path,
    }


# ---------------------------------------------------------
# CLI
# ---------------------------------------------------------

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline Strategist Agent . End to end lead processing pipeline."
    )

    parser.add_argument(
        "--input_csv",
        type=str,
        required=True,
        help="Path to input CSV or TXT file.",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        required=True,
        help="Directory where outputs will be written.",
    )
    parser.add_argument(
        "--skip_ai",
        action="store_true",
        help="Skip AI enrichment step.",
    )

    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)

    setup_logging()
    input_path, output_dir = validate_paths(args.input_csv, args.output_dir)

    logging.info(f"Reading leads from {input_path}")
    logging.info(f"Writing outputs to {output_dir}")

    outputs = run_pipeline(
        input_path=input_path,
        output_dir=output_dir,
        skip_ai=bool(args.skip_ai),
    )

    print("Pipeline complete. Outputs written to:")
    for name, path in outputs.items():
        print(f"  - {name}: {path}")


if __name__ == "__main__":
    main()
