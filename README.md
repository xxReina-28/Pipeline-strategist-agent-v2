📘 README.md — Pipeline Strategist Agent (PSA)
Pipeline Strategist Agent

A modular outbound GTM engine that transforms raw lead lists into a fully-generated, multi-segment go-to-market playbook.

Built for the Google AI Agents Intensive Capstone.
Refined into a production-grade portfolio project.

What This Agent Does
The Pipeline Strategist Agent takes any messy lead file (CSV/TXT) and automatically:

1. Ingests & normalizes data
  - maps arbitrary headers to canonical fields
  - handles weird CSVs and inconsistent formatting

2. Cleans & validates leads

  - whitespace normalization
  - email normalization
  - deduplication
  - blank-row elimination
  - required-column creation

3. Optionally runs AI enrichment

 - pain points
 - behavioral signals
 - ICP fit notes

4. Scores leads

  - role, industry, region, seniority
  - strategic value
  - quality indicators

5. Segments the list into:

  - A1 Strategic ICP
  - A2 Standard ICP
  - B1 Contactable Leads
  - B2 AI-Potential Leads
  - C0 Disqualified

6. Polishes final cleaned dataset
7. Generates a complete GTM Playbook (Markdown)
8. Produces a Quality Report

All outputs are saved in a structured folder.

Outputs Generated

Each run produces:

  output_dir/
    cleaned_scored_leads.csv
    lead_segments.csv
    outbound_playbook.md
    quality_report.md


These artifacts can be directly included in a sales enablement library, CRM upload, or GTM ops workflow.

High-Level Architecture
┌──────────────────────────┐
│ Raw Ingestion (CSV/TXT)  │
└──────────────┬───────────┘
               ▼
┌──────────────────────────┐
│ Data Cleaner             │
│ - normalize headers      │
│ - remove blanks          │
│ - deduplicate            │
└──────────────┬───────────┘
               ▼
┌──────────────────────────┐
│ (Optional) AI Enrichment │
│ - pain points            │
│ - ICP signals            │
└──────────────┬───────────┘
               ▼
┌──────────────────────────┐
│ Lead Scorer              │
└──────────────┬───────────┘
               ▼
┌──────────────────────────┐
│ Segment Designer         │
│ A1 / A2 / B1 / B2 / C0   │
└──────────────┬───────────┘
               ▼
┌──────────────────────────┐
│ Output Polisher          │
└──────────────┬───────────┘
               ▼
┌──────────────────────────┐
│ Playbook Writer          │
│ + Quality Report         │
└──────────────────────────┘

How to Run

1. Install dependencies
pip install -r requirements.txt

2. Run the pipeline
python -m pipeline_agent.agent \
  --input_csv tests/data/minimal.csv \
  --output_dir outputs/run_example

3. Optional: skip AI enrichment
python -m pipeline_agent.agent \
  --input_csv yourfile.csv \
  --output_dir outputs/run_no_ai \
  --skip_ai

Test Datasets Included
Located in:

tests/data/
✔ minimal.csv
Clean, simple inputs for smoke testing.

✔ dirty.csv
Tests cleaning, normalization, and deduplication.

✔ weird_headers.csv
Tests header aliasing + ingestion resilience.
These files ensure the pipeline behaves predictably even with ugly real-world data.

Key Modules
🔹 RawIngestionAgent
Handles CSV or TXT ingestion. Applies header normalization.

🔹 DataCleanerAgent
Ensures required columns exist, applies whitespace cleanup, lowercasing, dedupe.

🔹 LeadScorerAgent
Assigns a strategic score for segmentation.

🔹 SegmentDesignerAgent
Segment logic used by the GTM framework.

🔹 PlaybookWriterAgent
Generates a fully formatted Markdown GTM playbook with:
  - narrative
  - positioning
  - channel strategy
  - essaging templates
  - AI-inferred pain points

🔹 OutputPolisherAgent
Final dataframe refinements.

🔹 QualityCheckerAgent
Validates completeness and logic of final output.

Example Playbook Snippet

  ## A1 Strategic ICP

  ### Ideal personas
  C-level and VP-level executives in cyber, fintech, IT, and SaaS.

  ### Strategic narrative
  You are speaking to owners of risk, growth, and cost.

  ### Messaging template
  Hi {{Name}},
  Given your role overseeing {{Function}} at {{Company}}, I wanted to share a quick observation...

Error Handling & Pipeline Hardening

The pipeline is hardened with:

  - safe file validation
  - defensive playbook generation
  - early termination if cleaning removes all rows
  - robust header mapping
  - guaranteed markdown output
  - clear human-readable errors

This makes the system production-safe and predictable.

Folder Structure:

  pipeline_agent/
    agent.py
    sub_agents/
      raw_ingestion.py
      data_cleaner.py
      segment_designer.py
      lead_scorer.py
      playbook_writer.py
      quality_checker.py
      output_polisher.py
    tools/
  tests/
    data/
  outputs/
  README.md

Future Enhancements

  - Config-driven scoring + ICP rules
  - Multi-industry playbook templates
  - PDF playbook generation
  - Web UI + API interface
  - CRM connector (HubSpot / Salesforce input + output)

Author:

Niña Peterine Sheen Suico (Reina)
Business Systems/Strategy Analyst in the making
Building GTM systems, AI agents, and operational intelligence frameworks.