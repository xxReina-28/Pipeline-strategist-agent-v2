# Pipeline Strategist Agent (PSA): A Multi-Agent GTM Intelligence Engine

The **Pipeline Strategist Agent (PSA)** is a modular, multi-agent system designed to turn messy B2B lead data into a complete **GTM intelligence workflow**.  
It ingests, cleans, enriches, scores, segments, and generates a structured **Outbound GTM Playbook** using deterministic logic and optional LLM-powered insights.

PSA is engineered for reproducibility, clarity, and enterprise-grade extensibility.

---

## Architecture Overview

PSA uses a **sequential multi-agent architecture**. Each agent transforms the input, adds structured context, and passes it downstream.


### **High-Level Flow**
1. **Input Layer** — raw CSV/TXT  
2. **Data Processing Module** — cleaning & normalization  
3. **Analytics Module** — enrichment, scoring, segmentation  
4. **Output Module** — polished CSVs + GTM playbook + quality report  

---

## Agents and Responsibilities

### **1. RawIngestionAgent**
- Accepts CSV/TXT  
- Normalizes arbitrary headers  
- Maps fields into a canonical schema  
- Outputs consistent input for the rest of the system  

---

### **2. DataCleanerAgent**
- Removes duplicates  
- Standardizes casing & whitespace  
- Validates required fields  
- Drops invalid rows  

Produces clean, reliable lead data.

---

### **3. AIEnrichmentAgent (Gemini-Ready / Optional)**
This agent is **optional at runtime**, but fully integrated into the architecture.

When enabled, it:
- Builds a structured JSON prompt  
- Sends it to Gemini (or simulated Gemini output if offline)  
- Produces:
  - inferred pain points  
  - maturity signals  
  - value drivers  
  - risks  
  - narrative summary  

These are combined into a single `AI_Notes` column and used downstream.

Disable anytime with:
    --skip_ai

---

### **4. LeadScorerAgent**
Assigns a **StrategicScore** using:
- industry  
- seniority  
- job title patterns  
- contactability  
- optional AI-inferred insights  

---

### **5. SegmentDesignerAgent**
Classifies each lead into PSA segments:
- **A1** – Strategic ICP  
- **A2** – Standard ICP  
- **B1** – Contactable Leads  
- **B2** – AI-Potential Leads  
- **C0** – Disqualified  

Outputs a segment table + summary context.

---

### **6. OutputPolisherAgent**
- Reorders columns  
- Normalizes values  
- Ensures final CSVs are clean and human-readable  

---

### **7. PlaybookWriterAgent**
Generates a full Markdown **Outbound GTM Playbook**, including:
- segment summaries  
- suggested angles  
- AI-based pain points  
- recommended channels  
- narrative messaging  

---

### **8. QualityCheckerAgent**
Produces a final QA report including:
- missing values  
- scoring sanity  
- segmentation validation  
- playbook header verification  

Outputs: `quality_report.md`

---

## How to Run PSA

### **Basic run**

  python -m pipeline_agent.agent \
    --input_csv path/to/leads.csv \
    --output_dir outputs/run_001

**Run with Gemini-style enrichment (default)**

  python -m pipeline_agent.agent \
    --input_csv path/to/leads.csv \
    --output_dir outputs/run_gemini

**Disable enrichment**

  python -m pipeline_agent.agent \
    --input_csv path/to/leads.csv \
    --output_dir outputs/run_no_ai \
    --skip_ai

If no real API key is configured, PSA automatically uses a simulated Gemini response for stability.

## Output Files

Each run generates four artifacts:

|File	| Description |
| cleaned_scored_leads.csv	| Final cleaned, enriched, and scored lead dataset
| lead_segments.csv	| Segment-level summary table
| outbound_playbook.md	| Full Markdown GTM playbook
| quality_report.md	| Data validation & pipeline health report

## Test Datasets

Provided under tests/data/:
  - minimal.csv
  - dirty.csv
  - weird_headers.csv
These validate ingestion, cleaning, segmentation, and enrichment robustness.

## Project Structure

pipeline_agent/
  agent.py
  sub_agents/
    raw_ingestion.py
    data_cleaner.py
    ai_enrichment.py
    lead_scorer.py
    segment_designer.py
    output_polisher.py
    playbook_writer.py
    quality_checker.py
tests/
outputs/
README.md

## Why PSA Matters

PSA demonstrates how modern multi-agent systems can automate real-world GTM data workflows using:
  - deterministic operations
  - context-passing across agents
  - LLM-enriched reasoning (optional)
  - strong observability
  - clean artifact generation
It blends AI agents and traditional data engineering in a reproducible, production-ready design.


