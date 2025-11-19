
import pandas as pd


class DataCleanerAgent:
    """Cleans and normalizes the raw lead list.

    This includes:
    - trimming whitespace
    - standardizing column names
    - normalizing industry labels
    - filling simple missing values
    """

    def __init__(self):
        self.required_columns = [
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
            "PriorityScore",
        ]

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Standardize column names
        df.columns = [c.strip().replace(" ", "_") for c in df.columns]

        # Ensure required columns exist
        missing = [c for c in self.required_columns if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Trim strings
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

        # Normalize industry text
        df["Industry"] = (
            df["Industry"]
            .str.title()
            .replace(
                {
                    "Saas": "SaaS",
                    "Fin-tech": "Fintech",
                    "Cyber Security": "Cybersecurity",
                }
            )
        )

        # Normalize company size categories
        size_map = {
            "1-10": "1-10",
            "11-50": "11-50",
            "51-200": "51-200",
            "201-500": "201-500",
            "500+": "500+",
            "Enterprise": "500+",
        }
        df["CompanySize"] = df["CompanySize"].replace(size_map)

        # Fill missing priority with median
        if df["PriorityScore"].isna().any():
            median_score = df["PriorityScore"].median()
            df["PriorityScore"] = df["PriorityScore"].fillna(median_score)

        return df
