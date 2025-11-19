
import pandas as pd


class SegmentDesignerAgent:
    """Designs practical segments based on industry, region, and seniority."""

    def __init__(self):
        # Simple region mapping that can be extended later
        self.region_map = {
            "US": "North America",
            "Canada": "North America",
            "UK": "Europe",
            "Germany": "Europe",
            "Australia": "APAC",
            "Singapore": "APAC",
            "Philippines": "APAC",
            "India": "APAC",
            "UAE": "Middle East",
        }

    def _region_for_country(self, country: str) -> str:
        return self.region_map.get(country, "Other")

    def _seniority_bucket(self, seniority: str) -> str:
        seniority = seniority.lower()
        if "c-level" in seniority or "vp" in seniority:
            return "Executive"
        if "director" in seniority:
            return "Director"
        if "senior" in seniority:
            return "Senior"
        if "mid" in seniority:
            return "Mid"
        return "Other"

    def run(self, df: pd.DataFrame):
        df = df.copy()

        df["Region"] = df["Country"].apply(self._region_for_country)
        df["SeniorityBucket"] = df["SeniorityLevel"].apply(self._seniority_bucket)

        df["Segment"] = (
            df["Industry"].fillna("Unknown")
            + " | "
            + df["Region"].fillna("Unknown")
            + " | "
            + df["SeniorityBucket"].fillna("Unknown")
        )

        segments_df = (
            df.groupby("Segment")
            .agg(
                LeadCount=("LeadID", "count"),
                AvgPriority=("PriorityScore", "mean"),
                Industries=("Industry", lambda x: ", ".join(sorted(set(x)))),
                Regions=("Region", lambda x: ", ".join(sorted(set(x)))),
            )
            .reset_index()
            .sort_values(by=["AvgPriority", "LeadCount"], ascending=False)
        )

        return df, segments_df
