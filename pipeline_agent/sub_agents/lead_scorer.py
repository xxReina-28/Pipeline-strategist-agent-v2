
import pandas as pd


class LeadScorerAgent:
    """Applies a simple scoring model to each lead.

    The score combines:
    - base PriorityScore
    - company size weight
    - status weight
    - region focus
    """

    def __init__(self):
        self.size_weight = {
            "1-10": 0,
            "11-50": 1,
            "51-200": 2,
            "201-500": 3,
            "500+": 4,
        }
        self.status_weight = {
            "New": 0,
            "Contacted": 1,
            "In Progress": 2,
            "Nurture": 1,
            "Qualified": 3,
            "Unqualified": -2,
        }

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df["SizeWeight"] = df["CompanySize"].map(self.size_weight).fillna(0)
        df["StatusWeight"] = df["LeadStatus"].map(self.status_weight).fillna(0)

        df["StrategicScore"] = (
            df["PriorityScore"].astype(float)
            + df["SizeWeight"]
            + df["StatusWeight"]
        )

        df["StrategicRank"] = df["StrategicScore"].rank(ascending=False, method="dense")

        return df
