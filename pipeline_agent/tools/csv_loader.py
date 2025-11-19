
import pandas as pd
from pathlib import Path


def load_leads_from_csv(path: Path) -> pd.DataFrame:
    """Load leads from a CSV file into a dataframe."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found at {path}")
    return pd.read_csv(path)
