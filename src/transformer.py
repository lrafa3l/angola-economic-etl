"""
transformer.py
==============
Cleans, type-casts, and normalises raw World Bank data into a
single tidy DataFrame ready for loading.

Transformation steps
--------------------
1. Concatenate all per-indicator DataFrames
2. Cast types (year → int, value → float)
3. Drop nulls on critical columns
4. Remove duplicate (country_code, indicator_code, year) combinations
5. Add human-readable indicator label
6. Add metadata columns (loaded_at, source)
7. Validate value ranges by indicator type
"""

import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger("pipeline.transformer")

# Reasonable value bounds per indicator (used for anomaly flagging, not hard rejection)
INDICATOR_BOUNDS: dict[str, tuple[float | None, float | None]] = {
    "FP.CPI.TOTL.ZG":    (-5.0, 5000.0),   # Inflation %: Angola hit ~300% in 2000
    "SL.UEM.TOTL.ZS":    (0.0, 100.0),      # Unemployment %
    "NE.EXP.GNFS.ZS":    (0.0, 200.0),      # Exports % of GDP
    "NE.IMP.GNFS.ZS":    (0.0, 200.0),      # Imports % of GDP
}


class DataTransformer:
    def __init__(self, indicator_labels: dict[str, str] | None = None):
        self.indicator_labels = indicator_labels or {}

    def transform(self, raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Apply all transformation steps and return a clean DataFrame.

        Parameters
        ----------
        raw_data : dict[str, DataFrame]
            Output from WorldBankExtractor.extract()

        Returns
        -------
        DataFrame with columns:
            country_code, country_name, indicator_code, indicator_label,
            year, value, unit, obs_status, is_anomaly, loaded_at, source
        """
        if not raw_data:
            logger.warning("No raw data to transform — returning empty DataFrame")
            return pd.DataFrame()

        # 1. Concatenate
        df = pd.concat(raw_data.values(), ignore_index=True)
        logger.info(f"  Concatenated {len(df):,} rows from {len(raw_data)} indicators")

        # 2. Type casting
        df = self._cast_types(df)

        # 3. Drop nulls on key columns
        before = len(df)
        df = df.dropna(subset=["country_code", "indicator_code", "year"])
        dropped = before - len(df)
        if dropped:
            logger.warning(f"  Dropped {dropped} rows with null key columns")

        # 4. Drop rows where value is null (missing data from API)
        null_values = df["value"].isna().sum()
        df = df.dropna(subset=["value"])
        logger.info(f"  Removed {null_values} rows with null values (expected for some years)")

        # 5. Deduplicate
        df = self._deduplicate(df)

        # 6. Add indicator label
        df["indicator_label"] = df["indicator_code"].map(self.indicator_labels).fillna(df["indicator_code"])

        # 7. Flag anomalies
        df["is_anomaly"] = df.apply(self._flag_anomaly, axis=1)
        anomaly_count = df["is_anomaly"].sum()
        if anomaly_count:
            logger.warning(f"  Flagged {anomaly_count} potential anomalies (not removed)")

        # 8. Metadata
        df["loaded_at"] = datetime.now(timezone.utc).isoformat()
        df["source"] = "World Bank Open Data API"

        # 9. Sort
        df = df.sort_values(["indicator_code", "year"]).reset_index(drop=True)

        logger.info(f"  Transform complete: {len(df):,} clean rows, {df['indicator_code'].nunique()} indicators, years {df['year'].min()}–{df['year'].max()}")

        return df

    # ── Private ───────────────────────────────────────────────────────────────

    @staticmethod
    def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
        """Safe type casting with error coercion."""
        df = df.copy()
        df["year"]  = pd.to_numeric(df["year"],  errors="coerce").astype("Int64")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["decimal"] = pd.to_numeric(df.get("decimal", 0), errors="coerce").fillna(0).astype(int)

        # Normalise string columns
        for col in ["country_code", "country_name", "indicator_code", "unit", "obs_status"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace({"nan": "", "None": ""})

        return df

    @staticmethod
    def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
        """Keep the latest occurrence for any duplicate keys."""
        key_cols = ["country_code", "indicator_code", "year"]
        duplicates = df.duplicated(subset=key_cols, keep=False).sum()
        if duplicates:
            logger.warning(f"  Found {duplicates} duplicate rows — keeping last occurrence")
        return df.drop_duplicates(subset=key_cols, keep="last")

    @staticmethod
    def _flag_anomaly(row: pd.Series) -> bool:
        """Return True if value is outside expected bounds for its indicator."""
        bounds = INDICATOR_BOUNDS.get(row["indicator_code"])
        if bounds is None:
            return False
        lo, hi = bounds
        val = row["value"]
        return (lo is not None and val < lo) or (hi is not None and val > hi)
