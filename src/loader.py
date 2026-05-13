"""
loader.py
=========
Loads clean data into a SQLite database with:
- Schema creation (if not exists)
- Incremental upsert (INSERT OR REPLACE)
- Analytical SQL views created on first load
- Row-level change tracking (inserted / updated / skipped)
"""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

logger = logging.getLogger("pipeline.loader")

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_INDICATORS_TABLE = """
CREATE TABLE IF NOT EXISTS economic_indicators (
    id                INTEGER  PRIMARY KEY AUTOINCREMENT,
    country_code      TEXT     NOT NULL,
    country_name      TEXT,
    indicator_code    TEXT     NOT NULL,
    indicator_label   TEXT,
    year              INTEGER  NOT NULL,
    value             REAL,
    unit              TEXT,
    obs_status        TEXT,
    is_anomaly        INTEGER  DEFAULT 0,   -- 0 = ok, 1 = anomaly flag
    loaded_at         TEXT,
    source            TEXT,
    UNIQUE (country_code, indicator_code, year)
);
"""

CREATE_INDEX_1 = "CREATE INDEX IF NOT EXISTS idx_indicator_year ON economic_indicators (indicator_code, year);"
CREATE_INDEX_2 = "CREATE INDEX IF NOT EXISTS idx_country       ON economic_indicators (country_code);"

# ── Views ─────────────────────────────────────────────────────────────────────

CREATE_VIEW_LATEST = """
CREATE VIEW IF NOT EXISTS v_latest_values AS
SELECT
    country_code,
    country_name,
    indicator_code,
    indicator_label,
    year AS latest_year,
    value,
    unit,
    is_anomaly
FROM economic_indicators
WHERE (country_code, indicator_code, year) IN (
    SELECT country_code, indicator_code, MAX(year)
    FROM economic_indicators
    WHERE value IS NOT NULL
    GROUP BY country_code, indicator_code
);
"""

CREATE_VIEW_YOY = """
CREATE VIEW IF NOT EXISTS v_year_on_year_growth AS
SELECT
    curr.country_code,
    curr.indicator_code,
    curr.indicator_label,
    curr.year,
    curr.value                                                     AS value,
    prev.value                                                     AS prev_value,
    ROUND(((curr.value - prev.value) / ABS(prev.value)) * 100, 2) AS yoy_pct_change,
    curr.is_anomaly
FROM economic_indicators curr
LEFT JOIN economic_indicators prev
       ON curr.country_code   = prev.country_code
      AND curr.indicator_code = prev.indicator_code
      AND curr.year           = prev.year + 1
WHERE curr.value IS NOT NULL;
"""

CREATE_VIEW_ANOMALIES = """
CREATE VIEW IF NOT EXISTS v_anomalies AS
SELECT
    country_code,
    indicator_code,
    indicator_label,
    year,
    value,
    loaded_at
FROM economic_indicators
WHERE is_anomaly = 1
ORDER BY indicator_code, year;
"""

CREATE_VIEW_SUMMARY = """
CREATE VIEW IF NOT EXISTS v_indicator_summary AS
SELECT
    indicator_code,
    indicator_label,
    COUNT(*)                    AS total_records,
    MIN(year)                   AS first_year,
    MAX(year)                   AS last_year,
    ROUND(AVG(value), 4)        AS avg_value,
    ROUND(MIN(value), 4)        AS min_value,
    ROUND(MAX(value), 4)        AS max_value,
    SUM(is_anomaly)             AS anomaly_count
FROM economic_indicators
WHERE value IS NOT NULL
GROUP BY indicator_code, indicator_label
ORDER BY indicator_code;
"""

ALL_DDL = [
    CREATE_INDICATORS_TABLE,
    CREATE_INDEX_1,
    CREATE_INDEX_2,
    CREATE_VIEW_LATEST,
    CREATE_VIEW_YOY,
    CREATE_VIEW_ANOMALIES,
    CREATE_VIEW_SUMMARY,
]


class DatabaseLoader:
    def __init__(self, db_path: str = "data/angola_economics.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ── Public ────────────────────────────────────────────────────────────────

    def load(self, df: pd.DataFrame) -> tuple[int, int, int]:
        """
        Incrementally upsert rows into economic_indicators.

        Returns
        -------
        (inserted, updated, skipped) counts
        """
        if df.empty:
            logger.warning("Empty DataFrame — nothing to load")
            return 0, 0, 0

        columns = [
            "country_code", "country_name", "indicator_code", "indicator_label",
            "year", "value", "unit", "obs_status", "is_anomaly", "loaded_at", "source",
        ]
        df_to_load = df[[c for c in columns if c in df.columns]].copy()
        df_to_load["is_anomaly"] = df_to_load["is_anomaly"].astype(int)

        inserted = updated = skipped = 0

        with sqlite3.connect(self.db_path) as conn:
            existing = self._get_existing_keys(conn)

            for _, row in df_to_load.iterrows():
                key = (row["country_code"], row["indicator_code"], int(row["year"]))
                if key in existing:
                    # Check if value changed
                    old_val = existing[key]
                    if old_val == row["value"]:
                        skipped += 1
                        continue
                    updated += 1
                else:
                    inserted += 1

            # Bulk upsert
            records = [tuple(row) for _, row in df_to_load.iterrows()]
            placeholders = ", ".join(["?"] * len(df_to_load.columns))
            sql = f"INSERT OR REPLACE INTO economic_indicators ({', '.join(df_to_load.columns)}) VALUES ({placeholders})"
            conn.executemany(sql, records)
            conn.commit()

        logger.info(f"  DB: {self.db_path}")
        return inserted, updated, skipped

    def query(self, sql: str) -> pd.DataFrame:
        """Run an arbitrary SQL query and return results as a DataFrame."""
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(sql, conn)

    # ── Private ───────────────────────────────────────────────────────────────

    def _init_schema(self) -> None:
        """Create tables, indexes, and views if they do not exist."""
        with sqlite3.connect(self.db_path) as conn:
            for ddl in ALL_DDL:
                conn.execute(ddl)
            conn.commit()
        logger.info(f"  Schema ready: {self.db_path}")

    @staticmethod
    def _get_existing_keys(conn: sqlite3.Connection) -> dict:
        """Return {(country_code, indicator_code, year): value} for existing rows."""
        cursor = conn.execute(
            "SELECT country_code, indicator_code, year, value FROM economic_indicators"
        )
        return {(r[0], r[1], r[2]): r[3] for r in cursor.fetchall()}
