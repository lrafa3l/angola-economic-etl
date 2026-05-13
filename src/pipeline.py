"""
Angola Economic ETL Pipeline
=============================
Orchestrates the full ETL process:
  1. Extract  — World Bank API
  2. Transform — clean, type-cast, normalise with Pandas
  3. Load      — incremental upsert into SQLite
  4. Validate  — data quality report

Usage:
    python -m src.pipeline
    python -m src.pipeline --indicators NY.GDP.MKTP.CD BX.KLT.DINV.CD.WD
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from src.extractor import WorldBankExtractor
from src.transformer import DataTransformer
from src.loader import DatabaseLoader
from src.quality import DataQualityChecker

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_DIR = Path("data/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / f"pipeline_{datetime.now():%Y%m%d}.log"),
    ],
)
logger = logging.getLogger("pipeline")

# ── Default indicators ────────────────────────────────────────────────────────
DEFAULT_INDICATORS = {
    "NY.GDP.MKTP.CD":     "GDP (USD current)",
    "FP.CPI.TOTL.ZG":     "Inflation rate (%)",
    "BN.CAB.XOKA.CD":     "Current account balance (USD)",
    "BX.KLT.DINV.CD.WD":  "Foreign direct investment (USD)",
    "SL.UEM.TOTL.ZS":     "Unemployment rate (%)",
    "NY.GDP.PCAP.CD":     "GDP per capita (USD)",
    "NE.EXP.GNFS.ZS":     "Exports (% of GDP)",
    "NE.IMP.GNFS.ZS":     "Imports (% of GDP)",
}


def run(indicators: dict | None = None) -> None:
    indicators = indicators or DEFAULT_INDICATORS
    start = datetime.now()
    logger.info("=" * 60)
    logger.info("ANGOLA ECONOMIC ETL PIPELINE — START")
    logger.info(f"Indicators: {len(indicators)}  |  Run: {start:%Y-%m-%d %H:%M:%S}")
    logger.info("=" * 60)

    # 1 ── EXTRACT ─────────────────────────────────────────────────────────────
    logger.info("[1/4] EXTRACT — World Bank API")
    extractor = WorldBankExtractor(country="AGO", start_year=2000, end_year=2024)
    raw_data = extractor.extract(list(indicators.keys()))
    logger.info(f"  Rows fetched: {sum(len(df) for df in raw_data.values()):,}")

    # 2 ── TRANSFORM ───────────────────────────────────────────────────────────
    logger.info("[2/4] TRANSFORM — clean, normalise, enrich")
    transformer = DataTransformer(indicator_labels=indicators)
    clean_df = transformer.transform(raw_data)
    logger.info(f"  Rows after transform: {len(clean_df):,}")
    logger.info(f"  Columns: {list(clean_df.columns)}")

    # 3 ── LOAD ────────────────────────────────────────────────────────────────
    logger.info("[3/4] LOAD — incremental upsert into SQLite")
    loader = DatabaseLoader(db_path="data/angola_economics.db")
    inserted, updated, skipped = loader.load(clean_df)
    logger.info(f"  Inserted: {inserted}  |  Updated: {updated}  |  Skipped: {skipped}")

    # 4 ── QUALITY CHECK ───────────────────────────────────────────────────────
    logger.info("[4/4] VALIDATE — data quality report")
    checker = DataQualityChecker(db_path="data/angola_economics.db")
    report = checker.run_checks()
    checker.print_report(report)
    checker.save_report(report)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE — {elapsed:.1f}s")
    logger.info("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Angola Economic ETL Pipeline")
    parser.add_argument(
        "--indicators",
        nargs="+",
        help="World Bank indicator codes (space-separated). Defaults to 8 macro indicators.",
    )
    args = parser.parse_args()

    if args.indicators:
        indicators = {code: code for code in args.indicators}
    else:
        indicators = DEFAULT_INDICATORS

    run(indicators)


if __name__ == "__main__":
    main()
