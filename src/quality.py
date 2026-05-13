"""
quality.py
==========
Runs automated data quality checks against the loaded SQLite database
and produces a structured report.

Checks
------
- Completeness  : % of non-null values per indicator
- Freshness     : latest year available vs current year
- Anomalies     : rows flagged by transformer
- Duplicates    : duplicate keys remaining in DB
- Coverage      : year range completeness (gap detection)
"""

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger("pipeline.quality")


class DataQualityChecker:
    def __init__(self, db_path: str = "data/angola_economics.db"):
        self.db_path = Path(db_path)

    def run_checks(self) -> dict:
        """Run all checks and return a structured report dict."""
        report = {
            "run_at": datetime.utcnow().isoformat(),
            "db_path": str(self.db_path),
            "checks": {},
        }

        with sqlite3.connect(self.db_path) as conn:
            report["checks"]["row_count"]    = self._check_row_count(conn)
            report["checks"]["completeness"] = self._check_completeness(conn)
            report["checks"]["freshness"]    = self._check_freshness(conn)
            report["checks"]["anomalies"]    = self._check_anomalies(conn)
            report["checks"]["duplicates"]   = self._check_duplicates(conn)
            report["checks"]["year_gaps"]    = self._check_year_gaps(conn)

        # Overall pass/fail
        issues = [
            v for k, v in report["checks"].items()
            if isinstance(v, dict) and v.get("status") == "FAIL"
        ]
        report["overall_status"] = "FAIL" if issues else "PASS"
        report["issues_count"]   = len(issues)
        return report

    def print_report(self, report: dict) -> None:
        """Print a human-readable summary to stdout/log."""
        status_icon = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️"}.get
        logger.info("─" * 50)
        logger.info(f"DATA QUALITY REPORT  [{report['overall_status']}]")
        logger.info(f"Run: {report['run_at']}")
        logger.info("─" * 50)
        for name, result in report["checks"].items():
            if isinstance(result, dict):
                icon = status_icon(result.get("status", ""), "ℹ️")
                msg  = result.get("message", "")
                logger.info(f"  {icon}  {name:<20} {msg}")
        logger.info("─" * 50)

    def save_report(self, report: dict) -> None:
        """Save report as JSON in data/reports/."""
        report_dir = Path("data/reports")
        report_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = report_dir / f"quality_report_{ts}.json"
        path.write_text(json.dumps(report, indent=2))
        logger.info(f"  Quality report saved: {path}")

    # ── Checks ────────────────────────────────────────────────────────────────

    @staticmethod
    def _check_row_count(conn: sqlite3.Connection) -> dict:
        count = conn.execute("SELECT COUNT(*) FROM economic_indicators").fetchone()[0]
        return {
            "status":  "PASS" if count > 0 else "FAIL",
            "message": f"{count:,} rows in economic_indicators",
            "value":   count,
        }

    @staticmethod
    def _check_completeness(conn: sqlite3.Connection) -> dict:
        df = pd.read_sql_query(
            """
            SELECT
                indicator_code,
                COUNT(*)                                           AS total,
                SUM(CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END) AS non_null
            FROM economic_indicators
            GROUP BY indicator_code
            """,
            conn,
        )
        if df.empty:
            return {"status": "FAIL", "message": "No data"}

        df["pct"] = (df["non_null"] / df["total"] * 100).round(1)
        low = df[df["pct"] < 80]
        status = "WARN" if not low.empty else "PASS"
        msg = (
            f"All indicators ≥80% complete"
            if low.empty
            else f"{len(low)} indicator(s) <80% complete: {low['indicator_code'].tolist()}"
        )
        return {"status": status, "message": msg, "detail": df.to_dict(orient="records")}

    @staticmethod
    def _check_freshness(conn: sqlite3.Connection) -> dict:
        current_year = datetime.now().year
        df = pd.read_sql_query(
            "SELECT indicator_code, MAX(year) AS latest_year FROM economic_indicators GROUP BY indicator_code",
            conn,
        )
        stale = df[df["latest_year"] < current_year - 3]
        status = "WARN" if not stale.empty else "PASS"
        msg = (
            f"All indicators have data up to {df['latest_year'].max()}"
            if stale.empty
            else f"{len(stale)} indicator(s) stale (>3 years old)"
        )
        return {"status": status, "message": msg, "detail": df.to_dict(orient="records")}

    @staticmethod
    def _check_anomalies(conn: sqlite3.Connection) -> dict:
        count = conn.execute("SELECT COUNT(*) FROM economic_indicators WHERE is_anomaly = 1").fetchone()[0]
        status = "WARN" if count > 0 else "PASS"
        return {
            "status":  status,
            "message": f"{count} anomalous values flagged",
            "value":   count,
        }

    @staticmethod
    def _check_duplicates(conn: sqlite3.Connection) -> dict:
        count = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT country_code, indicator_code, year, COUNT(*) AS cnt
                FROM economic_indicators
                GROUP BY country_code, indicator_code, year
                HAVING cnt > 1
            )
            """
        ).fetchone()[0]
        status = "FAIL" if count > 0 else "PASS"
        return {
            "status":  status,
            "message": f"{count} duplicate key combinations found",
            "value":   count,
        }

    @staticmethod
    def _check_year_gaps(conn: sqlite3.Connection) -> dict:
        """Detect indicators with gaps of more than 2 consecutive missing years."""
        df = pd.read_sql_query(
            "SELECT indicator_code, year FROM economic_indicators WHERE value IS NOT NULL ORDER BY indicator_code, year",
            conn,
        )
        gaps = []
        for code, group in df.groupby("indicator_code"):
            years = sorted(group["year"].tolist())
            for i in range(1, len(years)):
                gap = years[i] - years[i - 1]
                if gap > 2:
                    gaps.append({"indicator": code, "from": years[i - 1], "to": years[i], "gap": gap})

        status = "WARN" if gaps else "PASS"
        return {
            "status":  status,
            "message": f"{len(gaps)} year gap(s) > 2 years detected",
            "detail":  gaps,
        }
