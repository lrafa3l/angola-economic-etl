"""
extractor.py
============
Extracts economic data from the World Bank Open Data API.

Endpoint:  https://api.worldbank.org/v2/country/{country}/indicator/{indicator}
Docs:      https://datahelpdesk.worldbank.org/knowledgebase/articles/898581
"""

import logging
import time
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger("pipeline.extractor")

BASE_URL = "https://api.worldbank.org/v2"
REQUEST_TIMEOUT = 15  # seconds
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2   # seconds between retries


class WorldBankExtractor:
    """
    Fetches one or more World Bank indicators for a single country
    over a given date range, returning a dict of {indicator_code: DataFrame}.
    """

    def __init__(self, country: str = "AGO", start_year: int = 2000, end_year: int = 2024):
        self.country = country
        self.start_year = start_year
        self.end_year = end_year
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    # ── Public ────────────────────────────────────────────────────────────────

    def extract(self, indicator_codes: list[str]) -> dict[str, pd.DataFrame]:
        """
        Extract all requested indicators.

        Returns
        -------
        dict[str, DataFrame]
            Keys are indicator codes; values are raw DataFrames with columns:
            [year, value, indicator_code, country_code]
        """
        results: dict[str, pd.DataFrame] = {}

        for code in indicator_codes:
            logger.info(f"  Fetching indicator: {code}")
            try:
                df = self._fetch_indicator(code)
                if df is not None and not df.empty:
                    results[code] = df
                    logger.info(f"  ✓ {code}: {len(df)} records")
                else:
                    logger.warning(f"  ✗ {code}: no data returned")
            except Exception as exc:
                logger.error(f"  ✗ {code}: failed — {exc}")

        return results

    # ── Private ───────────────────────────────────────────────────────────────

    def _fetch_indicator(self, indicator_code: str) -> pd.DataFrame | None:
        """Fetch a single indicator with pagination and retry logic."""
        url = (
            f"{BASE_URL}/country/{self.country}/indicator/{indicator_code}"
            f"?date={self.start_year}:{self.end_year}&format=json&per_page=500"
        )

        all_records: list[dict[str, Any]] = []
        page = 1

        while True:
            paged_url = f"{url}&page={page}"
            data = self._get_with_retry(paged_url)
            if data is None:
                break

            # World Bank JSON: [metadata, data_array]
            if not isinstance(data, list) or len(data) < 2:
                logger.warning(f"Unexpected response structure for {indicator_code}")
                break

            metadata, records = data[0], data[1]
            if not records:
                break

            all_records.extend(records)

            total_pages = metadata.get("pages", 1)
            if page >= total_pages:
                break
            page += 1

        if not all_records:
            return None

        return self._records_to_dataframe(all_records, indicator_code)

    def _get_with_retry(self, url: str) -> Any:
        """HTTP GET with exponential-backoff retries."""
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                logger.warning(f"  Timeout on attempt {attempt}/{RETRY_ATTEMPTS}")
            except requests.exceptions.HTTPError as exc:
                logger.error(f"  HTTP error: {exc}")
                return None
            except requests.exceptions.RequestException as exc:
                logger.warning(f"  Request error on attempt {attempt}: {exc}")

            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF * attempt)

        return None

    @staticmethod
    def _records_to_dataframe(records: list[dict], indicator_code: str) -> pd.DataFrame:
        """Convert raw API records to a tidy DataFrame."""
        rows = []
        for rec in records:
            rows.append({
                "country_code":    rec.get("countryiso3code") or rec.get("country", {}).get("id", ""),
                "country_name":    rec.get("country", {}).get("value", ""),
                "indicator_code":  indicator_code,
                "year":            rec.get("date"),
                "value":           rec.get("value"),
                "unit":            rec.get("unit", ""),
                "obs_status":      rec.get("obs_status", ""),
                "decimal":         rec.get("decimal", 0),
            })
        return pd.DataFrame(rows)
