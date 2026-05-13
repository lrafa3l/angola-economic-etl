"""
tests/test_transformer.py
=========================
Unit tests for the DataTransformer module.
Run with:  python -m pytest tests/ -v
"""

import pandas as pd
import pytest

from src.transformer import DataTransformer


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_raw_data():
    """Simulate raw output from WorldBankExtractor."""
    gdp_df = pd.DataFrame([
        {"country_code": "AGO", "country_name": "Angola", "indicator_code": "NY.GDP.MKTP.CD",
         "year": "2020", "value": 6.28e10, "unit": "USD", "obs_status": "", "decimal": 0},
        {"country_code": "AGO", "country_name": "Angola", "indicator_code": "NY.GDP.MKTP.CD",
         "year": "2021", "value": 7.41e10, "unit": "USD", "obs_status": "", "decimal": 0},
        {"country_code": "AGO", "country_name": "Angola", "indicator_code": "NY.GDP.MKTP.CD",
         "year": "2022", "value": None,    "unit": "USD", "obs_status": "", "decimal": 0},  # null value
    ])
    inflation_df = pd.DataFrame([
        {"country_code": "AGO", "country_name": "Angola", "indicator_code": "FP.CPI.TOTL.ZG",
         "year": "2020", "value": 22.3, "unit": "%", "obs_status": "", "decimal": 1},
        {"country_code": "AGO", "country_name": "Angola", "indicator_code": "FP.CPI.TOTL.ZG",
         "year": "2021", "value": 25.8, "unit": "%", "obs_status": "", "decimal": 1},
    ])
    return {"NY.GDP.MKTP.CD": gdp_df, "FP.CPI.TOTL.ZG": inflation_df}


@pytest.fixture
def transformer():
    return DataTransformer(indicator_labels={
        "NY.GDP.MKTP.CD": "GDP (USD current)",
        "FP.CPI.TOTL.ZG": "Inflation rate (%)",
    })


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestDataTransformer:

    def test_returns_dataframe(self, transformer, sample_raw_data):
        result = transformer.transform(sample_raw_data)
        assert isinstance(result, pd.DataFrame)

    def test_null_values_removed(self, transformer, sample_raw_data):
        """Rows where value is null should be dropped."""
        result = transformer.transform(sample_raw_data)
        assert result["value"].isna().sum() == 0

    def test_year_cast_to_integer(self, transformer, sample_raw_data):
        result = transformer.transform(sample_raw_data)
        assert pd.api.types.is_integer_dtype(result["year"])

    def test_value_cast_to_float(self, transformer, sample_raw_data):
        result = transformer.transform(sample_raw_data)
        assert pd.api.types.is_float_dtype(result["value"])

    def test_indicator_label_applied(self, transformer, sample_raw_data):
        result = transformer.transform(sample_raw_data)
        assert "GDP (USD current)" in result["indicator_label"].values
        assert "Inflation rate (%)" in result["indicator_label"].values

    def test_is_anomaly_column_exists(self, transformer, sample_raw_data):
        result = transformer.transform(sample_raw_data)
        assert "is_anomaly" in result.columns

    def test_no_duplicates(self, transformer, sample_raw_data):
        # Inject duplicate
        dup = sample_raw_data["NY.GDP.MKTP.CD"].copy()
        sample_raw_data["NY.GDP.MKTP.CD"] = pd.concat([
            sample_raw_data["NY.GDP.MKTP.CD"], dup.iloc[[0]]
        ], ignore_index=True)

        result = transformer.transform(sample_raw_data)
        dupes = result.duplicated(subset=["country_code", "indicator_code", "year"]).sum()
        assert dupes == 0

    def test_empty_input_returns_empty_dataframe(self, transformer):
        result = transformer.transform({})
        assert result.empty

    def test_loaded_at_column_exists(self, transformer, sample_raw_data):
        result = transformer.transform(sample_raw_data)
        assert "loaded_at" in result.columns

    def test_source_column_set(self, transformer, sample_raw_data):
        result = transformer.transform(sample_raw_data)
        assert (result["source"] == "World Bank Open Data API").all()
