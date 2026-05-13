-- ============================================================
-- Angola Economic ETL Pipeline — Analytical SQL Queries
-- Database: SQLite  |  Table: economic_indicators
-- ============================================================

-- ── 1. Latest value per indicator ──────────────────────────
SELECT *
FROM v_latest_values
ORDER BY indicator_code;


-- ── 2. GDP trend for Angola (2000–2024) ───────────────────
SELECT
    year,
    ROUND(value / 1e9, 2)   AS gdp_billion_usd,
    yoy_pct_change          AS gdp_yoy_growth_pct
FROM v_year_on_year_growth
WHERE indicator_code = 'NY.GDP.MKTP.CD'
ORDER BY year;


-- ── 3. Inflation history ───────────────────────────────────
SELECT
    year,
    ROUND(value, 2) AS inflation_pct,
    CASE
        WHEN value > 50  THEN 'High'
        WHEN value > 10  THEN 'Moderate'
        WHEN value >= 0  THEN 'Stable'
        ELSE 'Deflation'
    END AS inflation_category
FROM economic_indicators
WHERE indicator_code = 'FP.CPI.TOTL.ZG'
ORDER BY year;


-- ── 4. FDI vs GDP growth comparison ───────────────────────
SELECT
    g.year,
    ROUND(g.value / 1e9, 2)   AS gdp_billion_usd,
    ROUND(g.yoy_pct_change, 2) AS gdp_growth_pct,
    ROUND(f.value / 1e9, 2)   AS fdi_billion_usd
FROM v_year_on_year_growth g
JOIN economic_indicators f
  ON f.indicator_code = 'BX.KLT.DINV.CD.WD'
 AND f.year = g.year
WHERE g.indicator_code = 'NY.GDP.MKTP.CD'
ORDER BY g.year;


-- ── 5. Trade balance (Exports - Imports as % of GDP) ──────
SELECT
    e.year,
    ROUND(e.value, 2)            AS exports_pct_gdp,
    ROUND(i.value, 2)            AS imports_pct_gdp,
    ROUND(e.value - i.value, 2)  AS trade_balance_pct_gdp
FROM economic_indicators e
JOIN economic_indicators i
  ON i.indicator_code = 'NE.IMP.GNFS.ZS'
 AND i.year = e.year
WHERE e.indicator_code = 'NE.EXP.GNFS.ZS'
ORDER BY e.year;


-- ── 6. Decade summary — average values per indicator ──────
SELECT
    indicator_label,
    CASE
        WHEN year BETWEEN 2000 AND 2009 THEN '2000s'
        WHEN year BETWEEN 2010 AND 2019 THEN '2010s'
        WHEN year BETWEEN 2020 AND 2024 THEN '2020s'
    END                                    AS decade,
    ROUND(AVG(value), 2)                   AS avg_value,
    COUNT(*)                               AS data_points
FROM economic_indicators
WHERE value IS NOT NULL
  AND year IS NOT NULL
GROUP BY indicator_label, decade
ORDER BY indicator_label, decade;


-- ── 7. Data quality reconciliation ────────────────────────
-- Expected 25 years × 8 indicators = 200 data points max
-- This query shows completeness per indicator
SELECT
    indicator_code,
    indicator_label,
    total_records,
    first_year,
    last_year,
    ROUND(avg_value, 2) AS avg_value,
    anomaly_count,
    ROUND(total_records * 100.0 / 25, 1) AS completeness_pct
FROM v_indicator_summary;


-- ── 8. Stored procedure equivalent (SQLite trigger) ───────
-- SQLite doesn't have stored procedures; this is the
-- equivalent logic as a query for audit logging
-- (In SQL Server you would create this as a SP)
SELECT
    'RECONCILIATION REPORT'        AS report_type,
    COUNT(*)                       AS total_rows,
    COUNT(DISTINCT indicator_code) AS indicators,
    COUNT(DISTINCT year)           AS years_covered,
    SUM(CASE WHEN value IS NULL   THEN 1 ELSE 0 END) AS null_values,
    SUM(CASE WHEN is_anomaly = 1  THEN 1 ELSE 0 END) AS anomalies,
    MIN(loaded_at)                 AS first_loaded,
    MAX(loaded_at)                 AS last_loaded
FROM economic_indicators;
