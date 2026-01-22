# Changelog

## 2025-01-22 - Fixed Date Parsing in Bronze Loader

### Issue
The `period`, `filed`, and `changed` date fields were all NULL after loading into the bronze layer, despite having valid data in the source files.

### Root Cause
SEC EDGAR data stores dates in `YYYYMMDD` format (e.g., `20241231`) without separators. DuckDB's `TRY_CAST(column AS DATE)` expects ISO format dates with dashes (`YYYY-MM-DD`), causing all date conversions to fail silently and return NULL.

### Fix
Changed date parsing from:
```sql
TRY_CAST(period AS DATE)
```

To:
```sql
TRY_STRPTIME(period, '%Y%m%d')::DATE
```

This explicitly tells DuckDB to parse the date using the `YYYYMMDD` format string.

### Files Changed
- `02_src/01_extract/sampleDataLoadBronze.py`
  - Updated `period` field parsing (line ~296)
  - Updated `filed` field parsing (line ~299)
  - Updated `changed` field parsing (line ~291)

### Impact
- All date fields now correctly parse and store as DATE type
- Data quality logs will show successful date conversions
- Previous bronze database was deleted and reloaded with corrected data

### Notes
- Old exploratory notebook archived as `InitBronzeLoadAnalysis_BUGGY_DATE_PARSING_2025-01-22.py`
- HTML export of old notebook preserved for reference
