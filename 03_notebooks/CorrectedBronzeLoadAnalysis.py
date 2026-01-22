import marimo

__generated_with = "0.19.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # EDA of Bronze Layer - Corrected Load

    This notebook analyzes the bronze layer data after fixing the date parsing issue.

    **Load Date:** 2025-01-22

    **Fix Applied:** Changed date parsing from `TRY_CAST(... AS DATE)` to `TRY_STRPTIME(..., '%Y%m%d')::DATE`

    ## Goals
    - Verify date fields (`period`, `filed`, `changed`) are correctly populated
    - Review data quality logs for any remaining issues
    - Understand data distribution and patterns
    - Identify any edge cases or data quality concerns
    """)
    return


@app.cell
def _():
    import duckdb
    from pathlib import Path

    # Use relative path from project root
    project_root = Path(__file__).parent.parent
    database_path = project_root / "01_data" / "01_sampleData" / "02_bronze" / "secSampleData_bronze.duckdb"

    engine = duckdb.connect(str(database_path), read_only=True)
    return (engine,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Verify Date Fields Are Correctly Loaded
    """)
    return


@app.cell
def _(bronze_sub, engine, mo):
    date_check = mo.sql(
        f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(period) as period_not_null,
            COUNT(filed) as filed_not_null,
            COUNT(changed) as changed_not_null,
            COUNT(*) - COUNT(period) as period_null_count,
            COUNT(*) - COUNT(filed) as filed_null_count,
            COUNT(*) - COUNT(changed) as changed_null_count
        FROM bronze_sub
        """,
        engine=engine
    )
    return


@app.cell
def _(bronze_sub, engine, mo):
    sample_dates = mo.sql(
        f"""
        SELECT 
            adsh,
            name,
            period,
            filed,
            changed,
            accepted
        FROM bronze_sub
        LIMIT 10
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Data Quality Log Review
    """)
    return


@app.cell
def _(engine, mo):
    data_quality_log = mo.sql(
        f"""
        SELECT 
            field_name,
            check_type,
            severity,
            issue_count,
            total_records,
            issue_percentage,
            check_passed,
            error_details
        FROM data_quality_log
        ORDER BY severity DESC, issue_count DESC
        """,
        engine=engine
    )
    return (data_quality_log,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Bronze Sub Table Overview
    """)
    return


@app.cell
def _(bronze_sub, engine, mo):
    bronze_sub_summary = mo.sql(
        f"""
        SELECT 
            COUNT(*) as total_submissions,
            COUNT(DISTINCT cik) as unique_companies,
            COUNT(DISTINCT form) as unique_forms,
            MIN(period) as earliest_period,
            MAX(period) as latest_period,
            MIN(filed) as earliest_filed,
            MAX(filed) as latest_filed
        FROM bronze_sub
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Companies with Multiple Submissions
    """)
    return


@app.cell
def _(bronze_sub, engine, mo):
    multi_submission_companies = mo.sql(
        f"""
        SELECT 
            name,
            cik,
            COUNT(*) as submission_count
        FROM bronze_sub
        GROUP BY name, cik
        HAVING COUNT(*) > 1
        ORDER BY submission_count DESC
        LIMIT 20
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Forms Distribution
    """)
    return


@app.cell
def _(bronze_sub, engine, mo):
    forms_distribution = mo.sql(
        f"""
        SELECT 
            form,
            COUNT(*) as count,
            COUNT(DISTINCT cik) as unique_companies
        FROM bronze_sub
        GROUP BY form
        ORDER BY count DESC
        """,
        engine=engine
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
