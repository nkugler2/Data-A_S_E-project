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
    # Base Tables
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Bronze_sub
    """)
    return


@app.cell
def _(bronze_sub, engine, mo):
    _df = mo.sql(
        f"""
        select * from bronze_sub
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Data_quality_log
    """)
    return


@app.cell
def _(data_quality_log, engine, mo):
    _df = mo.sql(
        f"""
        select * from data_quality_log
        """,
        engine=engine
    )
    return


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
    database_path = (
        project_root
        / "01_data"
        / "01_sampleData"
        / "02_bronze"
        / "secSampleData_bronze.duckdb"
    )

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
    ## Investigating Name vs CIK Discrepancy

    The difference between distinct names and distinct CIKs can occur in two scenarios:
    1. **Same name, different CIKs**: Multiple companies share the same name (different legal entities)
    2. **Same CIK, different names**: A company changed its name over time
    """)
    return


@app.cell
def _(bronze_sub, engine, mo):
    name_cik_summary = mo.sql(
        f"""
        SELECT 
            COUNT(DISTINCT name) as distinct_names,
            COUNT(DISTINCT cik) as distinct_ciks,
            COUNT(DISTINCT name) - COUNT(DISTINCT cik) as name_minus_cik,
            COUNT(DISTINCT cik) - COUNT(DISTINCT name) as cik_minus_name
        FROM bronze_sub
        """,
        engine=engine
    )
    return


@app.cell
def _(bronze_sub, engine, mo):
    names_with_multiple_ciks = mo.sql(
        f"""
        SELECT 
            name,
            COUNT(DISTINCT cik) as cik_count,
            STRING_AGG(DISTINCT CAST(cik AS VARCHAR), ', ' ORDER BY CAST(cik AS VARCHAR)) as cik_list,
            COUNT(*) as total_submissions
        FROM bronze_sub
        GROUP BY name
        HAVING COUNT(DISTINCT cik) > 1
        ORDER BY cik_count DESC, name
        """,
        engine=engine
    )
    return


@app.cell
def _(bronze_sub, engine, mo):
    ciks_with_multiple_names = mo.sql(
        f"""
        SELECT 
            cik,
            COUNT(DISTINCT name) as name_count,
            STRING_AGG(DISTINCT name, ' | ' ORDER BY name) as name_list,
            COUNT(*) as total_submissions,
            MIN(filed) as earliest_filing,
            MAX(filed) as latest_filing
        FROM bronze_sub
        GROUP BY cik
        HAVING COUNT(DISTINCT name) > 1
        ORDER BY name_count DESC, cik
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Detailed Examples: Names with Multiple CIKs

    Let's see specific examples of companies that share the same name but have different CIKs:
    """)
    return


@app.cell
def _(bronze_sub, engine, mo):
    name_cik_details = mo.sql(
        f"""
        SELECT 
            name,
            cik,
            COUNT(*) as submission_count,
            MIN(filed) as earliest_filing,
            MAX(filed) as latest_filing,
            STRING_AGG(DISTINCT form, ', ' ORDER BY form) as forms_filed
        FROM bronze_sub
        WHERE name IN (
            SELECT name 
            FROM bronze_sub
            GROUP BY name
            HAVING COUNT(DISTINCT cik) > 1
        )
        GROUP BY name, cik
        ORDER BY name, cik
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Detailed Examples: CIKs with Multiple Names

    Let's see specific examples of CIKs that have used different names over time:
    """)
    return


@app.cell
def _(bronze_sub, engine, mo):
    cik_name_details = mo.sql(
        f"""
        SELECT 
            cik,
            name,
            COUNT(*) as submission_count,
            MIN(filed) as earliest_filing,
            MAX(filed) as latest_filing,
            STRING_AGG(DISTINCT form, ', ' ORDER BY form) as forms_filed
        FROM bronze_sub
        WHERE cik IN (
            SELECT cik 
            FROM bronze_sub
            GROUP BY cik
            HAVING COUNT(DISTINCT name) > 1
        )
        GROUP BY cik, name
        ORDER BY cik, earliest_filing
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
