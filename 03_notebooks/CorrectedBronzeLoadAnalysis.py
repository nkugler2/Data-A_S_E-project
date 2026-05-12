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
    ## Bronze_num
    """)
    return


@app.cell
def _(bronze_num, engine, mo):
    _df = mo.sql(
        f"""
        select * from bronze_num limit 100
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
def _(mo):
    mo.md(r"""
    # EDA of Bronze_tag

    tag.txt is a reference/dimension table — one row per unique (tag, version) concept.
    Key columns:
    - **tag / version**: XBRL concept name and taxonomy (e.g., us-gaap/2024)
    - **custom**: 1 = company-defined tag, 0 = standard taxonomy tag
    - **abstract**: 1 = grouping label only (no value attached), 0 = reportable fact
    - **datatype**: value type (monetary, shares, string, etc.) — NULL for abstract tags
    - **iord**: I = instant (point-in-time), D = duration — NULL for abstract tags
    - **crdr**: C = credit-normal, D = debit-normal — NULL for non-monetary tags
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Bronze_tag Table Overview
    """)
    return


@app.cell
def _(bronze_tag, engine, mo):
    bronze_tag_summary = mo.sql(
        f"""
        SELECT
            COUNT(*) as total_tags,
            COUNT(DISTINCT tag) as unique_tag_names,
            COUNT(DISTINCT version) as unique_versions,
            SUM(CASE WHEN custom THEN 1 ELSE 0 END) as custom_tags,
            SUM(CASE WHEN abstract THEN 1 ELSE 0 END) as abstract_tags,
            SUM(CASE WHEN NOT abstract AND datatype IS NULL THEN 1 ELSE 0 END) as non_abstract_missing_datatype
        FROM bronze_tag
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Datatype Distribution
    """)
    return


@app.cell
def _(bronze_tag, engine, mo):
    tag_datatype_dist = mo.sql(
        f"""
        SELECT
            datatype,
            COUNT(*) as tag_count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_total
        FROM bronze_tag
        GROUP BY datatype
        ORDER BY tag_count DESC
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Credit/Debit Distribution
    """)
    return


@app.cell
def _(bronze_tag, engine, mo):
    tag_crdr_dist = mo.sql(
        f"""
        SELECT
            crdr,
            COUNT(*) as tag_count
        FROM bronze_tag
        GROUP BY crdr
        ORDER BY tag_count DESC
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Data Quality Log — bronze_tag
    """)
    return


@app.cell
def _(engine, mo):
    tag_quality_log = mo.sql(
        f"""
        SELECT
            field_name,
            check_category,
            check_type,
            severity,
            issue_count,
            total_records,
            issue_percentage,
            check_passed,
            error_details
        FROM data_quality_log
        WHERE table_name = 'bronze_tag'
        ORDER BY severity DESC, issue_count DESC
        """,
        engine=engine
    )
    return (tag_quality_log,)


@app.cell
def _(mo):
    mo.md(r"""
    # EDA of Bronze_pre

    pre.txt is the presentation linkbase — it maps each filing's financial statements to their
    display structure (which tags appear in which statement, in what order).
    Key columns:
    - **adsh**: accession number — ties back to bronze_sub
    - **report**: report number within the filing
    - **line**: line number within the report
    - **stmt**: statement type — BS (balance sheet), IS (income), CF (cash flow), EQ (equity), CI (comprehensive income), UN (unlabeled)
    - **inpth**: displayed in parentheses (negative presentation)
    - **rfile**: R = XBRL report, H = HTML report
    - **tag / version**: the XBRL concept — ties back to bronze_tag
    - **negating**: whether to flip the sign for display
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Bronze_pre Table Overview
    """)
    return


@app.cell
def _(bronze_pre, engine, mo):
    bronze_pre_summary = mo.sql(
        f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT adsh) as unique_filings,
            COUNT(DISTINCT stmt) as unique_stmt_types,
            COUNT(DISTINCT tag) as unique_tags,
            MIN(report) as min_report,
            MAX(report) as max_report
        FROM bronze_pre
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Statement Type Distribution

    Expect BS, IS, CF, EQ, CI, UN — this confirms all statement types are present.
    """)
    return


@app.cell
def _(bronze_pre, engine, mo):
    pre_stmt_dist = mo.sql(
        f"""
        SELECT
            stmt,
            COUNT(*) as row_count,
            COUNT(DISTINCT adsh) as filing_count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_total
        FROM bronze_pre
        GROUP BY stmt
        ORDER BY row_count DESC
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Referential Integrity: pre → sub and pre → tag
    """)
    return


@app.cell
def _(bronze_pre, bronze_sub, engine, mo):
    pre_sub_integrity = mo.sql(
        f"""
        SELECT
            COUNT(*) as total_pre_rows,
            COUNT(CASE WHEN s.adsh IS NOT NULL THEN 1 END) as matched_to_sub,
            COUNT(CASE WHEN s.adsh IS NULL THEN 1 END) as unmatched_to_sub
        FROM bronze_pre p
        LEFT JOIN bronze_sub s ON p.adsh = s.adsh
        """,
        engine=engine
    )
    return


@app.cell
def _(bronze_pre, bronze_tag, engine, mo):
    pre_tag_integrity = mo.sql(
        f"""
        SELECT
            COUNT(*) as total_pre_rows,
            COUNT(CASE WHEN t.tag IS NOT NULL THEN 1 END) as matched_to_tag,
            COUNT(CASE WHEN t.tag IS NULL THEN 1 END) as unmatched_to_tag
        FROM bronze_pre p
        LEFT JOIN bronze_tag t ON p.tag = t.tag AND p.version = t.version
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Data Quality Log — bronze_pre
    """)
    return


@app.cell
def _(engine, mo):
    pre_quality_log = mo.sql(
        f"""
        SELECT
            field_name,
            check_category,
            check_type,
            severity,
            issue_count,
            total_records,
            issue_percentage,
            check_passed,
            error_details
        FROM data_quality_log
        WHERE table_name = 'bronze_pre'
        ORDER BY severity DESC, issue_count DESC
        """,
        engine=engine
    )
    return (pre_quality_log,)


@app.cell
def _(mo):
    mo.md(r"""
    # EDA of Bronze_num

    num.txt stores one row per XBRL numeric fact: a (company filing, tag, period, duration) tuple.
    Key columns:
    - **adsh**: accession number — ties back to bronze_sub
    - **tag / version**: XBRL concept name and taxonomy version
    - **ddate**: period end date (YYYYMMDD in source, parsed to DATE)
    - **qtrs**: duration in quarters (0 = instant/balance-sheet, 1–4 = income/flow)
    - **uom**: unit of measure (USD, shares, pure, etc.)
    - **value**: numeric value (NULL when a footnote replaces the number)
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Bronze_num Table Overview
    """)
    return


@app.cell
def _(bronze_num, engine, mo):
    bronze_num_summary = mo.sql(
        f"""
        SELECT
            COUNT(*) as total_facts,
            COUNT(DISTINCT adsh) as unique_filings,
            COUNT(DISTINCT tag) as unique_tags,
            COUNT(DISTINCT uom) as unique_uoms,
            MIN(ddate) as earliest_ddate,
            MAX(ddate) as latest_ddate,
            SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_value_count,
            ROUND(100.0 * SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_null_value
        FROM bronze_num
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Verify Date Field (ddate)

    Checks that TRY_STRPTIME correctly parsed all YYYYMMDD strings to DATE.
    Any NULLs here that were not NULL in the source indicate conversion failures.
    """)
    return


@app.cell
def _(bronze_num, engine, mo):
    num_date_check = mo.sql(
        f"""
        SELECT
            COUNT(*) as total_records,
            COUNT(ddate) as ddate_not_null,
            COUNT(*) - COUNT(ddate) as ddate_null_count
        FROM bronze_num
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Qtrs Distribution

    - 0 = instant (balance sheet items like Assets, Liabilities)
    - 1 = single quarter
    - 4 = full year (annual income statement items)
    - Other values are possible for trailing-twelve-month or multi-year periods
    """)
    return


@app.cell
def _(bronze_num, engine, mo):
    qtrs_distribution = mo.sql(
        f"""
        SELECT
            qtrs,
            COUNT(*) as fact_count,
            COUNT(DISTINCT tag) as unique_tags,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_total
        FROM bronze_num
        GROUP BY qtrs
        ORDER BY qtrs
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Unit of Measure (UOM) Distribution
    """)
    return


@app.cell
def _(bronze_num, engine, mo):
    uom_distribution = mo.sql(
        f"""
        SELECT
            uom,
            COUNT(*) as fact_count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_total
        FROM bronze_num
        GROUP BY uom
        ORDER BY fact_count DESC
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Top Tags by Frequency
    """)
    return


@app.cell
def _(bronze_num, engine, mo):
    top_tags = mo.sql(
        f"""
        SELECT
            tag,
            version,
            COUNT(*) as fact_count,
            COUNT(DISTINCT adsh) as filing_count,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM bronze_num
        WHERE value IS NOT NULL
        GROUP BY tag, version
        ORDER BY fact_count DESC
        LIMIT 20
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## NULL Value Analysis

    Rows where value IS NULL should have a footnote instead.
    If both value and footnote are NULL, that is a data quality concern.
    """)
    return


@app.cell
def _(bronze_num, engine, mo):
    null_value_analysis = mo.sql(
        f"""
        SELECT
            CASE
                WHEN value IS NULL AND footnote IS NULL THEN 'both_null'
                WHEN value IS NULL AND footnote IS NOT NULL THEN 'null_value_with_footnote'
                WHEN value IS NOT NULL THEN 'has_value'
            END as value_status,
            COUNT(*) as count
        FROM bronze_num
        GROUP BY value_status
        ORDER BY count DESC
        """,
        engine=engine
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Data Quality Log — bronze_num
    """)
    return


@app.cell
def _(engine, mo):
    num_quality_log = mo.sql(
        f"""
        SELECT
            field_name,
            check_category,
            check_type,
            severity,
            issue_count,
            total_records,
            issue_percentage,
            check_passed,
            error_details
        FROM data_quality_log
        WHERE table_name = 'bronze_num'
        ORDER BY severity DESC, issue_count DESC
        """,
        engine=engine
    )
    return (num_quality_log,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Sample Filings — Cross-reference with bronze_sub

    Confirms adsh values in bronze_num exist in bronze_sub (referential integrity check).
    """)
    return


@app.cell
def _(bronze_num, bronze_sub, engine, mo):
    referential_check = mo.sql(
        f"""
        SELECT
            COUNT(*) as total_num_facts,
            COUNT(CASE WHEN s.adsh IS NOT NULL THEN 1 END) as matched_to_sub,
            COUNT(CASE WHEN s.adsh IS NULL THEN 1 END) as unmatched_to_sub,
            ROUND(
                100.0 * COUNT(CASE WHEN s.adsh IS NULL THEN 1 END) / COUNT(*),
                2
            ) as pct_unmatched
        FROM bronze_num n
        LEFT JOIN bronze_sub s ON n.adsh = s.adsh
        """,
        engine=engine
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
