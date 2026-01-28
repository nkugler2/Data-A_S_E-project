import duckdb

# from numpy import where
import pandas as pd
from pathlib import Path
from datetime import datetime

# import sys
import importlib.util

# Set the path to the configuration file #
config_path = (
    Path(__file__).parent.parent.parent / "05_config" / "sampleDataConfig.py"
)  #

# Set the specification for the configuration file #
spec = importlib.util.spec_from_file_location("sampleDataConfig", config_path)

if spec is None:
    raise FileNotFoundError(f"Configuration file not found: {config_path}")

assert spec is not None

# Import the configuration file #
sampleDataConfig = importlib.util.module_from_spec(spec)

if spec.loader is not None:
    spec.loader.exec_module(sampleDataConfig)

# Assign the configuration file to the Config variable #
Config = sampleDataConfig.Config


class BronzeLoader:
    """Load raw SEC TXT files into DuckDB bronze tables"""

    ##################################################################################################################
    # Step 1: Initialize the BronzeLoader class
    # This is the class that will be called to actually load the .txt files into the database
    ##################################################################################################################

    def __init__(self, config: Config):
        """
        Initializes the BronzeLoader class
        Args:
            self: The class instance
            config: The configuration object, uses my configuration file to connect to the database and load the data
        """
        # Assign the configuration file to the config variable #
        self.config = config

        # Ensure the database directory exists #
        bronze_db_path = config.bronze_database_path
        bronze_db_path.parent.mkdir(parents=True, exist_ok=True)

        # Connect to the bronze database #
        self.conn = duckdb.connect(str(bronze_db_path))

        # Initialize data quality infrastructure
        self._initialize_data_quality_infrastructure()

    ##################################################################################################################
    # Step 2: Initialize data quality infrastructure
    ##################################################################################################################

    def _initialize_data_quality_infrastructure(self):
        """Create sequences and tables for data quality tracking"""

        # Create sequence for auto-incrementing log_id
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_data_quality_log START 1
        """)

        # Create unified data quality log table for all bronze tables
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_log (
                log_id INTEGER PRIMARY KEY DEFAULT nextval('seq_data_quality_log'),
                table_name VARCHAR NOT NULL,
                data_quarter VARCHAR NOT NULL,
                load_timestamp TIMESTAMP NOT NULL,
                check_category VARCHAR NOT NULL,
                check_type VARCHAR NOT NULL,
                field_name VARCHAR NOT NULL,
                issue_count INTEGER NOT NULL,
                total_records INTEGER NOT NULL,
                issue_percentage DECIMAL(5,2),
                check_passed BOOLEAN NOT NULL,
                severity VARCHAR,
                error_details VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ Data quality infrastructure initialized")

    ##################################################################################################################
    # Step 3: Load files for a single quarter
    ##################################################################################################################

    def load_quarter(self, quarter: str):
        """Load all files for a single quarter

        This uses my configuration file to know where to load the data,
        and can later be used to iterate over all quarters in my configuration file

        Args:
         self: The class instance
         quarter: The quarter to load, uses my configuration file to load the data
        """
        # Used for logging in the terminal #
        print(f"Loading {quarter} into bronze layer...")

        # Defining the path to the quarter #
        ## The `bronze_path` is defined in my configuration file ##
        ## quarter is defined at the end of the function, and grabs each quarter from `quarters` in my configuration file ##
        quarter_path = self.config.bronze_path / quarter

        # Load each file type based on configuration
        ## Get the list of files to load from the configuration file ##
        files_to_load = self.config.bronze_files_to_load

        if "sub.txt" in files_to_load:
            self._load_sub(
                quarter_path, quarter
            )  # This is where I load the submission data #
        if "num.txt" in files_to_load:
            self._load_num(
                quarter_path, quarter
            )  # This is where I load the numeric data #
        if "tag.txt" in files_to_load:
            self._load_tag(quarter_path, quarter)  # This is where I load the tag data #
        if "pre.txt" in files_to_load:
            self._load_pre(
                quarter_path, quarter
            )  # This is where I load the presentation data #

        # Used for logging in the terminal #
        print(f"✓ {quarter} loaded successfully")

    ##################################################################################################################
    # Step 4: Load submission data
    ##################################################################################################################

    def _load_sub(self, quarter_path: Path, quarter: str):
        """Load submission data

        1. Reads the sub.txt file into a pandas dataframe
        2. Adds metadata columns to the dataframe
        3. Creates the bronze_sub table if it doesn't exist
        4. Inserts the dataframe into the bronze_sub table with explicit type conversion using TRY_CAST
        5. Tracks data quality metrics in a permanent audit table

        Args:
         self: The class instance
         quarter_path: The path to the quarter
         quarter: The quarter to load, uses my configuration file to load the data
        """

        # Defining the path to the sub.txt file #
        file_path = quarter_path / "sub.txt"

        # Reading the sub.txt file with pandas #
        df = pd.read_csv(
            file_path,
            sep="\t",
            encoding="utf-8",
            dtype=str,  # Read everything as string initially
            low_memory=False,
        )

        # Add metadata columns
        df["data_quarter"] = quarter
        df["load_timestamp"] = datetime.now()

        total_source_records = len(df)
        load_timestamp = datetime.now()

        # Track source data quality BEFORE conversion
        source_null_counts = {}
        for field in [
            "adsh",
            "cik",
            "name",
            "form",
            "period",
            "filed",
            "accepted",
            "sic",
            "ein",
            "fy",
            "changed",
            "fye",
            "instance",
            "detail",
            "wksi",
            "prevrpt",
            "accepted",
            "nciks",
        ]:
            source_null_counts[field] = int(df[field].isna().sum())

        # Create or append to DuckDB table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze_sub (
                -- Primary Identifier --
                adsh VARCHAR(20) NOT NULL,
                
                -- Company Identifier --
                cik INTEGER NOT NULL,
                name VARCHAR(150) NOT NULL,
                sic INTEGER,

                -- Business Address fields --                
                countryba VARCHAR(2),
                stprba VARCHAR(2),
                cityba VARCHAR(30),
                zipba VARCHAR(10),
                bas1 VARCHAR(40),
                bas2 VARCHAR(40),
                baph VARCHAR(20),

                -- Mailing Address fields --
                countryma VARCHAR(2),
                stprma VARCHAR(2),
                cityma VARCHAR(30),
                zipma VARCHAR(10),
                mas1 VARCHAR(40),
                mas2 VARCHAR(40),

                -- Incorporation Address fields --
                countryinc VARCHAR(3),
                stprinc VARCHAR(2),
                ein INTEGER,

                -- Company Name History --
                former VARCHAR(150),
                changed VARCHAR(8),

                -- Filing Characteristics --
                afs VARCHAR(5),
                wksi BOOLEAN NOT NULL,
                fye VARCHAR(4),
                form VARCHAR(10) NOT NULL,

                -- Period Information --
                period DATE,
                fy INTEGER,
                fp VARCHAR(2),

                -- Filing Dates -- 
                filed DATE,
                accepted TIMESTAMP NOT NULL,

                -- Additional flags --
                prevrpt BOOLEAN,
                detail BOOLEAN,

                -- Instance Information
                instance VARCHAR(40),
                nciks INTEGER,
                aciks VARCHAR,

                -- Metadata --
                data_quarter VARCHAR,
                load_timestamp TIMESTAMP
            )
        """)

        self.conn.register("sub_df", df)

        # Insert data with explicit type conversion using TRY_CAST #
        ## TRY_CAST us used only on columns that are not strings ##
        self.conn.execute("""
            INSERT INTO bronze_sub
            SELECT 
                adsh,
                TRY_CAST(cik AS INTEGER),
                name,
                TRY_CAST(sic AS INTEGER),
                countryba,
                stprba,
                cityba,
                zipba,
                bas1,
                bas2,
                baph,
                countryma,
                stprma,
                cityma,
                zipma,
                mas1,
                mas2,
                countryinc,
                stprinc,
                TRY_CAST(ein AS INTEGER),
                former,
                TRY_STRPTIME(changed, '%Y%m%d')::DATE,
                afs,
                TRY_CAST(wksi AS BOOLEAN),
                TRY_CAST(fye AS VARCHAR(4)),
                form,
                TRY_STRPTIME(period, '%Y%m%d')::DATE,
                TRY_CAST(fy AS INTEGER),
                fp,
                TRY_STRPTIME(filed, '%Y%m%d')::DATE,
                TRY_CAST(accepted AS TIMESTAMP),
                TRY_CAST(prevrpt AS BOOLEAN),
                TRY_CAST(detail AS BOOLEAN),
                instance,
                TRY_CAST(nciks AS INTEGER),
                aciks,
                
                -- Metadata --
                data_quarter,
                TRY_CAST(load_timestamp AS TIMESTAMP)
            FROM sub_df
        """)

        self.conn.unregister("sub_df")

        # Track data quality
        quality_checks = [
            ("null_check", "required_field", "adsh", "CRITICAL"),
            ("null_check", "required_field", "cik", "CRITICAL"),
            ("null_check", "required_field", "name", "CRITICAL"),
            ("null_check", "required_field", "wksi", "CRITICAL"),
            ("null_check", "required_field", "form", "CRITICAL"),
            ("null_check", "required_field", "period", "CRITICAL"),
            ("null_check", "required_field", "filed", "CRITICAL"),
            ("null_check", "required_field", "accepted", "CRITICAL"),
            ("null_check", "required_field", "prevrpt", "CRITICAL"),
            ("null_check", "required_field", "instance", "CRITICAL"),
            ("null_check", "required_field", "nciks", "CRITICAL"),
            ("type_conversion", "integer_conversion", "cik", "CRITICAL"),
            ("type_conversion", "integer_conversion", "sic", "WARNING"),
            ("type_conversion", "integer_conversion", "ein", "WARNING"),
            ("type_conversion", "integer_conversion", "fy", "WARNING"),
            ("type_conversion", "date_conversion", "period", "CRITICAL"),
            ("type_conversion", "date_conversion", "filed", "CRITICAL"),
            ("type_conversion", "date_conversion", "changed", "WARNING"),
            ("type_conversion", "date_conversion", "fye", "WARNING"),
            ("type_conversion", "boolean_conversion", "wksi", "WARNING"),
            ("type_conversion", "boolean_conversion", "prevrpt", "WARNING"),
            ("type_conversion", "timestamp_conversion", "accepted", "CRITICAL"),
            # wksi conversion
        ]

        self._log_data_quality(
            table_name="bronze_sub",
            quarter=quarter,
            load_timestamp=load_timestamp,
            quality_checks=quality_checks,
            total_records=total_source_records,
            source_null_counts=source_null_counts,
        )
        print(f"  ✓ Loaded {total_source_records:,} submissions from sub.txt")

    ##################################################################################################################
    # Step 5: Load numeric facts data
    ##################################################################################################################

    def _load_num(self, quarter_path: Path, quarter: str):
        """Load numeric facts data"""
        file_path = quarter_path / "num.txt"

        df = pd.read_csv(
            file_path,
            sep="\t",
            encoding="latin-1",
            dtype={
                "adsh": str,
                "tag": str,
                "version": str,
                "coreg": str,
                "ddate": str,
                "qtrs": str,
                "uom": str,
                "value": float,
                "footnote": str,
            },
            low_memory=False,
        )

        df["data_quarter"] = quarter
        df["load_timestamp"] = datetime.now()

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze_num (
                adsh VARCHAR,
                tag VARCHAR,
                version VARCHAR,
                coreg VARCHAR,
                ddate VARCHAR,
                qtrs VARCHAR,
                uom VARCHAR,
                value DOUBLE,
                footnote VARCHAR,
                data_quarter VARCHAR,
                load_timestamp TIMESTAMP
            )
        """)

        self.conn.register("num_df", df)
        self.conn.execute("INSERT INTO bronze_num SELECT * FROM num_df")
        self.conn.unregister("num_df")

        print(f"  ✓ Loaded {len(df):,} numeric facts from num.txt")

    ##################################################################################################################
    # Step 6: Load tag definitions
    ##################################################################################################################

    def _load_tag(self, quarter_path: Path, quarter: str):
        """Load tag definitions"""
        file_path = quarter_path / "tag.txt"

        df = pd.read_csv(
            file_path, sep="\t", encoding="latin-1", dtype=str, low_memory=False
        )

        df["data_quarter"] = quarter
        df["load_timestamp"] = datetime.now()

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze_tag (
                tag VARCHAR,
                version VARCHAR,
                custom VARCHAR,
                abstract VARCHAR,
                datatype VARCHAR,
                iord VARCHAR,
                crdr VARCHAR,
                tlabel VARCHAR,
                doc VARCHAR,
                data_quarter VARCHAR,
                load_timestamp TIMESTAMP
            )
        """)

        self.conn.register("tag_df", df)
        self.conn.execute("INSERT INTO bronze_tag SELECT * FROM tag_df")
        self.conn.unregister("tag_df")

        print(f"  ✓ Loaded {len(df):,} tag definitions from tag.txt")

    ##################################################################################################################
    # Step 7: Load presentation data
    ##################################################################################################################

    def _load_pre(self, quarter_path: Path, quarter: str):
        """Load presentation data"""
        file_path = quarter_path / "pre.txt"

        df = pd.read_csv(
            file_path, sep="\t", encoding="latin-1", dtype=str, low_memory=False
        )

        df["data_quarter"] = quarter
        df["load_timestamp"] = datetime.now()

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze_pre (
                adsh VARCHAR,
                report VARCHAR,
                line VARCHAR,
                stmt VARCHAR,
                inpth VARCHAR,
                rfile VARCHAR,
                tag VARCHAR,
                version VARCHAR,
                plabel VARCHAR,
                negating VARCHAR,
                data_quarter VARCHAR,
                load_timestamp TIMESTAMP
            )
        """)

        self.conn.register("pre_df", df)
        self.conn.execute("INSERT INTO bronze_pre SELECT * FROM pre_df")
        self.conn.unregister("pre_df")

        print(f"  ✓ Loaded {len(df):,} presentation rows from pre.txt")

    ##################################################################################################################
    # Step 8: Log Data Quality Metrics
    ##################################################################################################################
    def _log_data_quality(
        self,
        table_name: str,
        quarter: str,
        load_timestamp: datetime,
        quality_checks: list,
        total_records: int,
        source_null_counts: dict,
    ):
        """
        Logs data quality metrics for the specified table

        This method handles two types of checks:
        1. null_check: Verifies required fields have no NULL values
        2. type_conversion: Detects if TRY_CAST operations failed during data load

        Type conversion failures are detected by comparing NULL counts before and after conversion.
        If the target table has more NULLs than the source, those are conversion failures.

        Args:
            table_name: Name of the bronze table being checked
            quarter: Data quarter being processed
            load_timestamp: Timestamp of the load operation
            quality_checks: List of tuples (check_category, check_type, field_name, severity)
            total_records: Total number of records in source data
            source_null_counts: Dictionary of NULL counts from source data before conversion
        """
        failed_checks = []

        for check_category, check_type, field_name, severity in quality_checks:
            if check_category == "null_check":
                # For null checks, count NULLs in the target table
                result = self.conn.execute(
                    f"""
                    SELECT
                        COUNT(*) as total_records,
                        SUM(CASE WHEN {field_name} IS NULL THEN 1 ELSE 0 END) as null_count
                    FROM {table_name}
                    WHERE data_quarter = ?
                    """,
                    [quarter],
                ).fetchone()

                total = int(result[0]) if result else 0
                issue_count = int(result[1]) if result else 0
                error_details = (
                    f"{issue_count} NULL values found in {field_name}"
                    if issue_count > 0
                    else None
                )

            elif check_category == "type_conversion":
                # For type conversion checks, compare source NULLs vs target NULLs
                # If target has MORE nulls than source, those are conversion failures

                source_nulls = source_null_counts.get(field_name, 0)

                result = self.conn.execute(
                    f"""
                    SELECT
                        COUNT(*) as total_records,
                        SUM(CASE WHEN {field_name} IS NULL THEN 1 ELSE 0 END) as null_count
                    FROM {table_name}
                    WHERE data_quarter = ?
                    """,
                    [quarter],
                ).fetchone()

                total = int(result[0]) if result else 0
                target_nulls = int(result[1]) if result else 0

                # Conversion failures = target NULLs - source NULLs
                # (New NULLs that appeared after TRY_CAST failed)
                issue_count = max(0, int(target_nulls) - int(source_nulls))

                if issue_count > 0:
                    error_details = (
                        f"{issue_count} {check_type.replace('_', ' ')} failures in {field_name} "
                        f"(source NULLs: {source_nulls}, target NULLs: {target_nulls})"
                    )
                else:
                    error_details = None

            else:
                # Unknown check category - skip
                continue

            # Calculate issue percentage
            issue_percentage = float(issue_count / total * 100) if total > 0 else 0

            # Determine if check passed based on severity
            # CRITICAL: Must have zero issues
            # WARNING: Can have up to 5% issues
            if severity == "CRITICAL":
                check_passed = issue_count == 0
            else:
                check_passed = issue_percentage < 5.0

            # Insert into quality log
            self.conn.execute(
                """
                INSERT INTO data_quality_log (
                    table_name, data_quarter, load_timestamp,
                    check_category, check_type, field_name,
                    issue_count, total_records, issue_percentage,
                    check_passed, severity, error_details
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    table_name,
                    quarter,
                    load_timestamp,
                    check_category,
                    check_type,
                    field_name,
                    int(issue_count),
                    int(total_records),
                    float(round(issue_percentage, 2)),
                    check_passed,
                    severity,
                    error_details,
                ],
            )

            # Track failed checks for reporting
            if not check_passed:
                failed_checks.append((field_name, severity, issue_count, error_details))

        # Print warnings if there are failures
        if failed_checks:
            print("  ⚠️  Data quality issues detected:")
            for field, sev, count, details in failed_checks:
                print(f"      [{sev}] {field}: {count} issues - {details}")
        else:
            print("  ✓ All data quality checks passed")

    ##################################################################################################################
    # Step 9: Create indexes
    ##################################################################################################################

    ##### I NEED TO UNDERSTAND IF I NEED TO CREATE INDEXES FOR THE BRONZE LAYER #####
    def create_indexes(self):
        """Create indexes for better query performance"""
        print("Creating indexes...")

        # Key indexes for joins
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sub_adsh ON bronze_sub(adsh)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sub_cik ON bronze_sub(cik)")
        # self.conn.execute("CREATE INDEX IF NOT EXISTS idx_num_adsh ON bronze_num(adsh)")
        # self.conn.execute("CREATE INDEX IF NOT EXISTS idx_num_tag ON bronze_num(tag)")
        # self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pre_adsh ON bronze_pre(adsh)")
        # self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pre_stmt ON bronze_pre(stmt)")

        print("✓ Indexes created")

    ##### THESE ARE SUMMARY STATS, NEED TO CHECK IF I NEED THIS OR TO ADD MORE #####
    def get_summary_stats(self):
        """Get summary statistics of loaded data"""
        stats = {}
        result = self.conn.execute("SELECT COUNT(*) FROM bronze_sub").fetchone()
        stats["submissions"] = result[0] if result else 0

        result = self.conn.execute(
            "SELECT COUNT(DISTINCT cik) FROM bronze_sub"
        ).fetchone()
        stats["companies"] = result[0] if result else 0

        # stats["numeric_facts"] = self.conn.execute(
        #     "SELECT COUNT(*) FROM bronze_num"
        # ).fetchone()[0]

        # stats["unique_tags"] = self.conn.execute(
        #     "SELECT COUNT(DISTINCT tag) FROM bronze_num"
        # ).fetchone()[0]

        # stats["presentation_rows"] = self.conn.execute(
        #     "SELECT COUNT(*) FROM bronze_pre"
        # ).fetchone()[0]

        return stats

    def close(self):
        """Close database connection"""
        self.conn.close()


# Usage script
if __name__ == "__main__":
    # Derive YAML config path from the Python config path (replace .py with .yaml)
    config_yaml_path = config_path.with_suffix(".yaml")
    config = Config(str(config_yaml_path))
    loader = BronzeLoader(config)
    # Create indexes
    # loader.create_indexes()

    # Load all configured quarters
    for quarter in config.quarters:
        loader.load_quarter(quarter)

    # Print summary
    stats = loader.get_summary_stats()
    print("\n" + "=" * 50)
    print("BRONZE LAYER SUMMARY")
    print("=" * 50)
    print(f"Submissions:      {stats['submissions']:,}")
    print(f"Unique Companies: {stats['companies']:,}")
    # print(f"Numeric Facts:    {stats['numeric_facts']:,}")
    # print(f"Unique Tags:      {stats['unique_tags']:,}")
    # print(f"Presentation Rows: {stats['presentation_rows']:,}")
    print("=" * 50)

    loader.close()
