import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys
import importlib.util

# Set the path to the configuration file #
config_path = Path(__file__).parent.parent.parent / "05_config" / "sampleDataConfig.py"#

# Set the specification for the configuration file #
spec = importlib.util.spec_from_file_location("sampleDataConfig", config_path)

# Import the configuration file #
sampleDataConfig = importlib.util.module_from_spec(spec)

# Execute the configuration file #
spec.loader.exec_module(sampleDataConfig) # This is where I execute the configuration file #

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

        # Connect to the database #
        self.conn = duckdb.connect(str(config.database_path)) 

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
        
        # Load each file type
        self._load_sub(quarter_path, quarter) # This is where I load the submission data #
        self._load_num(quarter_path, quarter) # This is where I load the numeric data #
        self._load_tag(quarter_path, quarter) # This is where I load the tag data #
        self._load_pre(quarter_path, quarter) # This is where I load the presentation data #
        
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
            sep='\t',
            encoding='utf-8',
            dtype=str,  # Read everything as string initially
            low_memory=False
        )
        
        # Add metadata columns
        df['data_quarter'] = quarter
        df['load_timestamp'] = datetime.now()

        total_source_records = len(df)
        load_timestamp = datetime.now()
        

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
                changed DATE,

                -- Filing Characteristics --
                afs VARCHAR(5),
                wksi BOOLEAN NOT NULL,
                fye DATE,
                form VARCHAR(10) NOT NULL,

                -- Period Information --
                period DATE NOT NULL,
                fy DATE,
                fp VARCHAR(2),

                -- Filing Dates -- 
                filed DATE NOT NULL,
                accepted TIMESTAMP NOT NULL,

                -- Additional flags --
                prevrpt BOOLEAN,
                detail BOOLEAN,

                -- Instance Information
                instance VARCHAR,
                nciks INTEGER,
                aciks VARCHAR,

                -- Metadata --
                data_quarter VARCHAR,
                load_timestamp TIMESTAMP
            )
        """)

        self.conn.register('sub_df', df)


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
                TRY_CAST(changed AS DATE),
                afs,
                TRY_CAST(wksi AS BOOLEAN),
                TRY_CAST(fye AS DATE),
                form,
                TRY_CAST(period AS DATE),
                TRY_CAST(fy AS INTEGER),
                fp,
                TRY_CAST(filed AS DATE),
                TRY_CAST(accepted AS DATETIME),
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
        
        self.conn.unregister('sub_df')
        
        # Track data quality
        quality_checks = [
            ('null_check', 'required_field', 'adsh', 'CRITICAL'),
            ('null_check', 'required_field', 'cik', 'CRITICAL'),
            ('null_check', 'required_field', 'name', 'CRITICAL'),
            ('null_check', 'required_field', 'form', 'CRITICAL'),
            ('null_check', 'required_field', 'instance', 'CRITICAL'),
            ('type_conversion', 'integer_conversion', 'cik', 'CRITICAL'),
            ('type_conversion', 'integer_conversion', 'sic', 'WARNING'),
            ('type_conversion', 'integer_conversion', 'ein', 'WARNING'),
            ('type_conversion', 'integer_conversion', 'fy', 'WARNING'),
            ('type_conversion', 'date_conversion', 'period', 'CRITICAL'),
            ('type_conversion', 'date_conversion', 'filed', 'CRITICAL'),
            ('type_conversion', 'date_conversion', 'changed', 'WARNING'),
            ('type_conversion', 'date_conversion', 'fye', 'WARNING'),

            ('type_conversion', 'timestamp_conversion', 'accepted', 'CRITICAL'),
            # wksi conversion
        ]
        
        self._log_data_quality(
            table_name='bronze_sub',
            quarter=quarter,
            load_timestamp=load_timestamp,
            quality_checks=quality_checks,
            total_records=total_source_records
        )
        
        print(f"  ✓ Loaded {total_source_records:,} submissions from sub.txt")


    # TO DO
    # 1. Ensure that the quality checks here account for all issues
    #    - Critical issues should be when it is supposed to be NOT NULL
    #    - Warning issues should be things that would be nice to have but are not crucial data that i need
    # 2. Change the DATE() things in my schema to either DATE alone for year/month/day or just integer for yyyy or mmdd
    

    ##################################################################################################################
    # Step 5: Load numeric facts data
    ##################################################################################################################

    def _load_num(self, quarter_path: Path, quarter: str):
        """Load numeric facts data"""
        file_path = quarter_path / "num.txt"
        
        df = pd.read_csv(
            file_path,
            sep='\t',
            encoding='latin-1',
            dtype={'adsh': str, 'tag': str, 'version': str, 
                   'coreg': str, 'ddate': str, 'qtrs': str, 
                   'uom': str, 'value': float, 'footnote': str},
            low_memory=False
        )
        
        df['data_quarter'] = quarter
        df['load_timestamp'] = datetime.now()
        
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
        
        self.conn.register('num_df', df)
        self.conn.execute("INSERT INTO bronze_num SELECT * FROM num_df")
        self.conn.unregister('num_df')
        
        print(f"  ✓ Loaded {len(df):,} numeric facts from num.txt")
    
    ##################################################################################################################
    # Step 6: Load tag definitions
    ##################################################################################################################

    def _load_tag(self, quarter_path: Path, quarter: str):
        """Load tag definitions"""
        file_path = quarter_path / "tag.txt"
        
        df = pd.read_csv(
            file_path,
            sep='\t',
            encoding='latin-1',
            dtype=str,
            low_memory=False
        )
        
        df['data_quarter'] = quarter
        df['load_timestamp'] = datetime.now()
        
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
        
        self.conn.register('tag_df', df)
        self.conn.execute("INSERT INTO bronze_tag SELECT * FROM tag_df")
        self.conn.unregister('tag_df')
        
        print(f"  ✓ Loaded {len(df):,} tag definitions from tag.txt")
    
    
    ##################################################################################################################
    # Step 7: Load presentation data
    ##################################################################################################################
    
    def _load_pre(self, quarter_path: Path, quarter: str):
        """Load presentation data"""
        file_path = quarter_path / "pre.txt"
        
        df = pd.read_csv(
            file_path,
            sep='\t',
            encoding='latin-1',
            dtype=str,
            low_memory=False
        )
        
        df['data_quarter'] = quarter
        df['load_timestamp'] = datetime.now()
        
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
        
        self.conn.register('pre_df', df)
        self.conn.execute("INSERT INTO bronze_pre SELECT * FROM pre_df")
        self.conn.unregister('pre_df')
        
        print(f"  ✓ Loaded {len(df):,} presentation rows from pre.txt")
    

    ##################################################################################################################
    # Step 8: Log Data Quality Metrics
    ##################################################################################################################
    







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
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_num_adsh ON bronze_num(adsh)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_num_tag ON bronze_num(tag)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pre_adsh ON bronze_pre(adsh)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pre_stmt ON bronze_pre(stmt)")
        
        print("✓ Indexes created")
    

    ##### THESE ARE SUMMARY STATS, NEED TO CHECK IF I NEED THIS OR TO ADD MORE #####
    def get_summary_stats(self):
        """Get summary statistics of loaded data"""
        stats = {}
        
        stats['submissions'] = self.conn.execute(
            "SELECT COUNT(*) FROM bronze_sub"
        ).fetchone()[0]
        
        stats['companies'] = self.conn.execute(
            "SELECT COUNT(DISTINCT cik) FROM bronze_sub"
        ).fetchone()[0]
        
        stats['numeric_facts'] = self.conn.execute(
            "SELECT COUNT(*) FROM bronze_num"
        ).fetchone()[0]
        
        stats['unique_tags'] = self.conn.execute(
            "SELECT COUNT(DISTINCT tag) FROM bronze_num"
        ).fetchone()[0]
        
        stats['presentation_rows'] = self.conn.execute(
            "SELECT COUNT(*) FROM bronze_pre"
        ).fetchone()[0]
        
        return stats
    
    def close(self):
        """Close database connection"""
        self.conn.close()


# Usage script
if __name__ == "__main__":
    config = Config("05_config/sampleDataConfig.yaml")
    loader = BronzeLoader(config)
    
    # Load all configured quarters
    for quarter in config.quarters:
        loader.load_quarter(quarter)
    
    # Create indexes
    loader.create_indexes()
    
    # Print summary
    stats = loader.get_summary_stats()
    print("\n" + "="*50)
    print("BRONZE LAYER SUMMARY")
    print("="*50)
    print(f"Submissions:      {stats['submissions']:,}")
    print(f"Unique Companies: {stats['companies']:,}")
    print(f"Numeric Facts:    {stats['numeric_facts']:,}")
    print(f"Unique Tags:      {stats['unique_tags']:,}")
    print(f"Presentation Rows: {stats['presentation_rows']:,}")
    print("="*50)
    
    loader.close()