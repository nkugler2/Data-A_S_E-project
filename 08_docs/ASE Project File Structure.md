# File Structure

```
project_root/
├── 01_data/
│   ├── 01_raw/           # Original downloaded data
│   ├── 02_bronze/        # Ingested data (minimal changes)
│   ├── 03_silver/        # Cleaned data
│   └── 04_gold/          # Analytics-ready data
├── 02_src/
│   ├── 01_extract/       # Data ingestion scripts
│   ├── 02_transform/     # Cleaning and transformation
│   ├── 03_load/          # Loading to destinations
│   └── 04_utils/         # Helper functions
├── 03_notebooks/         # Exploratory analysis
├── 04_tests/             # Unit tests
├── 05_config/            # Configuration files
├── 06_logs/              # Pipeline logs
├── 07_sql/               # Has my SQL code
└── 08_docs/              # Has my documentation that is not in Obsidian


```

