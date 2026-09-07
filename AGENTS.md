# Data-A-S-E — SEC financial data pipeline

## What this is

An end-to-end pipeline over SEC "Financial Statement Data Sets" (quarterly bulk
dumps of every filer's XBRL facts), built to learn the full path: extraction →
data engineering → analytics → data science. Medallion layout: raw `.txt` →
**bronze** (typed, loaded, quality-logged) → **silver** (core metrics) → **gold**
(analytics-ready). Goals and the stretch list are in [readme.md](readme.md).

**Only the bronze layer exists today.** Silver and gold are empty directories.
One quarter (`2025q1`) is loaded; `2025q2` is downloaded but not loaded.

## Layout

Numbered directories, in pipeline order.

| Path | What |
| --- | --- |
| `01_data/01_sampleData/01_raw/` | Downloaded SEC quarter ZIPs (`2025q1.zip`, `2025q2.zip`) |
| `01_data/01_sampleData/02_bronze/` | Extracted `.txt` per quarter + `secSampleData_bronze.duckdb` |
| `01_data/01_sampleData/03_silver/`, `04_gold/` | Empty — not built yet |
| `01_data/01_fullData/` | Placeholder for the non-sample run |
| `02_src/01_extract/` | `sampleDataTestExtract.py` (download/unzip), `sampleDataLoadBronze.py` (`BronzeLoader` → DuckDB) |
| `02_src/02_transform/`, `03_load/`, `04_utils/` | Empty |
| `03_notebooks/` | Marimo notebooks — `CorrectedBronzeLoadAnalysis.py` |
| `05_config/` | `sampleDataConfig.yaml` (the knobs) + `sampleDataConfig.py` (`Config` loader) |
| `06_logs/downloads.csv` | Download audit log |
| `08_docs/` | Hand-written docs — structure, changelog, SEC requirements |
| `notes/` | Project vault — [home](notes/HOME.md) |

## Commands

Scripts resolve paths relative to the project root via `05_config/sampleDataConfig.py`,
so run them from the repo root.

```sh
python3 02_src/01_extract/sampleDataTestExtract.py   # download + unzip a quarter into 02_bronze/
python3 02_src/01_extract/sampleDataLoadBronze.py    # load the .txt files into the bronze DuckDB
marimo edit 03_notebooks/CorrectedBronzeLoadAnalysis.py   # bronze load analysis notebook
duckdb -readonly 01_data/01_sampleData/02_bronze/secSampleData_bronze.duckdb
duckdb --ui                                          # browser UI; attach the path above
```

Dependencies are imported directly, with no `requirements.txt` and no venv in the
repo: `duckdb`, `pandas`, `pyyaml`, `requests`, `marimo`.

## Querying the data (DuckDB MCP)

`.mcp.json` wires a project-scoped [DuckDB MCP server](https://github.com/motherduckdb/mcp-server-motherduck)
(`uvx mcp-server-motherduck`) at the bronze database. **Prefer it over shelling out
to the `duckdb` CLI** for questions about the data.

Tools: `list_databases`, `list_tables`, `list_columns`, `execute_query`.

It is **read-only** — the server attaches the file read-only and DuckDB itself
rejects DDL/DML, so a `CREATE`/`INSERT` comes back as
`Cannot execute statement of type "CREATE" ... attached in read-only mode!`.
Loading data is the pipeline scripts' job, never the MCP's. Read-only also means
it takes no file lock, so it coexists with a `duckdb --ui` session on the same file.

### Bronze schema

Four SEC tables plus a quality log. Every table carries `data_quarter` (e.g.
`2025q1`) and `load_timestamp` provenance columns added by the loader.

| Table | Rows (2025q1) | Grain | Key columns |
| --- | --- | --- | --- |
| `bronze_sub` | 6,231 | one filing (submission) | `adsh` (accession no.), `cik`, `name`, `sic`, `form`, `period`, `fy`, `fp`, `filed` |
| `bronze_num` | 3,658,551 | one numeric fact | `adsh` + `tag` + `version` + `ddate` + `qtrs` + `uom` + `coreg`, `value` |
| `bronze_pre` | 750,778 | one line on one statement | `adsh` + `report` + `line`, `stmt`, `tag`, `plabel` |
| `bronze_tag` | 90,655 | one XBRL tag definition | `tag` + `version`, `custom`, `abstract`, `datatype`, `crdr`, `tlabel` |
| `data_quality_log` | 49 | one check run | `check_category`, `check_type`, `field_name`, `issue_count`, `check_passed`, `severity` |

`adsh` joins `sub` → `num` → `pre`; `tag` + `version` joins `num`/`pre` → `tag`.
`cik` is the company identifier (`bronze_sub.cik`), and one company has many
filings across quarters.

## Gotchas

- **The `.duckdb` file is 297 MB and is NOT gitignored** — it is merely untracked.
  A careless `git add .` would try to commit it. `.gitignore` excludes
  `01_data/**/*.txt` and `*.zip` but has no `*.duckdb` rule.
- Only `2025q1` is in the database. `2025q2` is unzipped in `01_data/01_sampleData/02_bronze/2025q2/`
  but never loaded — so any "all quarters" question is really one quarter today.
- `sampleDataConfig.yaml` lists silver/gold database paths and `sample_companies`
  (Apple, Microsoft, Amazon, Meta, Alphabet CIKs). Those are **intent, not state** —
  nothing consumes them yet.
- The loader finds the project root by walking up for a `05_config` directory.
  Moving or renaming that directory breaks path resolution.
- Directory names are zero-padded and numbered (`01_data`, not `data`), and some
  `08_docs` filenames contain spaces — quote paths.
- SEC source data is tab-delimited UTF-8 `.txt`, not CSV.

## Learning mode: OFF

<!-- ON | OFF - set this line; leave the rest of the section as written.

When ON, the agent applies the five-rung learning ladder (`/learn`)
AUTOMATICALLY, without being asked, in three contexts:

  1. breaking down a question Noah asks,
  2. teaching a concept while Noah is deciding what to build,
  3. pushing back on an idea.

Discretion governs DEPTH, never whether: a small question gets the compressed
form, a subsystem gets the full ladder.

It never fires at the end of a phase, or after a batch of edits, at any flag
setting. Phases end at the /phase-complete report and the devlog entry; Noah
runs /learn himself when he wants a phase's work explained.

Foundational explanations (architecture, whole subsystems) are written to
notes/explanations/<kebab-topic>.md and linked in that folder's MOC;
day-to-day answers stay in the terminal.

When OFF, none of the above happens unprompted - `/learn` still works on
demand. Operational spec: ~/ai/claude/skills/learn/SKILL.md. -->
