# Bronze DB connection path

_Originally written 2026-01-17; corrected 2026-09-07 — the old note recorded an
absolute path from a previous machine/directory layout
(`Documents/Coding/Data-A_S_E-project/...`) that no longer exists._

So I can get back into the bronze database from the first sample load without
hunting for it.

**Repo-relative** (preferred — survives the repo moving):

```text
01_data/01_sampleData/02_bronze/secSampleData_bronze.duckdb
```

**Absolute, on this machine:**

```text
/Users/noahkugler/Documents/code/Data-A-S-E/01_data/01_sampleData/02_bronze/secSampleData_bronze.duckdb
```

## Connecting

From the repo root:

```sh
duckdb -readonly 01_data/01_sampleData/02_bronze/secSampleData_bronze.duckdb
duckdb --ui        # then ATTACH the path above
```

In marimo / Python:

```python
import duckdb
con = duckdb.connect(
    "01_data/01_sampleData/02_bronze/secSampleData_bronze.duckdb",
    read_only=True,
)
```

`read_only=True` takes no file lock, so a notebook and a CLI session can share
the file.

## For agents

Don't shell out to the CLI — this repo has a project-scoped **DuckDB MCP server**
wired in `.mcp.json` (read-only). Schema notes live in
[AGENTS.md](../AGENTS.md).
