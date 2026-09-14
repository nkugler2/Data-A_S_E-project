# Data-A-S-E — Pipeline plan

Goal: finish the bronze layer so it can be trusted and re-run, then decide
where silver and gold get built before building them. The long-range goals
are the eleven in [readme.md](../../readme.md).

## How to execute this plan

1. One phase per prompt: "complete phase N of @plan-pipeline.md". Do only that
   phase.
2. Learning mode is OFF in [AGENTS.md](../../AGENTS.md), but this is a
   learning project and Noah has written the pipeline by hand so far. Keep it
   that way unless he asks otherwise: explain, review, and scaffold, but don't
   rewrite `sampleDataLoadBronze.py` for him.
3. Never commit. Mark a phase `[x]` below when its "Done when" is true.

## Where things stand (2026-09-14)

- **Bronze, 2025q1:** all four tables are loaded (`bronze_sub` 6,231 rows,
  `bronze_num` 3.66 M, `bronze_pre` 751 K, `bronze_tag` 91 K). 48 of 49
  quality checks pass.
- **The one failing CRITICAL check** is `null_check / period`. All 3 rows are
  fund filings (`486BPOS`, `POS AMI`) that also have null `fy`/`fp`, which
  suggests they are non-periodic forms rather than bad loads.
- **Bronze, 2025q2:** downloaded and unzipped, not loaded;
  `sampleDataConfig.yaml` lists only `2025q1`.
- **Silver and gold:** nothing yet. The config's `silver.core_metrics`,
  `sample_companies`, and `gold` block are intent, not code.
- **Environment:** no `pyproject.toml` or venv; dependencies are imported from
  whatever Python runs the script.
- **Open question** from the 2026-01-27 commit: should every load run be
  logged, and should old databases be kept?

## Phase checklist

- [x] Phase 0 — Decide how this repo relates to learning-analytics-engineering
      (**B: split the work**, 2026-09-14)
- [ ] Phase 1 — Make bronze loads safe to re-run
- [ ] Phase 2 — Close out bronze (the failing check, 2025q2, environment)

**Paused until learning-analytics-engineering finishes Module 6** (Phase 0,
option B). These have no checkboxes on purpose, so they don't count toward
progress. When you resume them, rewrite them around readme goals 5–9, using
what the course taught.

- Phase 3 — Silver design doc
- Phase 4 — Build silver
- Phase 5 — Gold: reconstructed statements and ratios
- Phase 6 — Analytics: trends and comparisons

---

## Phase 0 — Decide the relationship with learning-analytics-engineering

**Why first:** both focus repos pull the same SEC Financial Statement Data Sets
(`sub`/`num`/`tag`/`pre`) into DuckDB. The course's Modules 0–6 then build
staging, dimensions, restatement-correct facts, and marts over them with dbt.
Phases 3–5 below would be a hand-rolled version of that same work, done before
learning it properly.

**Options:**

| | Option | What Data-A-S-E becomes |
| --- | --- | --- |
| A | Independent, both continue | A hand-written Python/SQL pipeline to compare against the dbt one. Phases 3–6 as written |
| B | Split the work | Finish bronze (Phases 1–2) now. Pause silver/gold until the course reaches Module 6, then do readme goals 5–9 (statements, trends, models) here on top of what you learned, or on the course's marts |
| C | Archive after bronze | Phases 1–2, then mark it complete; the course carries the SEC work forward |

**Decided 2026-09-14: B.** Phases 1–2 finish something that's 90% done and
answer the open question. After that, the course teaches silver and gold with
better tools and rubrics, and this repo keeps the part the course doesn't
cover: data science on the result. The phases below Phase 2 are kept as notes
for when the course reaches Module 6.

**Done when:** the choice is recorded in this plan and in the course's
[plan](../../../learning-analytics-engineering/notes/plans/plan-course.md),
and Phases 3–6 are kept, rewritten, or removed to match.

## Phase 1 — Make bronze loads safe to re-run

**Problem:** every `_load_*` method does `CREATE TABLE IF NOT EXISTS` followed
by `INSERT INTO` (`sampleDataLoadBronze.py:203/273`, `397/421`, `498/522`,
`595/622`). `load_quarter` (`:99`) has no check for an already-loaded
quarter. Running the loader twice for `2025q1` doubles every table, and
`data_quality_log` can't tell the two runs apart.

**Tasks:**

1. Decide the re-run rule: replace a quarter (delete its rows, then insert,
   in one transaction) or skip it if already loaded. Replace is the usual
   choice, because it lets you fix a loader bug and reload.
2. Give each run an identity (a `run_id` or the run's timestamp) and write it
   into `data_quality_log`, so checks from different runs are distinguishable.
   This answers the 2026-01-27 question: log every run and keep one database,
   rather than keeping old databases around.
3. Run the loader twice for `2025q1`.

**Done when:** row counts per table are identical after the second run, and
`data_quality_log` shows two distinguishable runs.

## Phase 2 — Close out bronze

**Tasks:**

1. **The failing check.** Confirm in the SEC field docs
   ([SEC Requirements](../../08_docs/SEC%20Requirements.md)) whether `period` is
   legitimately absent for non-periodic forms. If so, scope the not-null check
   to periodic forms or downgrade it. Record the decision in
   `08_docs/CHANGELOG.md`.
2. **Load 2025q2.** Add it to `data.quarters`, load it, and verify counts per
   `data_quarter`. This is the first real test that multi-quarter loading works.
3. **Environment.** Add a `pyproject.toml` with `duckdb`, `pandas`, `pyyaml`,
   `requests`, `marimo`, and a uv `.venv` (per the domain rules). Update
   AGENTS.md's Commands section to match.
4. **AGENTS.md is stale.** Its Gotchas say the `.duckdb` file is not
   gitignored, but `.gitignore` now has `*.duckdb`. Fix that line.
5. **Changelog date.** `08_docs/CHANGELOG.md`'s entry says 2025-01-22, but
   the commit is 2026-01-22.

**Done when:** all checks pass for both quarters (or the one check is scoped
and documented), `uv run python 02_src/01_extract/sampleDataLoadBronze.py`
works from a fresh clone, and AGENTS.md matches reality.

## Phase 3 — Silver design doc

Write the design before code, in `08_docs/` or `notes/docs/`. It must answer:

- **Grain** of the silver fact table: one row per what?
- **Which facts:** `silver.core_metrics` from the config. Are the eight tags
  enough to rebuild an income statement and a balance sheet?
- **`qtrs`:** point-in-time (balance sheet) vs duration (income statement)
  facts, and how each is kept.
- **`coreg`:** consolidated entity only, or subsidiaries too.
- **Restatements across quarters:** the same period reported again later with
  a different value. Which one wins, and is the history kept?
- **Units:** USD only, or also shares and ratios.
- **Scope:** all companies, or `sample_companies` first.

**Done when:** each question has an answer with a reason, and the doc is
linked from [docs/MOC.md](../docs/MOC.md).

## Phase 4 — Build silver

**Tasks:** create the silver DuckDB (`data.paths.databases.silver`), build the
tables the design doc specifies, add quality checks modeled on bronze's, and
write a marimo notebook that spot-checks one sample company against its
actual 10-K.

**Done when:** the spot-check matches the filing for at least one income
statement figure and one balance sheet figure.

## Phase 5 — Gold: reconstructed statements and ratios

Covers readme goal 5. Build per-company, per-period income statement and
balance sheet views (`gold.reconstruct_statements`), ratios
(`gold.calculate_ratios`), and apply `gold.min_data_points`.

**Done when:** a notebook shows one sample company's statements for both
loaded quarters next to the filed numbers.

## Phase 6 — Analytics: trends and comparisons

Covers readme goals 6–8. Two quarters aren't a trend, so this starts with a
scope decision: how many quarters to backfill (`01_data/01_fullData/`), and
whether Phase 1's loader handles that size.

**Done when:** a notebook compares the five sample companies over time, and
exports a spreadsheet.

---

## Later (not phased)

- Incremental updates from the SEC API (goal 4)
- Models of financial performance (goal 9)
- Outside data and news sentiment (stretch goals 10–11)
