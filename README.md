# QA Report Builder -- Cross-Platform (Snowflake / Postgres / Redshift)

An interactive terminal script that compares a BEFORE and an AFTER table -- on the same database engine or on two different ones, any combination of Snowflake, Postgres, and Redshift -- and writes a formatted 6-sheet Excel QA report.

## Why it can't be a single SQL JOIN

A same-engine comparison (e.g. Snowflake-to-Snowflake) could push the whole thing into one SQL `JOIN` and only ever download a small summary. That's not possible here: **no database engine can `JOIN` a table that lives in a different engine.** So this tool works differently:

1. Each side computes a **per-row checksum server-side** -- `MD5` of the compared columns, normalized (trimmed, NULL treated as a fixed sentinel) and concatenated in a fixed column order. The same algorithm runs on all three engines, so a checksum computed on Postgres is directly comparable to one computed on Snowflake.
2. Only `(join_key, checksum)` pairs come back to this machine -- small and fixed-size per row, regardless of how many columns you're comparing.
3. Comparing two lists of `(key, checksum)` runs as an **in-memory DuckDB join** (`:memory:` mode -- nothing is ever written to disk, no database file, no persisted temp data). This is meaningfully faster than a plain Python dictionary comparison at tens of millions of rows, and it's where "which rows differ" gets figured out without ever joining or downloading the full tables.
4. **Only** for the keys whose checksums disagree (or that are missing on one side) does the script pull full row detail -- batched and capped (`MISSING_RECORDS_CAP`, `CHECKSUM_BATCH_SIZE`). That row detail is then categorized into the NULL-transition buckets with another in-memory DuckDB query.

This keeps two core safety properties -- never download a full table, and never touch local disk -- but it is not as cheap as a same-engine SQL join: two live connections, a full checksum pass over every row on each side, and an in-memory DuckDB reconciliation step. Budget more time than a same-engine SQL join would take, especially on large tables -- DuckDB makes the reconciliation step itself fast even at tens of millions of rows, but the two independent full-table checksum scans on the source engines remain the dominant cost either way.

### Why DuckDB instead of local data export

An alternative design would bulk-export the compared columns from both sides to local Parquet files and let DuckDB do a real `JOIN` on the actual data -- faster still, and immune to the cross-engine formatting caveat below. That was deliberately **not** chosen here: it would mean every row's real values (names, emails, any PII) land on local disk on every run, not just the bounded set that actually differs. This tool keeps DuckDB's role limited to reconciling checksums and the already-bounded mismatched-row detail, entirely in memory -- the same data-minimization principle as the rest of the design, just executed faster.

### Known limitation: cross-engine checksum comparability

A checksum only matches across engines if both engines format a given value's text representation the same way after casting. This script normalizes what it reasonably can (whitespace, NULL handling, one hash algorithm everywhere), but genuine formatting differences between engines -- e.g. how a `NUMBER`/`numeric` renders its decimal places, or a timestamp's default string format -- can still surface as false "deviations" rather than real data differences. **If a run shows many deviations spread across most or all columns**, suspect formatting differences before concluding the data itself is wrong, and check a few sample pairs on the "NULL & Value-Change Patterns" sheet to see whether the before/after values are actually the same value in a different format.

### Known simplification: "both NULL" vs. "exact match" for rows that already matched at the checksum level

For a row whose overall checksum already matched, every compared column is known to be identical -- but not *how*: it could be that both sides are NULL for a given column, or that both sides hold the same real value. Telling those apart would mean pulling the real value for rows already known to match, which defeats the purpose of checksumming in the first place. So those rows are folded straight into each column's "exact match" tally, not split into "both NULL" vs. "exact match". This has no effect on deviation counts (computed exactly, only from the mismatched-checksum subset) -- it can only slightly under-count "both NULL" and over-count "exact match" on the NULL & Value-Change Patterns sheet.

### Cost on wide tables

There is no column-exclusion step -- every matched column is always compared. This directly affects the `CONCAT_WS(...)` + `MD5(...)` expression computed on every row of both tables, which is real compute work on the source engine, so a wider table costs proportionally more per row regardless of how many columns you actually care about.

The breakdown-column counts (Tenant Counts sheet) are computed in the *same* query as the checksums, not a separate `GROUP BY` pass -- using a breakdown column costs nothing extra in warehouse scans versus not using one. This matters because the checksum query is the single most expensive step in the whole run (a genuine full-table scan computing a hash over every compared column); avoiding a second one when a breakdown is requested is a real, not cosmetic, saving.

The per-column breakdown (Column Mismatch Summary and NULL & Value-Change Patterns sheets) always pulls full row detail for every mismatched key and counts exactly -- no sampling or estimation. On a table with both a very high mismatch count and many columns, this detail pull is the most expensive step in the whole run.

If a run comes back with a high checksum-mismatch rate (see the cross-engine formatting caveat above), that's a sign to check formatting before assuming the data itself is wrong. The mismatched-row detail pull batches its network round-trips at `CHECKSUM_BATCH_SIZE` keys per round-trip; a large mismatch count means many round-trips, each paying real network latency independent of query speed.

### Concurrency

BEFORE and AFTER are always independent connections to independent engines, so every step that touches both sides -- the row-count check, the checksum pass, the mismatched-row detail pull, and the missing-record detail pull -- runs both sides concurrently on a small thread pool instead of waiting on one side to finish before starting the other. This roughly halves wall-clock time on the two biggest full-table scans (the checksum pass on each side) versus running them sequentially, at no cost or accuracy tradeoff -- each side's query still runs to completion independently, only the *waiting* happens in parallel.

### A note on Snowflake compute cost specifically

Snowflake bills for warehouse-active time, not for how long the Python client takes to fetch results afterward. The actual dollar cost of the checksum pass is driven by how long the warehouse spends *computing* the checksum (a genuine full-table scan and hash over every compared column), not by how slowly this script pulls the already-computed `(key, checksum)` pairs back to your machine -- assuming your warehouse's auto-suspend is reasonably short, it can suspend (stop billing) while the client is still fetching, once the query itself has finished. Slow local fetching mostly costs you wall-clock time waiting, not necessarily extra warehouse dollars. Narrowing the compared columns (see above) is still the most direct lever over the actual compute cost, since it shrinks what the warehouse has to hash on every row.

## Requirements

- Python 3.9+
- Network access to whichever of Snowflake / Postgres / Redshift you're comparing
- Snowflake: SSO (`externalbrowser`) login
- Postgres / Redshift: host, port, database, username, and password for a role that can read `information_schema.columns` and the tables being compared

## Installation

```powershell
git clone https://github.com/dvaraprasadromala-sonata-coder/qa-report-builder-cli-cross-platform.git
cd qa-report-builder-cli-cross-platform
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```
(macOS/Linux/Git Bash: `source venv/bin/activate` instead of the `.ps1` line.)

Keep this venv separate from any other Python environment on your machine -- isolate this tool's dependencies (`snowflake-connector-python`, `psycopg2-binary`, `redshift_connector`, `openpyxl`, `duckdb`) from anything else you have installed.

### Setting up your `.env` (optional, but saves retyping connection details every run)

The tool works with zero setup beyond the venv above -- every credential can just be typed at the interactive prompts each time you run it. The `.env` file is purely a convenience so you don't have to retype the same Snowflake/Postgres/Redshift connection details on every run. **This repo ships with no real `.env` file** -- only [`.env.example`](.env.example), a template with blank values. You create your own local `.env` with your own credentials; nobody else's credentials are in this repo.

**Steps:**

1. Copy the template: `cp .env.example .env` (or on Windows, `copy .env.example .env`). This creates a new local file that's already gitignored (see [`.gitignore`](.gitignore)) -- it will never be committed or pushed, even by accident.
2. Open `.env` in a text editor and fill in the fields for whichever engine(s) you'll actually be comparing -- you don't need to fill in all three, only the ones you use. Leave the rest blank.
3. Where each value comes from:
   - **`SNOWFLAKE_ACCOUNT`** -- your Snowflake account identifier (the part before `.snowflakecomputing.com` in your Snowflake URL, e.g. `xy12345.us-east-1`). Ask whoever administers your Snowflake account if you don't know it.
   - **`SNOWFLAKE_USER`** -- your Snowflake username (usually your email).
   - **`SNOWFLAKE_ROLE`** -- a Snowflake role you have that can read `information_schema` and the tables you're comparing.
   - **`SNOWFLAKE_WAREHOUSE`** -- a Snowflake warehouse you have USAGE on.
   - **`POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_USER` / `POSTGRES_PASSWORD`** -- standard Postgres connection details for a role that can read `information_schema.columns` and the tables you're comparing. Get these from whoever manages the Postgres instance you're connecting to.
   - **`REDSHIFT_HOST` / `REDSHIFT_PORT` / `REDSHIFT_USER` / `REDSHIFT_PASSWORD`** -- same idea, for Redshift (Redshift's default port is `5439`, already filled in as the example default).
4. Save the file. That's it -- no restart, no build step. Next time you run the tool, whichever engine you pick at the "Engine" prompt has its saved fields used automatically instead of prompted for.

**How it behaves at runtime:** each block (`POSTGRES_*`, `REDSHIFT_*`, `SNOWFLAKE_*`) is keyed by **engine**, not by BEFORE/AFTER -- so if both sides of your comparison happen to use the same engine, you only need to fill that engine's block in once; it's reused automatically for both sides. Any field found in `.env` is used without prompting (printing a short `(using SNOWFLAKE_ROLE from .env)` note so it's never silently invisible), and any field you leave blank just falls back to the normal interactive prompt. **Engine, Database, Schema, and Table are always prompted interactively, every run, for both sides** -- there is no env var for those, since that's the part that actually changes between comparisons.

Worth knowing: if you set `POSTGRES_PASSWORD` or `REDSHIFT_PASSWORD`, that password sits in plaintext in your local `.env` file between runs, instead of being typed fresh via a masked prompt and held only in memory (the tool's default behavior with no `.env`). That file never leaves your machine (it's gitignored), but it's still a real trade-off worth knowing about -- leave that field blank if you'd rather it prompt you every time.

## Running it

```bash
python qa_report_builder_cross.py
```

### Prompts, in order

| Prompt | What to enter |
|---|---|
| BEFORE: Engine | `snowflake`, `postgres`, or `redshift` |
| BEFORE: Database | asked once, up front -- for Postgres/Redshift this is also the database the connection itself opens against, so it isn't asked twice |
| BEFORE: connection details | Snowflake: account / user / role / warehouse (SSO login). Postgres/Redshift: host / port / user / password. Any of these already set in `.env` (see below) are skipped |
| BEFORE: Schema, Table | two separate prompts |
| AFTER: Engine | same three connection prompts, independently -- can be the same engine as BEFORE or a different one |
| AFTER: Database, Schema, Table | three separate prompts |
| Join key column | type the exact column name directly -- the script doesn't print the matched-column list first; must be a column present on both sides, and should uniquely identify each row |
| Breakdown column *(optional)* | a column to group row counts by on the Tenant Counts sheet, e.g. `tenant_code` -- press Enter to skip |

Everything after that runs unattended and prints progress as it goes.

## What it compares

Columns are auto-matched by exact name (case-insensitive) between the two tables -- anything present on only one side is listed but excluded from comparison. For every matched column, present-in-both rows are categorized as: both NULL/empty, NULL→value, exact match, value→different value, value→NULL/empty. For each category, up to the top 5 distinct before/after value "sub-patterns" are shown (so a rare "value changed" case isn't drowned out by a common "NULL got backfilled" case in the same column), and for **each** sub-pattern, up to 5 real example records (with join-key values) are shown -- not just an aggregate count -- so you can look up an actual row.

## Report contents (`<after_table>_cross_qa_report.xlsx`)

Six sheets, in this order:

1. **Schema Comparison** -- every column from both tables, types side by side, a live formula flagging type mismatches, and a summary row. Note: cross-engine type names essentially never match exactly (e.g. Snowflake `NUMBER` vs. Postgres `numeric`) -- a `TYPE MISMATCH` here is expected and not itself a problem when BEFORE/AFTER are on different engines.
2. **Tenant Counts** -- row + distinct counts broken down by whatever column you gave at the optional prompt (or one `ALL ROWS` summary line if you skipped it), with live `DIFF`/`MATCH` formulas and a `TOTAL` row
3. **Overall Counts** -- table paths (with engine name), join key, row totals, present-in-both / missing counts
4. **Column Mismatch Summary** -- deviation count and % per column, with a blank `JUSTIFICATION` column for you to fill in after reviewing
5. **Missing Records** -- join-key value plus a few columns of context for rows present on only one side (capped sample; the sheet header states the true total even when it exceeds the sample)
6. **NULL & Value-Change Patterns** -- the full per-column, per-category breakdown described above, with a blank justification block under each deviating column

## Notes

- **By default, no credentials are stored anywhere.** Snowflake auth is SSO, entered fresh each run. Postgres/Redshift passwords are typed at a masked prompt (`getpass`) and held only in memory for the run -- never written to disk. This changes only if you opt into the `.env` file above and set a `*_PASSWORD` field, in which case that specific password sits in plaintext locally between runs -- see the `.env` section for the trade-off.
- `venv/`, `__pycache__/`, `.env`, and any generated `*.xlsx` report are gitignored -- see [`.gitignore`](.gitignore). Never commit those.
- The join key you choose should uniquely identify a row. If it isn't unique, the checksum-based comparison can be misleading (a duplicated key's checksum only represents whichever row the database happens to return for it) -- the script doesn't currently verify uniqueness, so pick carefully.
- Very large tables (tens of millions of rows or more): this script pulls every row's `(key, checksum)` pair into memory on both sides before comparing. That's small per row, but at extreme scale you may want to batch by key range rather than running one pass -- not currently implemented; ask if you need it.
- If dependency installation ever needs to change, update [`requirements.txt`](requirements.txt) and re-run `pip install -r requirements.txt` inside the activated venv.
