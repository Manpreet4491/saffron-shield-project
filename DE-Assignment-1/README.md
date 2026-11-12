# Saffron Shield Claims Lakehouse — DE Assignment 1

A Unity Catalog lakehouse for a health-insurance claims operation, built in layers on the `claims` catalog: raw reference data → bronze (ingested claim events) → silver (cleaned claims, quarantine, SCD2 policy history) → gold (point-in-time claim facts + monthly hospital summary).

Everything below is a runbook: run the steps in order, and after each step run the **Confirm** query to prove it worked before moving on.

## Repo layout

| Path | Purpose |
| --- | --- |
| `00_setup` | Catalog, schemas, volume + small reference tables (R1 groundwork) |
| `01_ingestion` | Reference tables + Auto Loader stream into bronze (R1, R2) |
| `claims_pipeline_/` | Lakeflow Spark Declarative Pipeline: clean + quarantine (R3) |
| `02_cleaning` | SCD2 policy-history dimension from CDC events (R4) |
| `03_gold` | Point-in-time gold tables + Delta internals (R5, R7) |
| `README.md` | This runbook |

## Prerequisites

* A Databricks workspace with Unity Catalog and serverless compute (Spark Connect).
* Source files already in workspace storage under `/Volumes/claims/raw/landing/raw/`:
  * `members.csv`, `hospitals.csv`, `plans.csv`, `hospital_network_history.csv`
  * `claims_raw/*.json` — claim events (3000 claims)
  * `policy_changes_cdc/*.json` — policy CDC events (661 events: 500 INSERT / 151 UPDATE / 10 DELETE)
* Attach any serverless SQL/Python compute to each notebook before running.

The notebooks take a `catalog` widget/parameter (default `claims`). Using a different catalog name works end to end, but the volume path `/Volumes/claims/raw/...` is hardcoded in the ingestion steps, so `claims` is the safe default.

## Build order

### Step 0 — `00_setup`: catalog, schemas, volume, plans & hospitals

Run all cells. This creates `claims.raw`, `claims.bronze`, `claims.silver`, `claims.gold`, the `landing` volume, and loads `claims.raw.plans` and `claims.raw.hospitals` from CSV via `read_files`.

**Confirm:**

```sql
SELECT COUNT(*) FROM claims.raw.plans;      -- 4 plan codes
SELECT COUNT(*) FROM claims.raw.hospitals;  -- 40 hospitals, city_tier never null
```

### Step 1 — `01_ingestion`: reference data + bronze claims stream (R1, R2)

Run the cells in order:

1. Load `claims.raw.members` and `claims.raw.hospital_network_history` from CSV.
2. **R2:** an Auto Loader (`cloudFiles`) stream reads `claims_raw/*.json`, adds `_source_file` and `_ingest_ts`, and appends into `claims.bronze.claims` with `trigger(availableNow=True)` and a checkpoint under `/Volumes/claims/raw/landing/raw/`.

**Confirm:**

```sql
SELECT COUNT(*) FROM claims.bronze.claims;                    -- 3000 (3000 distinct claim_ids)
SELECT COUNT(*) FROM claims.raw.members;
SELECT COUNT(*) FROM claims.raw.hospital_network_history;     -- 41 network windows
```

### Step 2 — `claims_pipeline_`: clean + quarantine (R3)

Start the Lakeflow Spark Declarative Pipeline `claims_pipeline_` (pipeline ID `938bc969-5d45-4d14-9ebd-96cd48352dda`) and run a full update. It publishes to `claims.silver`:

* `claims_clean` — deduplicated (latest `_ingest_ts` per `claim_id`), date-parsed claims that pass all validation rules.
* `claims_quarantine` — rejected rows tagged with failed rules:
  * **Q-A** missing referenced ids · **Q-B** bad/out-of-range dates · **Q-C** non-positive billed or approved > billed · **Q-D** status contradicting its own fields (rejection_code/settled_ts/approved_amount) · **Q-E** `room_days` ≠ `datediff(discharge, admission)`

**Confirm:** nothing is lost — clean + quarantine = bronze:

```sql
SELECT (SELECT COUNT(*) FROM claims.silver.claims_clean)     AS clean,      -- 2695
       (SELECT COUNT(*) FROM claims.silver.claims_quarantine) AS quarantine; -- 305  (2695 + 305 = 3000)

SELECT failed_rules, COUNT(*) FROM claims.silver.claims_quarantine GROUP BY failed_rules;
-- [Q-E] 289, [Q-C] 6, [Q-D] 6, [Q-A] 3, [Q-A,Q-E] 1
```

### Step 3 — `02_cleaning`: SCD2 policy history (R4)

Run the R4 cell. It reads `policy_changes_cdc/*.json`, orders events per `(member_id, policy_id)` by `effective_from` (business time, not file arrival order), and publishes `claims.silver.dim_policy_scd2`:

* `valid_from` = event date, `valid_to` = day before the next event, `is_current` on the open row.
* DELETE events close the prior row and never become rows of their own.
* `policy_sk` = md5 of natural key + version date (deterministic surrogate key).

**Confirm:** the cell itself ends with assertion-based sanity checks (row counts, unique `policy_sk`, one open row per live policy, no gaps/overlaps). It prints `All SCD2 invariants verified.`:

```sql
SELECT COUNT(*) FROM claims.silver.dim_policy_scd2;                 -- 651 (661 events − 10 DELETEs)
SELECT COUNT(*) FROM claims.silver.dim_policy_scd2 WHERE is_current; -- 490 (500 policies − 10 deleted)
```

### Step 4 — `03_gold`: point-in-time gold + Delta internals (R5, R7)

Run the cells in order:

1. **R5 — `claims.gold.claims_enriched`** (2695 rows). Facts only: each claim carries the plan and network facts that applied **on its own admission date** — the SCD2 version whose `[valid_from, valid_to]` window contains `admission_date`, the plan family/room-rent cap of that plan, and network membership from `hospital_network_history` windows. Derived facts: `policy_start_date`, `policy_tenure_months`, `length_of_stay`, `room_rent_over_cap`, `turnaround_days` (empty until settled), `hospital_city`, `hospital_city_tier`. No payable/eligibility/waiting-period verdict columns — scope explicitly excludes them.
2. **R5 — `claims.gold.hospital_monthly`** (467 rows): `hospital_id`, `admission_month`, `claim_count`, `total_billed`, `total_approved` (0, never NULL), `rejected_count`.
3. **Verification cell** asserts exact column list (no more, no fewer), forbidden columns absent, spec types, and that `hospital_monthly` recomputed from `claims_enriched` matches exactly.
4. **R7 — Delta internals:** `DESCRIBE HISTORY` (the table's write history), time travel via `versionAsOf` (pull an earlier version when claim numbers are disputed), and `DESCRIBE DETAIL` showing the liquid clustering.

`claims_enriched` is **clustered by `(admission_month, hospital_id)`** — liquid clustering, so new claim batches are clustered incrementally without a full rewrite.

**Confirm:**

```sql
SELECT COUNT(*) FROM claims.gold.claims_enriched;     -- 2695
SELECT COUNT(*) FROM claims.gold.hospital_monthly;    -- 467
SELECT SUM(claim_count) FROM claims.gold.hospital_monthly;  -- 2695 = total claims

DESCRIBE DETAIL claims.gold.claims_enriched;  -- clusteringColumns: [admission_month, hospital_id]
DESCRIBE HISTORY claims.gold.claims_enriched; -- version 1 WRITE (insert), version 0 CREATE TABLE
```

## Key design decisions

* **Quarantine, don't drop.** Bad rows keep their provenance with tagged failure rules; clean + quarantine always reconciles to bronze.
* **Business-time SCD2.** CDC events are ordered by `effective_from`, not file arrival order, so out-of-order file delivery can't corrupt the history. DELETEs terminate history rather than becoming versions.
* **Facts-only gold.** `claims_enriched` records what was true at admission time (plan, network, caps). Payability/eligibility decisions are deliberately excluded — they change independently and would force needless rebuilds.
* **Two implementations of the monthly summary agree.** `hospital_monthly` (SQL, this repo) is designed to be reproduced by a visual no-code builder (R6); both must match exactly, down to how a hospital/month with no approvals is represented (`total_approved = 0`, never NULL).

## Submission

Commit evidence files under `evidence/` (including `evidence/scorecard.json` from the grader run), then tag the final commit `submission`.