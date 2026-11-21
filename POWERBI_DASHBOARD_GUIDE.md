# Power BI Dashboard — Master Build Guide

This is the **one** Power BI dashboard for the whole project. It grows by
one or two pages per assignment — you do **not** build a new dashboard
each time. Every assignment's big-data extension script writes its CSVs
into the same shared folder, `results/powerbi/`, at the project root
(never inside an individual `assignmentN_.../` folder), with filenames
prefixed by which milestone produced them (`m3_`, `m4_`, `m5_`, `m6_`, and
so on for every assignment still to come). That prefix convention is what
lets files from every assignment sit in one folder without colliding.

> Power BI Desktop files (`.pbix`) are a binary format created by the
> Power BI Desktop application itself — no script can generate one. This
> guide is the exact recipe for building it by hand, once, and then just
> refreshing/extending it as each assignment adds new files.

## How this stays consistent, assignment to assignment

Every assignment's big-data extension script (`assignment2_bigdata_extension.py`,
`assignment3_bigdata_extension.py`, and so on) is built the same way, through
the same three shared modules:

| Module | Job |
|---|---|
| `common/spark_utils.py` | Starts Spark the same way every time; runs a distributed groupBy/aggregate |
| `common/big_data_layer.py` | Generates each assignment's 100,000-row dataset(s) and runs its Spark batch layer |
| `common/powerbi_export.py` | Writes every assignment's CSVs into the ONE shared `results/powerbi/` folder |

This means: **you only connect Power BI to `results/powerbi/` once.** Every
later assignment's extension script just adds more files to that same
folder — on your next "Refresh" in Power BI, or when you add the new file
as a new table, the dashboard grows without anything earlier breaking.

## Prerequisites

* Power BI Desktop installed
* You have run each assignment's big-data extension script at least once:
  - `python -m assignment2_m3_m4.assignment2_bigdata_extension`
  - `python -m assignment3_m5_m6.assignment3_bigdata_extension`
  - (repeat for each future assignment's equivalent script)

## One-time setup: connect Power BI to the shared folder

1. Power BI Desktop → **Get Data** → **Folder**.
2. Browse to `results/powerbi/` at the project root (NOT inside any
   `assignmentN_.../` subfolder) and select it.
3. Power BI shows a list of every CSV in that folder. **Do NOT click
   "Combine & Transform Data".** That button forces every file into the column
   layout of the FIRST file, and the files in this folder deliberately have
   different columns (see the data dictionary below), so it would scramble them.
   Instead click **Transform Data**, and in the Power Query Editor keep one row
   per file you need (filter the `Name` column), then click the **Content**
   cell's "Binary" link for each. The simplest path for beginners: skip the Folder
   connector and use **Get Data -> Text/CSV** once per file; every file keeps its
   own table with its own columns.

Either approach is fine as long as every file is loaded as its OWN table. What
matters is that all files live under the one `results/powerbi/` path, so nothing
needs reconnecting later.

## Assignment 2 pages (Milestones 3 & 4)

**Scalability (M3)** — uses `m3_scalability_results.csv`:
- Clustered Column Chart: Axis = `n_edge_nodes`, Values = `packet_loss_rate`
- Line Chart: Axis = `n_edge_nodes`, Values = `throughput_tasks_per_sec` and `mean_latency_ms`

**Coordination (M4)** — uses `m4_election_results.csv`:
- Table: all columns (scenario, initiator, elected_leader, rounds, total_messages, elapsed_ms, consistent_across_all_nodes)
- Clustered Bar Chart: Axis = `initiator`, Values = `total_messages`

**Architecture (M3)** — uses `m3_architecture_services.csv`:
- Matrix: Rows = `node_type` then `node`, Values = `service`

**Batch Layer — 100K rows (M3, Spark)** — uses `batch_summary_by_service_class.csv`
and `batch_summary_by_node.csv` (in `results/bigdata/`, imported the same way):
- Donut Chart: Legend = `service_class`, Values = `task_count`
- Clustered Column Chart: Axis = `assigned_node`, Values = `total_cpu_demand` and `total_bandwidth_demand_mbps`

**Speed Layer (Live Sample)** — uses `m3_speed_layer_summary.csv` and `m3_speed_layer_task_log.csv`:
- Cards for throughput/latency/jitter/loss (pivot the metric/value table first: Transform Data → select `metric` column → Transform → Pivot Column, values from `value`)
- Stacked Bar Chart: Axis = `node`, Legend = `status`, Values = Count of `status`

## Assignment 3 pages (Milestones 5 & 6)

**Transactions — Batch (M5, Spark, 100K rows)** — uses
`m5_batch_transactions_by_scenario.csv` and `m5_batch_transactions_by_pair_scenario.csv`:
- Clustered Column Chart: Axis = `scenario`, Values = `predicted_commit_rate` — shows the
  `node_down` scenario's commit rate collapsing to 0 at 100,000-row scale,
  the same finding the live experiment showed at small scale.
- Table: `pair`, `scenario`, `attempt_count`, `total_bandwidth_requested_mbps`

**Transactions — Live (M5)** — uses `m5_live_transaction_log.csv` and `m5_live_transaction_summary.csv`:
- Card: `commit_rate`
- Stacked Bar Chart: Axis = `participant_edge`, Legend = `decision`, Values = Count

**Deadlock — Batch Contention (M6, Spark, 100K rows)** — uses
`m6_batch_lock_contention_by_resource.csv`:
- Clustered Bar Chart: Axis = `resource` (or `node` + `resource` combined),
  Values = `request_count` — ranks which resources see the heaviest
  simulated contention across 100,000 lock requests.

**Deadlock — Live Detection (M6)** — uses `m6_live_wait_for_graph.csv` and `m6_live_deadlock_summary.csv`:
- Table: `waiting_tx`, `waiting_for_tx` — the actual wait-for graph edges
  from the live detected deadlock (should show the 2-edge cycle:
  Tx-A-bd → Tx-B-bd and Tx-B-bd → Tx-A-bd).
- Card: `cycles_found`, `victim_chosen`, `detection_elapsed_ms`

## Practical 1 pages (Milestones 7 & 8)

**Fault Tolerance — Live (M7)** — uses `m7_live_failure_comparison.csv`:
- Clustered Column Chart: Axis = `failure_type`, Legend = `fault_tolerance`, Values =
  `availability` — the headline picture: ~60% availability without fault tolerance
  versus 100% with it.
- Clustered Column Chart: Axis = `failure_type`, Legend = `fault_tolerance`, Values =
  `incorrect_results_accepted` — shows the Byzantine fault silently corrupting
  ~80 results without quorum voting, and 0 with it.
- Table: `failure_type`, `detected_as`, `detection_time_s`, `requests_needing_failover`, `p95_latency_ms`
  (filter `fault_tolerance` = "with FT").

**Fault Tolerance — Leader Failure (M7)** — uses `m7_live_leader_failure.csv`:
- Table (metric / value): detection time, re-election time, new leader, whether all nodes agree.

**Fault Tolerance — Batch (M7, Spark, 100K events)** — uses `m7_batch_detection_by_failure_type.csv`:
- Clustered Column Chart: Axis = `failure_type`, Values = `mean_detection_s`, `p95_detection_s`,
  `p99_detection_s` — the full detection-time distribution at 100,000-event scale.
- Table: `m7_model_validation.csv` (predicted vs measured detection time and failover
  requests) — the evidence that the batch model matches reality.
- Table: `m7_batch_availability_projection.csv` and a Line Chart from `m7_availability_model.csv`
  (Axis = `replicas`, Legend = `single_replica_availability`, Values = `system_availability`).

**System Models — Live (M8)** — uses `m8_live_scalability.csv`, `m8_live_reliability.csv`,
`m8_live_model_summary.csv`, `m8_live_scorecard.csv`:
- Line Chart: Axis = `clients`, Legend = `model`, Values = `throughput_per_s` (scalability).
- Clustered Column Chart: Axis = `model`, Legend = `crashed_node`, Values = `success_rate`
  (reliability under each single-node crash).
- Clustered Bar Chart: Axis = `model`, Values = `weighted_score` and `equal_weight_score`
  (the final scorecard).
- Table: `m8_live_model_summary.csv` (latency, throughput, utilization, cost per model).

**System Models — Batch (M8, Spark, 100K requests x 5 models)** — uses
`m8_batch_latency_by_model_class.csv`, `m8_batch_tier_share_by_model.csv`:
- Clustered Column Chart: Axis = `model`, Values = `sla_compliance` (filter `service_class` = "URLLC").
- Stacked Bar Chart: Axis = `model`, Legend = `tier`, Values = `share_of_model_requests` —
  shows which tier (edge/RAN/core/cloud) each model actually uses.

## Practical 2 pages (Milestones 9 & 10)

**Transparency - Access and Location (M9)** - `m9_live_access_transparency.csv`, `m9_live_location_transparency.csv`:
- Clustered Column Chart: Axis = `node`, Legend = `access_method`, Values = `successful_records` -
  the legacy node gets 0 of 100 by direct access and 100 of 100 through the adapter.
- Table / Card: `machine_names_in_application_code` (0) and `directory_lookup_us`; a Column Chart of
  `reads_served` by `served_by_node` with `phase` as legend shows the reads moving from edge-2 to cloud-1.

**Transparency - Replication and Overhead (M9)** - `m9_live_replication_overhead.csv`, `m9_live_overhead_summary.csv`:
- Clustered Column Chart: Axis = `configuration`, Values = `put_mean_ms` and `get_mean_ms`.
- Bar Chart: Axis = `mechanism`, Values = `put_overhead_ms` (the "price of transparency").

**Transparency - Migration (M9)** - `m9_live_migration.csv`:
- Clustered Column Chart: Axis = `keys`, Legend = `mode`, Values = `max_write_latency_ms`.
- Clustered Column Chart: Axis = `keys`, Legend = `mode`, Values = `migration_seconds`
  (live is slower but never freezes writes). `stalled_writes` is capped by the number of client
  threads; prefer `max_write_latency_ms`.

**Transparency - Failure (M9)** - `m9_live_failure_transparency.csv`:
- Clustered Column Chart: Axis = `client`, Values = `availability`, then a second one for `lost_reads`.

**Transparency - Batch (M9, Spark, 100K operations)** - `m9_batch_availability_by_replication.csv`,
`m9_batch_migration_downtime.csv`:
- Line Chart: Axis = `p_down`, Legend = `replicas`, Values = `availability`; add `analytic_availability` to compare with the formula.
- Line Chart: Axis = `replicas`, Legend = `p_down`, Values = `degraded_writes` (repair work created).
- Column Chart: Axis = `keys`, Values = `stop_the_world_freeze_s` against `live_migration_duration_s`
  (set the axis to Log scale); a Card for `live_blocked_writes` (always 0).

**Naming - Live (M10)** - `m10_live_resolution.csv`, `m10_live_names_scaling.csv`, `m10_live_naming_scorecard.csv`:
- Clustered Column Chart: Axis = `scheme`, Values = `mean_latency_us` (use Log scale: values span 0.1 to 670).
- Clustered Column Chart: Axis = `scheme`, Values = `stale_right_after_move`, `stale_after_update`, `stale_after_ttl`.
- Column Chart: Axis = `scheme`, Values = `resolution_success_when_core1_dead` (the single-point-of-failure picture).
- Clustered Bar Chart from the scorecard: Axis = `scheme`, Values = the four `score_*` columns (the ranking depends on the variant).

**Naming - Batch (M10, Spark, 100K lookups)** - `m10_batch_dns_ttl_tradeoff.csv`, `m10_batch_naming_scalability.csv`:
- Combo (Line and Column) Chart: Axis = `ttl_s`, Columns = `stale_answers`, Line = `exchanges_per_lookup`.
- Line Chart: Axis = `nodes` (as text, so the points are evenly spaced), Legend = `scheme`, Values = `busiest_server_utilization`;
  add a constant line at 1.0 (100%) - anything above is overloaded.
- Column Chart: Axis = `scheme`, Values = `exchanges_per_1000_lookups_and_10_moves`, filtered to `nodes` = 5000 (Log scale).

**Process Management (M10)** - `m10_live_process_lifecycle.csv`, `m10_live_scheduling_policies.csv`,
`m10_live_process_creation_throughput.csv`, `m10_live_process_coordination.csv`:
- Bar Chart: Axis = `stage`, Values = `mean_ms` and `p95_ms`.
- Clustered Column Chart: Axis = `policy`, Values = `urllc_mean_distance_rtt_ms`, `mean_distance_rtt_ms`; a second one for `load_balance_jain_index`.
- Table: `m10_live_process_coordination.csv` (scenario, outcome, success, failed_locates, resources_leaked).

## Data dictionary: the 12 Practical 1 files (m7_ and m8_)

The files are **different tables with different columns on purpose**: each has
its own "one row per ..." grain. Within each file, every column holds a single
type (numbers are numbers, text is text). Power BI will detect them correctly.

| File | One row per | Key columns | Notes |
|---|---|---|---|
| `m7_live_failure_comparison` | failure type x (with / without fault tolerance) | `failure_type`, `fault_tolerance` | `detection_time_s`, `detected_as`, `node_mttr_s` are **intentionally blank** where not applicable (no detector, or not a crash) |
| `m7_live_leader_failure` | one measured fact | `metric`, `value` | `value` is **text** (it mixes numbers, names and True/False). Use it as a Table, or pivot `metric` into columns first |
| `m7_availability_model` | replica count x single-replica availability | `single_replica_availability`, `replicas` | Maths table, no blanks |
| `m7_batch_detection_by_failure_type` | failure type (Spark, 100,000 events) | `failure_type` | Times in seconds |
| `m7_model_validation` | failure type | `failure_type` | Predicted vs measured |
| `m7_batch_availability_projection` | strategy | `strategy` | Assumes `assumed_faults_per_hour` |
| `m8_live_scalability` | model x load level | `model`, `clients` | |
| `m8_live_reliability` | model x crashed node | `model`, `crashed_node` | |
| `m8_live_model_summary` | model | `model` | One wide row per model |
| `m8_live_scorecard` | model | `model` | |
| `m8_batch_latency_by_model_class` | model x service class (Spark) | `model`, `service_class` | `requests` here = number of requests in that group |
| `m8_batch_tier_share_by_model` | model x tier (Spark) | `model`, `tier` | |

**Naming conventions that are the same everywhere:** time columns end in `_s`
(seconds) or `_ms` (milliseconds); rates and shares are 0-1 fractions (multiply by
100 for percent). **Join keys use identical values across files:** all six
`m8_` files use the same five `model` names, and all three `m7_` files that have
`failure_type` use the same five values (checked). Related columns that look alike
but mean different things: `requests` in `m8_live_scalability` is the number of
requests *offered* in that run, while in `m8_batch_*` it is the number in that group.

**To filter several tables with one slicer**, create a small `Models` table
(Home -> Enter Data, one column `model` with the five names) and create a
relationship from it to the `model` column of each m8 table.

## Data dictionary: the 17 Practical 2 files (m9_ and m10_)

Again different tables with different columns on purpose; every column holds a single type
(checked: no mixed-type columns). Blank cells mean "not applicable" (for example `repair_seconds` for
the client that has no repair, `cache_hit_rate` for schemes with no cache, `duration_s` for an aborted group).

| File | One row per | Key columns | Notes |
|---|---|---|---|
| `m9_live_access_transparency` | access method x replica node | `access_method`, `node` | `successful_records` out of `of_records` |
| `m9_live_location_transparency` | phase x serving node | `phase`, `served_by_node` | `machine_names_in_application_code` repeats on every row |
| `m9_live_replication_overhead` | configuration | `configuration` | Median of 5 rounds |
| `m9_live_overhead_summary` | mechanism | `mechanism` | Differences between the rows above |
| `m9_live_migration` | database size x method | `keys`, `mode` | `mode` = stop_the_world or live |
| `m9_live_failure_transparency` | client type | `client` | 2 rows |
| `m9_batch_availability_by_replication` | outage probability x copies (Spark) | `p_down`, `replicas` | `analytic_availability` is the formula 1-p^copies |
| `m9_batch_migration_downtime` | database size (Spark) | `keys` | Up to 2,000,000 records |
| `m10_live_resolution` | naming scheme | `scheme` | One wide row per scheme |
| `m10_live_names_scaling` | number of names | `names` | |
| `m10_live_naming_scorecard` | naming scheme | `scheme` | Four score variants (weighted/equal x linear/log) |
| `m10_live_process_lifecycle` | lifecycle stage | `stage` | |
| `m10_live_scheduling_policies` | policy | `policy` | |
| `m10_live_process_creation_throughput` | number of concurrent creators | `concurrent_creators` | |
| `m10_live_process_coordination` | scenario | `scenario` | `success`, `resources_leaked` are True/False |
| `m10_batch_dns_ttl_tradeoff` | TTL (Spark) | `ttl_s` | |
| `m10_batch_naming_scalability` | scheme x node count (Spark) | `scheme`, `nodes` | `overloaded` = busiest server at 100% or more |

**Shared keys:** the `scheme` column uses identical labels in all three files that have it
(`m10_live_resolution`, `m10_live_naming_scorecard`, `m10_batch_naming_scalability`), so one slicer
or relationship works across them. Tip: for a slicer on `nodes`, set the column's data category to Text so it plots evenly spaced.

## Pattern for every future assignment

When a new assignment's extension script runs (next: Practical 2,
`practical2_bigdata_extension.py`), it will write new `m9_*.csv` /
`m10_*.csv` files into the exact same `results/powerbi/` folder. To add its
pages: **Home → Refresh** picks up any files that existed when you first
connected; for genuinely NEW files, use **Get Data → Text/CSV** once per
new file (or re-point the Folder connector at the same path, which will
pick up new files automatically) and build new chart pages the same way
as above. The dashboard never needs to be rebuilt from scratch.

## Save and share

Save as `5g6g_platform_dashboard.pbix` at the project root (not inside any
assignment subfolder, since it now covers multiple assignments). Commit
and push it to GitHub alongside the code and reports, same as any other
file — see your existing Source Control workflow.
