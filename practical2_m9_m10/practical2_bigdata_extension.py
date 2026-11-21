"""
================================================================================
PRACTICAL 2 EXTENSION - Big Data Integration + Power BI Dashboard
================================================================================
System: Distributed 5G/6G Network Service Platform (Theme 1)
Extends: Milestone 9 (Transparency and Reliability) and Milestone 10 (Naming and
         Distributed Process Management)

Same framework and pattern as every earlier extension: shared modules
(common/spark_utils.py, common/big_data_layer.py, common/powerbi_export.py), the
same Lambda Architecture, files prefixed m9_ / m10_ written to the ONE shared
results/powerbi/ folder, feeding the ONE growing Power BI dashboard.

    [BATCH LAYER]   Apache Spark processes
                      * 100,000 client operations -> 1.2 million replication
                        evaluations and 600,000 migration evaluations (M9)
                      * 100,000 name lookups -> the DNS time-to-live trade-off, and
                        2 million evaluations of five naming schemes from 5 to
                        5,000 nodes (M10)
                          |
    [SPEED LAYER]    The live transparency, naming and process-management experiments
                     (their saved results are reused; re-run only if missing). Their
                     MEASURED costs calibrate the batch models.
                          |
    [SERVING LAYER]  Batch + live results merged into results/powerbi/m9_*.csv, m10_*.csv
                          |
    [PRESENTATION]   The SAME Power BI dashboard gains new pages
                     (see the project-root POWERBI_DASHBOARD_GUIDE.md)

Run:
    python -m practical2_m9_m10.practical2_bigdata_extension
================================================================================
"""

import sys
import os
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.big_data_layer import run_transparency_batch_layer, run_naming_batch_layer
from common.powerbi_export import write_csv, POWERBI_DIR

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)
M9_JSON = os.path.join(RESULTS_DIR, "milestone9_transparency.json")
M10_JSON = os.path.join(RESULTS_DIR, "milestone10_naming_process.json")
N_ROWS = 100_000


def log(tag, msg):
    print(f"[{tag}] {msg}")


def load_or_run_live():
    if os.path.exists(M9_JSON):
        log("SPEED", "Using saved live Milestone 9 results.")
        m9 = json.load(open(M9_JSON))
    else:
        log("SPEED", "Milestone 9 results not found -- running the live transparency experiments now ...")
        from practical2_m9_m10.milestone9_transparency import run as run_m9
        m9 = run_m9()
    if os.path.exists(M10_JSON):
        log("SPEED", "Using saved live Milestone 10 results.")
        m10 = json.load(open(M10_JSON))
    else:
        log("SPEED", "Milestone 10 results not found -- running the live naming/process experiments now ...")
        from practical2_m9_m10.milestone10_naming_process import run as run_m10
        m10 = run_m10()
    return m9, m10


# ---------------------------------------------------------------------------
# [SERVING LAYER] Milestone 9
# ---------------------------------------------------------------------------

def export_m9(m9, batch):
    acc = m9["access"]
    rows = [{"access_method": "physical addressing (no transparency)", "node": n, "wire_format": v["wire_format"],
             "successful_records": v["writes_ok"], "of_records": v["of"]} for n, v in acc["physical_addressing"].items()]
    rows += [{"access_method": "transparent (access adapter)", "node": n, "wire_format": v["wire_format"],
              "successful_records": v["records_correct"], "of_records": v["of"]}
             for n, v in acc["transparent"]["per_replica"].items()]
    write_csv("m9_live_access_transparency.csv", rows)

    loc = m9["location"]
    rows = []
    for phase, key in (("before relocation", "before_relocation"), ("after relocation", "after_relocation")):
        r = loc[key]
        for node, count in r["served_by"].items():
            rows.append({"phase": phase, "served_by_node": node, "reads_served": count,
                         "correct_reads_in_phase": r["read_back_correct"], "records_written": r["written"],
                         "machine_names_in_application_code": loc["machine_names_in_application_code"],
                         "directory_lookup_us": loc["directory_lookup_us"]})
    write_csv("m9_live_location_transparency.csv", rows)

    rows = [{"configuration": label, **v} for label, v in m9["replication"]["configs"].items()]
    write_csv("m9_live_replication_overhead.csv", rows)
    write_csv("m9_live_overhead_summary.csv", m9["replication"]["overhead"])
    write_csv("m9_live_migration.csv", m9["migration"])
    write_csv("m9_live_failure_transparency.csv", m9["failure"])
    write_csv("m9_batch_availability_by_replication.csv", batch["availability_by_replication"])
    write_csv("m9_batch_migration_downtime.csv", batch["migration_downtime"])
    log("SERVING", "Exported 8 Milestone 9 files (m9_live_access_transparency, m9_live_location_transparency, "
                    "m9_live_replication_overhead, m9_live_overhead_summary, m9_live_migration, "
                    "m9_live_failure_transparency, m9_batch_availability_by_replication, m9_batch_migration_downtime)")


# ---------------------------------------------------------------------------
# [SERVING LAYER] Milestone 10
# ---------------------------------------------------------------------------

def export_m10(m10, batch):
    nm, pr = m10["naming"], m10["process_management"]
    rows = []
    for s, lat in nm["latency"].items():
        st, ft = nm["staleness"][s], nm["fault_tolerance"][s]
        rows.append({"scheme": lat["scheme"], "mean_latency_us": lat["mean_latency_us"], "p95_latency_us": lat["p95_latency_us"],
                     "exchanges_per_lookup": lat["exchanges_per_lookup"], "cache_hit_rate": lat["cache_hit_rate"],
                     "lookups_per_second_16_clients": nm["throughput"][s]["lookups_per_second"],
                     "stale_right_after_move": st["stale_before_update"], "stale_after_update": st["stale_after_update"],
                     "stale_after_ttl": st["stale_after_ttl"], "update_exchanges_per_move": st["update_exchanges_per_move"],
                     "resolution_success_when_core1_dead": ft["resolution_success_with_core1_dead"]})
    write_csv("m10_live_resolution.csv", rows)
    write_csv("m10_live_names_scaling.csv", nm["names_scaling"])

    labels = {s: v["scheme"] for s, v in nm["latency"].items()}
    sc, rk = nm["scorecards"], nm["rankings"]
    rows = []
    for s in sc["weighted_linear"]:
        rows.append({"scheme": labels[s], "score_weighted_linear": sc["weighted_linear"][s],
                     "score_weighted_log": sc["weighted_log"][s], "score_equal_linear": sc["equal_linear"][s],
                     "score_equal_log": sc["equal_log"][s],
                     "rank_weighted_linear": rk["weighted_linear"].index(s) + 1, "rank_weighted_log": rk["weighted_log"].index(s) + 1,
                     "rank_equal_linear": rk["equal_linear"].index(s) + 1, "rank_equal_log": rk["equal_log"].index(s) + 1})
    write_csv("m10_live_naming_scorecard.csv", rows)

    write_csv("m10_live_process_lifecycle.csv",
              [{"stage": k, **v} for k, v in pr["lifecycle"]["stages"].items()])
    write_csv("m10_live_scheduling_policies.csv", [{"policy": k, **v} for k, v in pr["policies"].items()])
    write_csv("m10_live_process_creation_throughput.csv", pr["creation_throughput"])

    g, mg, rs = pr["group"], pr["migration"], pr["respawn"]
    coord = [
        {"scenario": "group start (Two-Phase Commit)", "outcome": g["normal_group"]["status"], "success": g["normal_group"]["status"] == "started",
         "duration_s": round(g["normal_group"]["twopc_ms"] / 1000, 5), "failed_locates": 0, "processes_affected": 3,
         "resources_leaked": g["normal_group"]["leak_check_after_terminate"]["max_cpu_utilization_after"] != 0.0},
        {"scenario": "group start forced to abort (a node votes no)", "outcome": g["forced_abort"]["status"],
         "success": g["forced_abort"]["status"] == "aborted" and g["forced_abort"]["group_processes_running_anywhere"] == 0,
         "duration_s": None, "failed_locates": 0, "processes_affected": g["forced_abort"]["group_processes_running_anywhere"],
         "resources_leaked": not g["forced_abort"]["no_resources_leaked"]},
        {"scenario": "move process: make-before-break", "outcome": "moved", "success": mg["make_before_break"]["failed_locates"] == 0,
         "duration_s": mg["make_before_break"]["migration_seconds"], "failed_locates": mg["make_before_break"]["failed_locates"],
         "processes_affected": 1, "resources_leaked": False},
        {"scenario": "move process: break-before-make", "outcome": "moved", "success": mg["break_before_make"]["failed_locates"] == 0,
         "duration_s": mg["break_before_make"]["migration_seconds"], "failed_locates": mg["break_before_make"]["failed_locates"],
         "processes_affected": 1, "resources_leaked": False},
        {"scenario": "node crash: automatic respawn", "outcome": "restored" if rs["failed_to_restore"] == 0 else "partly restored",
         "success": rs["restored"] == rs["processes_on_victim"] and rs["names_resolving_to_a_live_running_process"] == rs["names_total"],
         "duration_s": rs["all_restored_after_seconds"], "failed_locates": rs["names_total"] - rs["names_resolving_to_a_live_running_process"],
         "processes_affected": rs["processes_on_victim"], "resources_leaked": False},
    ]
    write_csv("m10_live_process_coordination.csv", coord)
    write_csv("m10_batch_dns_ttl_tradeoff.csv", batch["dns_ttl_tradeoff"])
    # use the SAME scheme labels as the live files, so one slicer / relationship works across both
    write_csv("m10_batch_naming_scalability.csv", [{**r, "scheme": labels[r["scheme"]]} for r in batch["scalability"]])
    log("SERVING", "Exported 9 Milestone 10 files (m10_live_resolution, m10_live_names_scaling, m10_live_naming_scorecard, "
                    "m10_live_process_lifecycle, m10_live_scheduling_policies, m10_live_process_creation_throughput, "
                    "m10_live_process_coordination, m10_batch_dns_ttl_tradeoff, m10_batch_naming_scalability)")


def main():
    m9, m10 = load_or_run_live()

    stw = [r for r in m9["migration"] if r["mode"] == "stop_the_world"]
    c_stw = sum(r["migration_seconds"] for r in stw) / sum(r["keys"] for r in stw)
    log("BATCH", f"[BIG DATA] Apache Spark over {N_ROWS:,} client operations (M9); measured stop-the-world copy cost "
                  f"= {c_stw*1e6:.1f} microseconds per record ...")
    b9 = run_transparency_batch_layer(n_rows=N_ROWS, stop_the_world_s_per_key=c_stw)
    log("BATCH", f"  Spark ran {b9['availability_evaluations']:,} replication evaluations and {b9['migration_evaluations']:,} "
                  f"migration evaluations across {b9['spark_partitions_used']} partitions")
    for r in b9["availability_by_replication"]:
        if r["p_down"] == 0.10:
            log("BATCH", f"    each copy down {r['p_down']*100:.0f}% of the time, {r['replicas']} copies: availability "
                          f"{r['availability']*100:.2f}% (formula {r['analytic_availability']*100:.2f}%), degraded writes {r['degraded_writes']}")
    for r in b9["migration_downtime"]:
        log("BATCH", f"    {r['keys']:9,d} records: stop-the-world freezes writes {r['stop_the_world_freeze_s']:7.3f}s "
                      f"({r['stw_blocked_writes']} writes blocked); live migration blocks 0 (takes {r['live_migration_duration_s']}s)")

    lat = m10["naming"]["latency"]
    live = {"exchange_s": lat["name_server"]["mean_latency_us"] / 1e6, "local_s": lat["static_map"]["mean_latency_us"] / 1e6,
            "broadcast_per_extra_node_s": (lat["broadcast"]["mean_latency_us"] - lat["name_server"]["mean_latency_us"]) / 4 / 1e6,
            "dns_hit_rate": lat["dns_style"]["cache_hit_rate"]}
    log("BATCH", f"[BIG DATA] Apache Spark over {N_ROWS:,} name lookups (M10), calibrated by the live per-message cost "
                  f"({live['exchange_s']*1e6:.1f} microseconds) ...")
    b10 = run_naming_batch_layer(n_rows=N_ROWS, live=live)
    log("BATCH", f"  Spark ran {b10['dns_ttl_evaluations']:,} DNS-cache evaluations and {b10['scalability_evaluations']:,} "
                  f"scalability evaluations across {b10['spark_partitions_used']} partitions")
    for r in b10["dns_ttl_tradeoff"]:
        log("BATCH", f"    TTL {r['ttl_s']:6.0f}s: hit rate {r['hit_rate']*100:5.1f}%  messages/lookup {r['exchanges_per_lookup']:.3f}  "
                      f"stale answers {r['stale_answers']}")
    for r in b10["scalability"]:
        if r["nodes"] in (5, 5000):
            log("BATCH", f"    {r['scheme']:12s} {r['nodes']:5d} nodes: mean {r['mean_latency_ms']:9.4f} ms  "
                          f"busiest server utilisation {r['busiest_server_utilization']*100:7.1f}%  overloaded={r['overloaded']}")

    export_m9(m9, b9)
    export_m10(m10, b10)
    with open(os.path.join(RESULTS_DIR, "bigdata_extension_summary.json"), "w") as f:
        json.dump({"n_rows": N_ROWS, "m9_batch": b9, "m10_batch": b10}, f, indent=2, default=str)
    log("SERVING", f"All Power BI CSVs written to: {POWERBI_DIR}")
    log("SERVING", "PRACTICAL 2 BIG DATA + POWER BI EXTENSION COMPLETE.")


if __name__ == "__main__":
    main()
