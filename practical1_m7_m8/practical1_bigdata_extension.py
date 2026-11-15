"""
================================================================================
PRACTICAL 1 EXTENSION - Big Data Integration + Power BI Dashboard
================================================================================
System: Distributed 5G/6G Network Service Platform (Theme 1)
Extends: Milestone 7 (Fault Tolerance) and Milestone 8 (Distributed System Models)

Same framework, same pattern as assignment2_/assignment3_bigdata_extension.py:
the SAME shared modules (common/spark_utils.py, common/big_data_layer.py,
common/powerbi_export.py), the SAME Lambda Architecture, writing into the SAME
project-wide results/powerbi/ folder (filenames prefixed m7_/m8_), feeding the
SAME one growing Power BI dashboard.

    [BATCH LAYER]   Apache Spark processes
                      * 100,000 simulated failure events (M7): detection-time
                        distributions per failure type, built from the heartbeat
                        detector's real settings
                      * a 100,000-request telecom trace evaluated under all five
                        distributed-system models (M8): 500,000 evaluations, using
                        the routing mix MEASURED live
                          |
    [SPEED LAYER]    The live fault-injection and system-model experiments from
                     milestone7_fault_tolerance.py / milestone8_system_models.py
                     (their saved results are reused; they are re-run only if
                     missing)
                          |
    [SERVING LAYER]  Batch + live results + a MODEL-vs-MEASUREMENT validation table
                     merged into results/powerbi/m7_*.csv and m8_*.csv
                          |
    [PRESENTATION]   The SAME Power BI dashboard gains two new pages
                     (see the project-root POWERBI_DASHBOARD_GUIDE.md)

Run:
    python -m practical1_m7_m8.practical1_bigdata_extension
================================================================================
"""

import sys
import os
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.big_data_layer import (run_failure_batch_layer, run_models_batch_layer, counts_to_shares,
                                   DETECTOR_PARAMS)
from common.powerbi_export import write_csv, write_metric_rows_csv, POWERBI_DIR
from common.system_models import MODEL_LABELS

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)
M7_JSON = os.path.join(RESULTS_DIR, "milestone7_fault_tolerance.json")
M8_JSON = os.path.join(RESULTS_DIR, "milestone8_system_models.json")

N_ROWS = 100_000
FAULTS_PER_HOUR = 6   # ASSUMPTION for the availability projection: one fault per 10 minutes on the primary


def log(tag, msg):
    print(f"[{tag}] {msg}")


def load_or_run_live():
    """Speed layer: reuse saved live results; run the experiment only if its JSON is missing."""
    if os.path.exists(M7_JSON):
        log("SPEED", "Using saved live Milestone 7 results.")
        m7 = json.load(open(M7_JSON))
    else:
        log("SPEED", "Milestone 7 results not found -- running the live fault-tolerance experiment now ...")
        from practical1_m7_m8.milestone7_fault_tolerance import run as run_m7
        m7 = run_m7()
    if os.path.exists(M8_JSON):
        log("SPEED", "Using saved live Milestone 8 results.")
        m8 = json.load(open(M8_JSON))
    else:
        log("SPEED", "Milestone 8 results not found -- running the live system-model experiment now ...")
        from practical1_m7_m8.milestone8_system_models import run as run_m8
        m8 = run_m8()
    return m7, m8


# ---------------------------------------------------------------------------
# [SERVING LAYER] M7
# ---------------------------------------------------------------------------

def export_m7(m7, batch):
    rows = [{k: r.get(k) for k in ("failure_type", "fault_tolerance", "client_mode", "total_requests",
                                    "failed_requests", "incorrect_results_accepted", "availability",
                                    "correct_service_ratio", "client_visible_downtime_s", "detection_time_s",
                                    "detected_as", "node_mttr_s", "requests_needing_failover",
                                    "messages_per_request", "p95_latency_ms")}
            for r in m7["failure_comparison"]]
    write_csv("m7_live_failure_comparison.csv", rows)

    lf = m7["leader_failure"]
    re, rc = lf.get("reelection_after_crash") or {}, lf.get("leader_reclaimed_by_recovered_node") or {}
    write_metric_rows_csv("m7_live_leader_failure.csv", {
        "initial_leader": lf["initial_leader"], "detected_as": lf["detected_as"],
        "detection_time_s": lf["detection_time_s"],
        "reelected_leader": re.get("new_leader"), "reelection_seconds_after_crash": re.get("seconds_after_crash"),
        "reelection_rounds": re.get("rounds"), "reelection_messages": re.get("messages"),
        "reelection_consistent": re.get("consistent_across_alive_nodes"),
        "restart_seconds": lf["restart_seconds"],
        "leader_reclaimed_by": rc.get("leader"), "reclaim_seconds_after_crash": rc.get("seconds_after_crash"),
        "final_leader_consistent": lf["final_leader_consistent"],
    })
    write_csv("m7_availability_model.csv", m7["availability_model"])

    write_csv("m7_batch_detection_by_failure_type.csv", batch["by_failure_type"])

    # --- model vs measurement: does the batch model predict what the live run saw? ---
    live = {(r["failure_type"]): r for r in m7["failure_comparison"] if r["fault_tolerance"] == "with FT"}
    val = []
    for b in batch["by_failure_type"]:
        t = b["failure_type"]
        meas = live.get(t, {})
        md = meas.get("detection_time_s")
        mf = meas.get("requests_needing_failover")
        val.append({
            "failure_type": t,
            "predicted_mean_detection_s": b["mean_detection_s"], "measured_detection_s": md,
            "detection_abs_error_s": round(abs(b["mean_detection_s"] - md), 3) if md is not None else None,
            "predicted_mean_failover_requests": b["mean_failover_requests"],
            "measured_failover_requests": mf if t != "byzantine" else 0,
        })
    write_csv("m7_model_validation.csv", val)

    # --- availability projection (assumption-based) ---
    ev = {b["failure_type"]: b["events"] for b in batch["by_failure_type"]}
    tot = sum(ev.values())
    mean_down = sum(b["mean_downtime_s_without_ft"] * ev[b["failure_type"]] for b in batch["by_failure_type"]) / tot
    mean_corrupt = sum(b["mean_corrupted_results_without_ft"] * ev[b["failure_type"]] for b in batch["by_failure_type"]) / tot
    mean_wasted = sum(b["mean_wasted_client_s_with_ft"] * ev[b["failure_type"]] for b in batch["by_failure_type"]) / tot
    proj = [
        {"strategy": "no fault tolerance", "assumed_faults_per_hour": FAULTS_PER_HOUR,
         "mean_client_downtime_s_per_fault": round(mean_down, 4),
         "projected_availability": round(1 - FAULTS_PER_HOUR * mean_down / 3600, 6),
         "corrupted_results_per_hour": round(FAULTS_PER_HOUR * mean_corrupt, 1),
         "slow_client_seconds_per_hour": 0.0},
        {"strategy": "replication + failover + quorum", "assumed_faults_per_hour": FAULTS_PER_HOUR,
         "mean_client_downtime_s_per_fault": 0.0, "projected_availability": 1.0,
         "corrupted_results_per_hour": 0.0,
         "slow_client_seconds_per_hour": round(FAULTS_PER_HOUR * mean_wasted, 3)},
    ]
    write_csv("m7_batch_availability_projection.csv", proj)
    log("SERVING", "Exported m7_live_failure_comparison.csv, m7_live_leader_failure.csv, m7_availability_model.csv, "
                    "m7_batch_detection_by_failure_type.csv, m7_model_validation.csv, m7_batch_availability_projection.csv")
    return val, proj


# ---------------------------------------------------------------------------
# [SERVING LAYER] M8
# ---------------------------------------------------------------------------

def export_m8(m8, batch):
    scal = []
    for model, runs in m8["scalability_runs"].items():
        for r in runs:
            scal.append({"model": MODEL_LABELS[model], "clients": r["clients"], "requests": r["tasks"],
                         "throughput_per_s": r["throughput_per_s"], "mean_latency_ms": r["mean_latency_ms"],
                         "p95_latency_ms": r["p95_latency_ms"], "success_rate": r["success_rate"],
                         "utilization_of_provisioned_capacity": r["utilization_of_provisioned_capacity"],
                         "load_balance_jain_index": r["load_balance_jain_index"],
                         "total_cost_per_1k_requests": r["total_cost_per_1k_requests"]})
    write_csv("m8_live_scalability.csv", scal)

    rel = [{"model": MODEL_LABELS[m], "crashed_node": n, "success_rate": v["success_rate"],
            "completed": v["completed"], "errors": v["errors"], "rejected": v["rejected"]}
           for m, nodes in m8["reliability_runs"].items() for n, v in nodes.items()]
    write_csv("m8_live_reliability.csv", rel)

    summ = [{k: v for k, v in t.items() if k != "routing_mix"} for t in m8["summary_table"].values()]
    write_csv("m8_live_model_summary.csv", summ)

    score = [{"model": MODEL_LABELS[m], "weighted_score": m8["scorecard_weighted"][m],
              "equal_weight_score": m8["scorecard_equal_weights"][m],
              "rank_weighted": m8["ranking_weighted"].index(m) + 1} for m in m8["scorecard_weighted"]]
    write_csv("m8_live_scorecard.csv", score)

    named = lambda rows: [{**r, "model": MODEL_LABELS.get(r["model"], r["model"])} for r in rows]
    write_csv("m8_batch_latency_by_model_class.csv", named(batch["by_model_class"]))
    write_csv("m8_batch_tier_share_by_model.csv", named(batch["tier_share"]))
    log("SERVING", "Exported m8_live_scalability.csv, m8_live_reliability.csv, m8_live_model_summary.csv, "
                    "m8_live_scorecard.csv, m8_batch_latency_by_model_class.csv, m8_batch_tier_share_by_model.csv")


def main():
    m7, m8 = load_or_run_live()

    log("BATCH", f"[BIG DATA] Apache Spark over {N_ROWS:,} simulated failure events (M7) ...")
    m7_batch = run_failure_batch_layer(n_rows=N_ROWS)
    log("BATCH", f"  Spark processed {m7_batch['total_rows_processed']:,} rows across "
                  f"{m7_batch['spark_partitions_used']} partitions")
    for r in m7_batch["by_failure_type"]:
        log("BATCH", f"    {r['failure_type']:13s} events={r['events']:6d}  mean detection={r['mean_detection_s']}s  "
                      f"p95={r['p95_detection_s']}s  p99={r['p99_detection_s']}s  failover_requests={r['mean_failover_requests']}")

    log("BATCH", f"[BIG DATA] Apache Spark over a {N_ROWS:,}-request trace under all 5 models (M8) ...")
    live_mix = {m: counts_to_shares(t["routing_mix"]) for m, t in m8["summary_table"].items()}
    m8_batch = run_models_batch_layer(n_rows=N_ROWS, routing_mix=live_mix)
    log("BATCH", f"  Spark processed {m8_batch['total_rows_processed']:,} model-request evaluations across "
                  f"{m8_batch['spark_partitions_used']} partitions")
    for r in m8_batch["by_model_class"]:
        if r["service_class"] == "URLLC":
            log("BATCH", f"    {MODEL_LABELS[r['model']]:26s} URLLC mean={r['mean_latency_ms']:6.2f} ms  "
                          f"p95={r['p95_latency_ms']:6.2f} ms  within {40} ms SLA: {r['sla_compliance']*100:5.1f}%")

    val, proj = export_m7(m7, m7_batch)
    export_m8(m8, m8_batch)

    log("SERVING", "Model-vs-measurement check (batch prediction vs live measurement):")
    for v in val:
        log("SERVING", f"    {v['failure_type']:13s} detection predicted {v['predicted_mean_detection_s']}s  "
                        f"measured {v['measured_detection_s']}s   failover requests predicted "
                        f"{v['predicted_mean_failover_requests']} measured {v['measured_failover_requests']}")

    with open(os.path.join(RESULTS_DIR, "bigdata_extension_summary.json"), "w") as f:
        json.dump({"n_rows": N_ROWS, "m7_batch": m7_batch, "m8_batch": {k: v for k, v in m8_batch.items()
                                                                          if k != "routing_mix_used"},
                   "availability_projection": proj, "model_validation": val}, f, indent=2, default=str)
    log("SERVING", f"All Power BI CSVs written to: {POWERBI_DIR}")
    log("SERVING", "PRACTICAL 1 BIG DATA + POWER BI EXTENSION COMPLETE.")


if __name__ == "__main__":
    main()
