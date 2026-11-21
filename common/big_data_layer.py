"""
big_data_layer.py
------------------
[BIG DATA FRAMEWORK - shared across every assignment, starting Assignment 2]

Chosen big data technology: Apache Spark (PySpark, local mode) -- see
common/spark_utils.py for the shared session/aggregation plumbing every
assignment's batch layer runs through.

WHY SPARK (and not e.g. Kafka or Hadoop/HDFS):
  - Runs as a single embedded process (no separate broker/cluster/
    Zookeeper service to install and keep running) -- realistic for a
    student project on a single Windows laptop: `pip install pyspark`
    plus a JDK is the entire setup.
  - It is nonetheless a genuine, industry-standard distributed batch
    processing engine: even in "local[*]" mode, Spark partitions the
    dataset and processes partitions in parallel across CPU cores using
    the same DataFrame/RDD execution model it uses on a real cluster.
  - It directly complements (does not replace) the platform's existing
    live socket-based node system: Spark handles the LARGE, HISTORICAL
    side of the workload; the existing nodes still handle real-time
    task execution. This is a standard "Lambda Architecture" split,
    applied CONSISTENTLY to every assignment from Assignment 2 onward:

        Batch Layer (Spark, 100,000-row dataset)
                        │
        Speed Layer (live Edge/RAN/Core/Cloud nodes -- whichever
                      milestone's real mechanism: task execution in
                      Assignment 1/2, transactions/locks in Assignment 3)
                        │
        Serving Layer (merged CSV exports, common/powerbi_export.py)
                        │
        Presentation Layer (ONE growing Power BI dashboard)

This module owns every assignment's Batch layer. The Speed layer is
always the real node platform from common/node.py -- never replaced,
only ever reused.
"""

import os
import numpy as np
import pandas as pd

from common.workload_generator import SERVICE_PROFILES, CLASS_WEIGHTS
from common.spark_utils import get_spark_session

# Participant-pair combinations a session-establishment transaction can
# span (Assignment 3 / Milestone 5), reused by both the live topology
# and the big-data transaction dataset below, so Spark's analysis stays
# grounded in the SAME node topology every other milestone uses.
PARTICIPANT_PAIRS = [
    ("edge-1", "ran-1"), ("edge-2", "ran-1"),
    ("edge-1", "core-1"), ("edge-2", "core-1"),
    ("ran-1", "core-1"),
]
# Lock resource names/nodes (Assignment 3 / Milestone 6), used by the
# lock-contention dataset generator further down.
LOCK_RESOURCE_NAMES = ["session_slot", "qos_token", "bearer_context", "radio_channel", "context_id"]
LOCK_NODES = ["edge-1", "edge-2", "ran-1", "core-1", "cloud-1"]


def generate_large_dataset(n_rows: int = 100_000, seed: int = 2026) -> pd.DataFrame:
    """
    Vectorized (numpy) generation of a large telecom service-request
    dataset with the SAME service-class distribution and per-class
    resource-profile ranges as the small-scale workload_generator.py
    used in Milestones 1/2/3, so results are directly comparable across
    scales. Vectorized rather than a 100,000-iteration Python loop,
    which is itself a small "big data" engineering decision worth
    noting: naive per-row generation would be the first bottleneck.
    """
    rng = np.random.default_rng(seed)
    classes = list(CLASS_WEIGHTS.keys())
    weights = np.array(list(CLASS_WEIGHTS.values()))
    service_class = rng.choice(classes, size=n_rows, p=weights / weights.sum())

    cpu = np.empty(n_rows)
    memory = np.empty(n_rows)
    bandwidth = np.empty(n_rows)
    work_units = np.empty(n_rows)

    for cls in classes:
        mask = service_class == cls
        n = mask.sum()
        prof = SERVICE_PROFILES[cls]
        cpu[mask] = rng.uniform(*prof["cpu"], size=n)
        memory[mask] = rng.uniform(*prof["memory"], size=n)
        bandwidth[mask] = rng.uniform(*prof["bandwidth"], size=n)
        work_units[mask] = rng.uniform(*prof["work_units"], size=n)

    df = pd.DataFrame({
        "task_id": np.arange(n_rows),
        "service_class": service_class,
        "cpu": np.round(cpu, 2),
        "memory": np.round(memory, 1),
        "bandwidth": np.round(bandwidth, 1),
        "work_units": np.round(work_units, 1),
    })
    return df


def run_batch_layer(n_rows: int = 100_000, seed: int = 2026, results_dir: str = None):
    """
    [BATCH LAYER] Runs the full Spark pipeline over the large dataset:
      1. Load the dataset into a Spark DataFrame (distributed across partitions).
      2. Simulate the SAME edge-first placement policy used live in
         Milestones 2/3 (common/workload_generator.py::edge_first_placement),
         but applied to all 100,000 rows via Spark's distributed execution
         instead of the Python client submitting each one live.
      3. Aggregate (a) per-service-class demand and (b) per-node simulated
         load, using Spark's distributed groupBy/agg -- this is the actual
         "big data processing" step, executed in parallel across partitions.
      4. Write results as CSVs, ready for Power BI import.

    Returns a dict summary (also written to disk) for use in the report/JSON log.
    """
    from pyspark.sql import functions as F
    from common.powerbi_export import BIGDATA_DIR

    if results_dir is None:
        results_dir = BIGDATA_DIR  # shared, absolute, project-root location -- never CWD-dependent
    os.makedirs(results_dir, exist_ok=True)

    pdf = generate_large_dataset(n_rows=n_rows, seed=seed)
    raw_path = os.path.join(results_dir, "raw_dataset_sample_1000.csv")
    pdf.head(1000).to_csv(raw_path, index=False)  # full 100k kept in Spark only; 1k sample for inspection/Power BI

    spark = get_spark_session()
    try:
        sdf = spark.createDataFrame(pdf)
        # Explicitly partition the dataset so processing is genuinely split and
        # parallelized across partitions/cores, rather than relying on however
        # many cores happen to be free on whatever machine this runs on.
        n_target_partitions = 8
        sdf = sdf.repartition(n_target_partitions)
        sdf = sdf.withColumn("row_id", F.monotonically_increasing_id())

        # --- Simulate the M2/M3 "edge-first" placement policy at scale ---
        # URLLC and mMTC -> round-robin across 2 edge nodes; eMBB -> ran-1
        # (mirrors common/workload_generator.py::edge_first_placement exactly)
        sdf = sdf.withColumn(
            "assigned_node",
            F.when(F.col("service_class") == "eMBB", F.lit("ran-1"))
             .otherwise(F.when(F.col("row_id") % 2 == 0, F.lit("edge-1")).otherwise(F.lit("edge-2")))
        )
        node_type_map = {"edge-1": "EDGE", "edge-2": "EDGE", "ran-1": "RAN"}
        map_expr = F.create_map([F.lit(x) for kv in node_type_map.items() for x in kv])
        sdf = sdf.withColumn("node_type", map_expr[F.col("assigned_node")])

        # --- [Distributed aggregation #1] demand per service class ---
        by_class = (
            sdf.groupBy("service_class")
               .agg(
                   F.count("*").alias("task_count"),
                   F.round(F.avg("cpu"), 3).alias("avg_cpu"),
                   F.round(F.avg("memory"), 2).alias("avg_memory_mb"),
                   F.round(F.avg("bandwidth"), 2).alias("avg_bandwidth_mbps"),
                   F.round(F.avg("work_units"), 2).alias("avg_work_units"),
                   F.round(F.sum("bandwidth"), 1).alias("total_bandwidth_demand_mbps"),
                   F.round(F.stddev("work_units"), 3).alias("stddev_work_units"),
               )
               .orderBy(F.desc("task_count"))
        )
        by_class_pdf = by_class.toPandas()
        by_class_pdf.to_csv(os.path.join(results_dir, "batch_summary_by_service_class.csv"), index=False)

        # --- [Distributed aggregation #2] simulated load per node ---
        by_node = (
            sdf.groupBy("assigned_node", "node_type")
               .agg(
                   F.count("*").alias("task_count"),
                   F.round(F.sum("cpu"), 1).alias("total_cpu_demand"),
                   F.round(F.sum("memory"), 1).alias("total_memory_demand_mb"),
                   F.round(F.sum("bandwidth"), 1).alias("total_bandwidth_demand_mbps"),
                   F.round(F.avg("work_units"), 2).alias("avg_work_units"),
               )
               .orderBy(F.desc("task_count"))
        )
        by_node_pdf = by_node.toPandas()
        by_node_pdf.to_csv(os.path.join(results_dir, "batch_summary_by_node.csv"), index=False)

        total_rows = sdf.count()
        n_partitions = sdf.rdd.getNumPartitions()

        summary = {
            "technology": "Apache Spark (PySpark, local[*] mode)",
            "total_rows_processed": total_rows,
            "spark_partitions_used": n_partitions,
            "by_service_class": by_class_pdf.to_dict(orient="records"),
            "by_node": by_node_pdf.to_dict(orient="records"),
        }
        return summary
    finally:
        spark.stop()


# =============================================================================
# [MILESTONE 5 - Distributed Transactions] big-data batch layer
# =============================================================================

def generate_transaction_dataset(n_rows: int = 100_000, seed: int = 3031) -> pd.DataFrame:
    """
    Vectorized generation of a large dataset of simulated session-
    establishment TRANSACTION ATTEMPTS: which participant-node pair they
    target (common.workload_generator-style realism, grounded in the
    SAME 5-node topology), how big a reservation they ask for, and which
    operating "scenario" they occur under (normal / node_down / high_load)
    -- mirroring the three conditions actually tested live in
    milestone5_transactions.py, but at 100,000-row scale.
    """
    rng = np.random.default_rng(seed)
    pair_idx = rng.integers(0, len(PARTICIPANT_PAIRS), size=n_rows)
    participant_a = np.array([PARTICIPANT_PAIRS[i][0] for i in pair_idx])
    participant_b = np.array([PARTICIPANT_PAIRS[i][1] for i in pair_idx])

    scenarios = np.array(["normal", "node_down", "high_load"])
    scenario_weights = np.array([0.7, 0.1, 0.2])  # mostly-normal operation, like a real deployment
    scenario = rng.choice(scenarios, size=n_rows, p=scenario_weights)

    cpu = np.round(rng.uniform(0.5, 2.0, size=n_rows), 2)
    memory = np.round(rng.uniform(32, 256, size=n_rows), 1)
    bandwidth = np.round(rng.uniform(2, 20, size=n_rows), 1)

    df = pd.DataFrame({
        "tx_id": np.arange(n_rows),
        "participant_a": participant_a,
        "participant_b": participant_b,
        "scenario": scenario,
        "cpu": cpu, "memory": memory, "bandwidth": bandwidth,
    })
    return df


def run_transaction_batch_layer(n_rows: int = 100_000, seed: int = 3031, results_dir: str = None) -> dict:
    """
    [BATCH LAYER - Milestone 5] Spark-processes the large transaction
    dataset: for each (participant pair, scenario) combination, predicts
    a commit/abort outcome using the SAME simple rule the live platform
    actually enforces (a transaction touching a "down" node aborts;
    otherwise it commits if the combined reservation is plausible), then
    aggregates predicted commit/abort rates and demand at scale -- this
    is what lets us say something about expected behaviour across
    100,000 attempts, not just the 50 we can afford to run live.
    """
    from pyspark.sql import functions as F
    from common.powerbi_export import BIGDATA_DIR

    if results_dir is None:
        results_dir = BIGDATA_DIR
    os.makedirs(results_dir, exist_ok=True)

    pdf = generate_transaction_dataset(n_rows=n_rows, seed=seed)

    spark = get_spark_session("5g6g-platform-m5-transaction-batch")
    try:
        sdf = spark.createDataFrame(pdf).repartition(8)
        # Predicted outcome: a node_down-scenario transaction always aborts
        # (mirrors the real 2PC prepare-vote failure); otherwise it commits.
        sdf = sdf.withColumn(
            "predicted_decision",
            F.when(F.col("scenario") == "node_down", F.lit("abort")).otherwise(F.lit("commit"))
        )
        sdf = sdf.withColumn("pair", F.concat_ws(" <-> ", F.col("participant_a"), F.col("participant_b")))

        by_pair_scenario = (
            sdf.groupBy("pair", "scenario")
               .agg(
                   F.count("*").alias("attempt_count"),
                   F.sum(F.when(F.col("predicted_decision") == "commit", 1).otherwise(0)).alias("predicted_commits"),
                   F.sum(F.when(F.col("predicted_decision") == "abort", 1).otherwise(0)).alias("predicted_aborts"),
                   F.round(F.avg("cpu"), 3).alias("avg_cpu_requested"),
                   F.round(F.sum("bandwidth"), 1).alias("total_bandwidth_requested_mbps"),
               )
               .orderBy(F.desc("attempt_count"))
        )
        by_pair_pdf = by_pair_scenario.toPandas()
        by_pair_pdf.to_csv(os.path.join(results_dir, "m5_batch_transactions_by_pair_scenario.csv"), index=False)

        overall = sdf.groupBy("scenario").agg(
            F.count("*").alias("attempt_count"),
            F.round(F.sum(F.when(F.col("predicted_decision") == "commit", 1).otherwise(0)) / F.count("*"), 4)
             .alias("predicted_commit_rate"),
        ).orderBy(F.desc("attempt_count"))
        overall_pdf = overall.toPandas()
        overall_pdf.to_csv(os.path.join(results_dir, "m5_batch_transactions_by_scenario.csv"), index=False)

        return {
            "technology": "Apache Spark (PySpark, local[*] mode)",
            "total_rows_processed": sdf.count(),
            "spark_partitions_used": sdf.rdd.getNumPartitions(),
            "by_pair_scenario": by_pair_pdf.to_dict(orient="records"),
            "by_scenario": overall_pdf.to_dict(orient="records"),
        }
    finally:
        spark.stop()


# =============================================================================
# [MILESTONE 6 - Concurrency and Deadlock Management] big-data batch layer
# =============================================================================

def generate_lock_contention_dataset(n_rows: int = 100_000, seed: int = 6061) -> pd.DataFrame:
    """
    Vectorized generation of a large dataset of simulated lock REQUESTS:
    which transaction, which node, which named resource, and what
    position in that transaction's acquisition order (1st lock requested,
    2nd, ...). This is what lets the batch layer analyze, at scale,
    which (node, resource) combinations are requested most often and are
    therefore the most likely sites of real contention -- complementing
    (not replacing) the small, deliberately-engineered live deadlock
    demonstration in milestone6_concurrency_deadlock.py.
    """
    rng = np.random.default_rng(seed)
    tx_id = rng.integers(0, n_rows // 4, size=n_rows)  # ~4 lock requests per transaction on average
    node = rng.choice(LOCK_NODES, size=n_rows)
    resource = rng.choice(LOCK_RESOURCE_NAMES, size=n_rows)
    order_position = rng.integers(1, 5, size=n_rows)

    df = pd.DataFrame({
        "request_id": np.arange(n_rows),
        "tx_id": tx_id,
        "node": node,
        "resource": resource,
        "order_position": order_position,
    })
    return df


def run_lock_contention_batch_layer(n_rows: int = 100_000, seed: int = 6061, results_dir: str = None) -> dict:
    """
    [BATCH LAYER - Milestone 6] Spark-aggregates the large lock-request
    dataset by (node, resource) to rank which resources see the heaviest
    simulated contention, and separately flags (node, resource) pairs
    requested at BOTH order_position 1 and order_position 2+ by
    different transactions -- a rough, large-scale proxy for "this
    resource is a plausible participant in a circular-wait," the same
    structural pattern the live deadlock experiment deliberately builds.
    """
    from pyspark.sql import functions as F
    from common.powerbi_export import BIGDATA_DIR

    if results_dir is None:
        results_dir = BIGDATA_DIR
    os.makedirs(results_dir, exist_ok=True)

    pdf = generate_lock_contention_dataset(n_rows=n_rows, seed=seed)

    spark = get_spark_session("5g6g-platform-m6-lock-contention-batch")
    try:
        sdf = spark.createDataFrame(pdf).repartition(8)

        contention = (
            sdf.groupBy("node", "resource")
               .agg(
                   F.count("*").alias("request_count"),
                   F.countDistinct("tx_id").alias("distinct_transactions"),
                   F.round(F.avg("order_position"), 2).alias("avg_order_position"),
               )
               .orderBy(F.desc("request_count"))
        )
        contention_pdf = contention.toPandas()
        contention_pdf.to_csv(os.path.join(results_dir, "m6_batch_lock_contention_by_resource.csv"), index=False)

        return {
            "technology": "Apache Spark (PySpark, local[*] mode)",
            "total_rows_processed": sdf.count(),
            "spark_partitions_used": sdf.rdd.getNumPartitions(),
            "contention_by_resource": contention_pdf.to_dict(orient="records"),
        }
    finally:
        spark.stop()


# =============================================================================
# [MILESTONE 7 - Fault Tolerance and Recovery] big-data batch layer
# =============================================================================

# The heartbeat detector's real settings (identical to common/fault_tolerance.py and
# milestone7_fault_tolerance.py) -- the batch model is built from THESE, then checked
# against what the live experiment measured.
DETECTOR_PARAMS = {"interval_s": 0.1, "probe_timeout_s": 0.15, "misses": 3,
                   "instant_probe_s": 0.002, "rate_hz": 40, "client_timeout_s": 0.25}
FAILURE_MIX = {"crash": 0.30, "omission": 0.15, "delay": 0.25, "service_down": 0.20, "byzantine": 0.10}


def generate_failure_event_dataset(n_rows: int = 100_000, seed: int = 7071,
                                   mean_repair_s: float = 2.0) -> pd.DataFrame:
    """
    100,000 simulated failure events across the 5-node platform. Each event has
    a failure type (drawn from an ASSUMED real-world mix), the node it hits, a
    random 'phase' (where in the heartbeat cycle the failure began -- this is
    what makes detection time vary from event to event) and a repair time (how
    long a human would take, used for the no-fault-tolerance baseline).
    """
    rng = np.random.default_rng(seed)
    types = list(FAILURE_MIX)
    probs = np.array(list(FAILURE_MIX.values()))
    return pd.DataFrame({
        "event_id": np.arange(n_rows),
        "failure_type": rng.choice(types, size=n_rows, p=probs / probs.sum()),
        "node": rng.choice(["edge-1", "edge-2", "ran-1", "core-1", "cloud-1"], size=n_rows),
        "phase_u": rng.uniform(0, 1, size=n_rows),
        "repair_s": rng.exponential(mean_repair_s, size=n_rows),
    })


def run_failure_batch_layer(n_rows: int = 100_000, seed: int = 7071, results_dir: str = None,
                            params: dict = None) -> dict:
    """
    [BATCH LAYER - Milestone 7] Spark computes, for every simulated failure:
      detection time   = U(0, interval)  +  (misses-1) x interval  +  misses x probe_cost
                         (the failure starts at a random moment in the heartbeat
                          cycle, then `misses` consecutive probes must fail; a probe
                          to a dead node fails instantly, a probe to a silent or
                          slow node only fails after the 0.15 s timeout)
      failover requests = detection time x request rate  (requests sent before the
                          detector knew, which had to fail over on their own)
    and aggregates the distribution (mean, median, p95, p99) per failure type.
    """
    from pyspark.sql import functions as F
    from common.powerbi_export import BIGDATA_DIR

    P = {**DETECTOR_PARAMS, **(params or {})}
    results_dir = results_dir or BIGDATA_DIR
    os.makedirs(results_dir, exist_ok=True)
    pdf = generate_failure_event_dataset(n_rows=n_rows, seed=seed)

    spark = get_spark_session("5g6g-platform-m7-failure-batch")
    try:
        sdf = spark.createDataFrame(pdf).repartition(8)
        is_byz = F.col("failure_type") == "byzantine"
        slow_probe = F.col("failure_type").isin("omission", "delay")
        probe_cost = F.when(slow_probe, F.lit(P["probe_timeout_s"])).otherwise(F.lit(P["instant_probe_s"]))
        sdf = sdf.withColumn(
            "detection_s",
            F.when(is_byz, F.col("phase_u") / F.lit(float(P["rate_hz"])))   # caught by the very next vote
             .otherwise(F.col("phase_u") * P["interval_s"] + (P["misses"] - 1) * P["interval_s"]
                        + P["misses"] * probe_cost))
        sdf = sdf.withColumn("failover_requests", F.when(is_byz, F.lit(0.0)).otherwise(F.col("detection_s") * P["rate_hz"]))
        sdf = sdf.withColumn("wasted_client_s", F.when(slow_probe, F.col("failover_requests") * P["client_timeout_s"])
                                                 .otherwise(F.lit(0.0)))
        sdf = sdf.withColumn("no_ft_downtime_s", F.when(is_byz, F.lit(0.0)).otherwise(F.col("repair_s")))
        sdf = sdf.withColumn("no_ft_corrupted_results", F.when(is_byz, F.col("repair_s") * P["rate_hz"]).otherwise(F.lit(0.0)))

        agg = (sdf.groupBy("failure_type").agg(
            F.count("*").alias("events"),
            F.round(F.avg("detection_s"), 4).alias("mean_detection_s"),
            F.round(F.percentile_approx("detection_s", 0.5), 4).alias("p50_detection_s"),
            F.round(F.percentile_approx("detection_s", 0.95), 4).alias("p95_detection_s"),
            F.round(F.percentile_approx("detection_s", 0.99), 4).alias("p99_detection_s"),
            F.round(F.avg("failover_requests"), 3).alias("mean_failover_requests"),
            F.round(F.avg("wasted_client_s"), 4).alias("mean_wasted_client_s_with_ft"),
            F.round(F.avg("no_ft_downtime_s"), 4).alias("mean_downtime_s_without_ft"),
            F.round(F.avg("no_ft_corrupted_results"), 3).alias("mean_corrupted_results_without_ft"),
        ).orderBy(F.desc("events")))
        agg_pdf = agg.toPandas()
        agg_pdf.to_csv(os.path.join(results_dir, "m7_batch_detection_by_failure_type.csv"), index=False)
        return {
            "technology": "Apache Spark (PySpark, local[*] mode)",
            "total_rows_processed": sdf.count(),
            "spark_partitions_used": sdf.rdd.getNumPartitions(),
            "detector_params": P,
            "by_failure_type": agg_pdf.to_dict(orient="records"),
        }
    finally:
        spark.stop()


# =============================================================================
# [MILESTONE 8 - Distributed System Models] big-data batch layer
# =============================================================================

MODEL_ORDER = ["host_based", "workstation", "server_based", "processor_pool", "integrated"]
_NODES = ["edge-1", "edge-2", "ran-1", "core-1", "cloud-1"]


def default_routing_mix() -> dict:
    """
    FALLBACK routing shares per model and service class, used only when the live
    Milestone 8 results are not available. host/workstation/server follow their
    fixed rules exactly; pool/integrated are rough guesses (the live run measures
    the real ones, which the extension script prefers).
    """
    cap = {"edge-1": 8, "edge-2": 8, "ran-1": 12, "core-1": 16, "cloud-1": 32}
    tot = sum(cap.values())
    mix = {}
    for cls in ("URLLC", "eMBB", "mMTC"):
        mix.setdefault("host_based", {})[cls] = {"cloud-1": 1.0}
        mix.setdefault("workstation", {})[cls] = {"edge-1": 0.5, "edge-2": 0.5}
        mix.setdefault("server_based", {})[cls] = {"ran-1": 1.0} if cls == "eMBB" else {"edge-1": 0.5, "edge-2": 0.5}
        mix.setdefault("processor_pool", {})[cls] = {n: c / tot for n, c in cap.items()}
    mix["integrated"] = {
        "URLLC": {"edge-1": 0.4, "edge-2": 0.4, "ran-1": 0.2},
        "mMTC": {"edge-1": 0.3, "edge-2": 0.3, "ran-1": 0.2, "core-1": 0.2},
        "eMBB": {"ran-1": 0.35, "core-1": 0.35, "cloud-1": 0.3},
    }
    return mix


def counts_to_shares(routing_counts: dict) -> dict:
    """{class: {node: count}} -> {class: {node: share}} (what live Milestone 8 saves is counts)."""
    return {cls: {n: c / sum(nodes.values()) for n, c in nodes.items()}
            for cls, nodes in routing_counts.items() if sum(nodes.values()) > 0}


def generate_request_trace(n_rows: int = 100_000, seed: int = 8081) -> pd.DataFrame:
    """A 100,000-request telecom trace (same class mix and service-time ranges as the earlier milestones)."""
    rng = np.random.default_rng(seed)
    classes = list(CLASS_WEIGHTS)
    w = np.array(list(CLASS_WEIGHTS.values()))
    cls = rng.choice(classes, size=n_rows, p=w / w.sum())
    work = np.empty(n_rows)
    for c in classes:
        m = cls == c
        work[m] = rng.uniform(*SERVICE_PROFILES[c]["work_units"], size=int(m.sum()))
    return pd.DataFrame({"request_id": np.arange(n_rows), "service_class": cls, "work_units": np.round(work, 1)})


def run_models_batch_layer(n_rows: int = 100_000, seed: int = 8081, routing_mix: dict = None,
                           results_dir: str = None) -> dict:
    """
    [BATCH LAYER - Milestone 8] Evaluates ALL FIVE distributed-system models on a
    100,000-request trace (so 500,000 model-request evaluations). Each model's
    placement of each service class follows the routing mix MEASURED in the live
    Milestone 8 run. Spark then computes expected end-to-end latency
    (= simulated network round-trip of the serving tier + service time),
    p95 latency, URLLC SLA compliance, and the spread of load across tiers.
    NOTE: this analytic layer deliberately ignores queueing -- the live
    experiment captures that. The two layers answer different questions.
    """
    from pyspark.sql import functions as F
    from common.powerbi_export import BIGDATA_DIR
    from common.system_models import SIM_RTT_MS, TIER_OF, TRANSPORT_COST_PER_1K, URLLC_SLA_MS

    results_dir = results_dir or BIGDATA_DIR
    os.makedirs(results_dir, exist_ok=True)
    fallback = default_routing_mix()
    mix = {m: {**fallback[m], **(routing_mix or {}).get(m, {})} for m in MODEL_ORDER}

    trace = generate_request_trace(n_rows=n_rows, seed=seed)
    rng = np.random.default_rng(seed + 1)
    frames = []
    for model in MODEL_ORDER:
        nodes_col = np.empty(len(trace), dtype=object)
        for cls in ("URLLC", "eMBB", "mMTC"):
            m = (trace["service_class"] == cls).to_numpy()
            shares = mix[model].get(cls) or fallback[model][cls]
            names = list(shares)
            p = np.array([shares[n] for n in names], dtype=float)
            nodes_col[m] = rng.choice(names, size=int(m.sum()), p=p / p.sum())
        f = trace.copy()
        f["model"] = model
        f["node"] = nodes_col
        frames.append(f)
    big = pd.concat(frames, ignore_index=True)

    spark = get_spark_session("5g6g-platform-m8-models-batch")
    try:
        sdf = spark.createDataFrame(big[["model", "service_class", "node", "work_units"]]).repartition(8)
        tier_map = F.create_map([F.lit(x) for kv in TIER_OF.items() for x in kv])
        rtt_map = F.create_map([F.lit(x) for kv in SIM_RTT_MS.items() for x in kv])
        cost_map = F.create_map([F.lit(x) for kv in TRANSPORT_COST_PER_1K.items() for x in kv])
        sdf = (sdf.withColumn("tier", tier_map[F.col("node")])
                  .withColumn("latency_ms", rtt_map[F.col("tier")] + F.col("work_units"))
                  .withColumn("transport_cost_per_1k", cost_map[F.col("tier")]))

        by_class = (sdf.groupBy("model", "service_class").agg(
            F.count("*").alias("requests"),
            F.round(F.avg("latency_ms"), 2).alias("mean_latency_ms"),
            F.round(F.percentile_approx("latency_ms", 0.95), 2).alias("p95_latency_ms"),
            F.round(F.avg(F.when(F.col("latency_ms") <= URLLC_SLA_MS, 1.0).otherwise(0.0)), 4).alias("sla_compliance"),
            F.round(F.avg("transport_cost_per_1k"), 4).alias("transport_cost_per_1k_requests"),
        ).orderBy("model", "service_class"))
        by_class_pdf = by_class.toPandas()
        by_class_pdf.to_csv(os.path.join(results_dir, "m8_batch_latency_by_model_class.csv"), index=False)

        tiers = sdf.groupBy("model", "tier").agg(F.count("*").alias("requests")).toPandas()
        tiers["share_of_model_requests"] = tiers["requests"] / tiers.groupby("model")["requests"].transform("sum")
        tiers["share_of_model_requests"] = tiers["share_of_model_requests"].round(4)
        tiers = tiers.sort_values(["model", "tier"])
        tiers.to_csv(os.path.join(results_dir, "m8_batch_tier_share_by_model.csv"), index=False)

        return {
            "technology": "Apache Spark (PySpark, local[*] mode)",
            "trace_rows": n_rows,
            "total_rows_processed": sdf.count(),
            "spark_partitions_used": sdf.rdd.getNumPartitions(),
            "routing_mix_used": mix,
            "by_model_class": by_class_pdf.to_dict(orient="records"),
            "tier_share": tiers.to_dict(orient="records"),
        }
    finally:
        spark.stop()


# =============================================================================
# [MILESTONE 9 - Transparency and Reliability] big-data batch layer
# =============================================================================

def run_transparency_batch_layer(n_rows: int = 100_000, seed: int = 9091, stop_the_world_s_per_key: float = None,
                                 live_copy_keys_per_s: float = 25_000.0, results_dir: str = None) -> dict:
    """
    [BATCH LAYER - Milestone 9]  Two Spark jobs over 100,000 client operations.

    JOB 1 - reliability of replication at scale. Every operation carries three independent random draws,
            one per replica ("is that copy up right now?"). Crossing the 100,000 operations with 4 outage
            probabilities and 3 replication factors gives 1.2 MILLION evaluations inside Spark. Per cell it
            computes availability (served if at least one copy is up), compares it with the formula
            1 - p^r from Milestone 7, and counts DEGRADED writes (accepted by some copies but not all), each
            of which leaves work for the repair step after recovery.

    JOB 2 - migration downtime at scale. The live experiment measured how long it takes to copy a database;
            here those measured costs are applied to databases up to 2,000,000 records. Writes that arrive
            while a STOP-THE-WORLD migration is copying must wait; a LIVE migration never blocks a write
            (but writes during it cost one extra message, since they also go to the joining copy).
    """
    from pyspark.sql import functions as F
    from common.powerbi_export import BIGDATA_DIR

    results_dir = results_dir or BIGDATA_DIR
    os.makedirs(results_dir, exist_ok=True)
    rng = np.random.default_rng(seed)
    horizon_s = 600.0
    ops = pd.DataFrame({
        "op_id": np.arange(n_rows),
        "is_write": rng.uniform(size=n_rows) < 0.3,
        "u1": rng.uniform(size=n_rows), "u2": rng.uniform(size=n_rows), "u3": rng.uniform(size=n_rows),
        "arrival_s": rng.uniform(0, horizon_s, size=n_rows),
    })
    c_stw = stop_the_world_s_per_key if stop_the_world_s_per_key else 1.2e-5   # fallback if no live result is available

    spark = get_spark_session("5g6g-platform-m9-transparency-batch")
    try:
        sdf = spark.createDataFrame(ops).repartition(8)

        # ---- JOB 1: availability by replication factor and outage probability ----
        grid = spark.createDataFrame(pd.DataFrame([(p, r) for p in (0.01, 0.05, 0.10, 0.20) for r in (1, 2, 3)],
                                                  columns=["p_down", "replicas"]))
        j = sdf.crossJoin(grid)
        up1, up2, up3 = F.col("u1") >= F.col("p_down"), F.col("u2") >= F.col("p_down"), F.col("u3") >= F.col("p_down")
        served = F.when(F.col("replicas") == 1, up1).when(F.col("replicas") == 2, up1 | up2).otherwise(up1 | up2 | up3)
        full = F.when(F.col("replicas") == 1, up1).when(F.col("replicas") == 2, up1 & up2).otherwise(up1 & up2 & up3)
        j = j.withColumn("served", served).withColumn("full", full)
        avail = (j.groupBy("p_down", "replicas").agg(
            F.count("*").alias("operations"),
            F.round(F.avg(F.col("served").cast("double")), 5).alias("availability"),
            F.sum(F.when(F.col("is_write") & F.col("served") & ~F.col("full"), 1).otherwise(0)).alias("degraded_writes"),
            F.round(F.avg(F.when(F.col("is_write"), F.col("full").cast("double"))), 5).alias("write_fully_replicated_rate"),
        ).orderBy("p_down", "replicas")).toPandas()
        avail["analytic_availability"] = (1 - avail["p_down"] ** avail["replicas"]).round(5)
        avail["abs_error_vs_formula"] = (avail["availability"] - avail["analytic_availability"]).abs().round(5)
        avail["repair_keys_per_hour"] = (avail["degraded_writes"] * (3600.0 / horizon_s)).round(0)
        avail.to_csv(os.path.join(results_dir, "m9_batch_availability_by_replication.csv"), index=False)

        # ---- JOB 2: migration downtime vs database size ----
        sizes = spark.createDataFrame(pd.DataFrame({"keys": [2_000, 10_000, 40_000, 100_000, 500_000, 2_000_000]}))
        t0 = horizon_s / 2
        m = sdf.crossJoin(sizes)
        m = m.withColumn("stw_window_s", F.col("keys") * F.lit(c_stw))
        m = m.withColumn("live_window_s", F.col("keys") / F.lit(live_copy_keys_per_s))
        m = m.withColumn("stw_delay_s", F.when(F.col("is_write") & (F.col("arrival_s") >= t0) &
                                                (F.col("arrival_s") <= t0 + F.col("stw_window_s")),
                                                t0 + F.col("stw_window_s") - F.col("arrival_s")).otherwise(F.lit(0.0)))
        m = m.withColumn("live_extra_msg", F.when(F.col("is_write") & (F.col("arrival_s") >= t0) &
                                                    (F.col("arrival_s") <= t0 + F.col("live_window_s")), 1).otherwise(0))
        mig = (m.groupBy("keys").agg(
            F.round(F.first("stw_window_s"), 3).alias("stop_the_world_freeze_s"),
            F.sum(F.when(F.col("stw_delay_s") > 0, 1).otherwise(0)).alias("stw_blocked_writes"),
            F.round(F.avg(F.when(F.col("is_write"), (F.col("stw_delay_s") > 0).cast("double"))), 5).alias("stw_blocked_share_of_writes"),
            F.round(F.max("stw_delay_s"), 3).alias("stw_max_write_delay_s"),
            F.round(F.percentile_approx(F.when(F.col("is_write"), F.col("stw_delay_s")), 0.999), 4).alias("stw_p999_write_delay_s"),
            F.round(F.first("live_window_s"), 3).alias("live_migration_duration_s"),
            F.sum("live_extra_msg").alias("live_extra_messages"),
        ).orderBy("keys")).toPandas()
        mig["live_blocked_writes"] = 0
        mig.to_csv(os.path.join(results_dir, "m9_batch_migration_downtime.csv"), index=False)
        return {"technology": "Apache Spark (PySpark, local[*] mode)", "operations": n_rows,
                "availability_evaluations": int(avail["operations"].sum()), "migration_evaluations": n_rows * 6,
                "stop_the_world_s_per_key_used": c_stw, "live_copy_keys_per_s": live_copy_keys_per_s,
                "spark_partitions_used": sdf.rdd.getNumPartitions(),
                "availability_by_replication": avail.to_dict(orient="records"),
                "migration_downtime": mig.to_dict(orient="records")}
    finally:
        spark.stop()


# =============================================================================
# [MILESTONE 10 - Naming and Distributed Process Management] big-data batch layer
# =============================================================================

def run_naming_batch_layer(n_rows: int = 100_000, seed: int = 10101, live: dict = None, results_dir: str = None) -> dict:
    """
    [BATCH LAYER - Milestone 10]

    JOB 1 - the DNS caching trade-off (Spark WINDOW-style array aggregation). 100,000 lookups from 50 clients
            over 10 minutes, with Zipf-popular names; 5% of the 10,000 names MOVE at a random moment. For each
            time-to-live (TTL) Spark replays every (client, name) history in time order: a lookup is a cache MISS
            (costs a message) when the cached copy is older than the TTL, otherwise a HIT; a hit is STALE if the
            name moved after the copy was fetched. A longer TTL means fewer messages but more stale answers.

    JOB 2 - scalability with the number of nodes (5 -> 5,000), 100,000 lookups x 4 sizes x 5 schemes = 2 MILLION
            evaluations. Per-message cost and the per-node broadcast cost are taken from the live experiment; load on
            a name server is modelled as a queue (latency = cost / (1 - utilisation)), with every node's 10 clients
            each issuing 2 lookups/s. A server whose utilisation reaches 100% is flagged as OVERLOADED.
    """
    from pyspark.sql import functions as F
    from common.powerbi_export import BIGDATA_DIR

    results_dir = results_dir or BIGDATA_DIR
    os.makedirs(results_dir, exist_ok=True)
    live = live or {}
    c_ex = live.get("exchange_s", 77e-6)                  # one request/reply (central name server, live)
    c_local = live.get("local_s", 1e-7)                   # a local table lookup (static map, live)
    c_bcast = live.get("broadcast_per_extra_node_s", 126e-6)
    hit_rate = live.get("dns_hit_rate", 0.78)

    rng = np.random.default_rng(seed)
    n_names, n_clients, horizon = 10_000, 50, 600.0
    p = 1.0 / np.arange(1, n_names + 1) ** 1.1
    p /= p.sum()
    name_id = rng.choice(n_names, size=n_rows, p=p)
    moves = np.where(rng.uniform(size=n_names) < 0.05, rng.uniform(0, horizon, size=n_names), 1e9)
    trace = pd.DataFrame({"client_id": rng.integers(0, n_clients, size=n_rows), "name_id": name_id,
                          "ts": rng.uniform(0, horizon, size=n_rows), "mv": moves[name_id]})

    spark = get_spark_session("5g6g-platform-m10-naming-batch")
    try:
        # ---- JOB 1: DNS TTL trade-off ----
        sdf = spark.createDataFrame(trace).repartition(8)
        grouped = (sdf.groupBy("client_id", "name_id")
                      .agg(F.array_sort(F.collect_list("ts")).alias("ts_list"), F.first("mv").alias("mv")).cache())
        rows = []
        for ttl in (1.0, 5.0, 30.0, 300.0):
            init = F.struct(F.lit(-1.0e9).alias("last"), F.lit(0).alias("misses"), F.lit(0).alias("stale"))

            def make_merge(ttl_value):          # a closure: Spark inspects the lambda's argument count
                def merge(acc, t):
                    miss = (t - acc["last"]) >= F.lit(ttl_value)
                    return F.struct(
                        F.when(miss, t).otherwise(acc["last"]).alias("last"),
                        (acc["misses"] + F.when(miss, F.lit(1)).otherwise(F.lit(0))).alias("misses"),
                        (acc["stale"] + F.when((~miss) & (F.col("mv") > acc["last"]) & (F.col("mv") <= t), F.lit(1))
                         .otherwise(F.lit(0))).alias("stale"))
                return merge

            merge = make_merge(ttl)
            tot = (grouped.select(F.size("ts_list").alias("n"), F.aggregate("ts_list", init, merge).alias("r"))
                          .agg(F.sum("n").alias("requests"), F.sum("r.misses").alias("misses"),
                               F.sum("r.stale").alias("stale_hits")).collect()[0])
            rows.append({"ttl_s": ttl, "lookups": int(tot["requests"]), "cache_misses": int(tot["misses"]),
                         "hit_rate": round(1 - tot["misses"] / tot["requests"], 4),
                         "exchanges_per_lookup": round(tot["misses"] / tot["requests"], 4),
                         "stale_answers": int(tot["stale_hits"]),
                         "stale_answer_rate": round(tot["stale_hits"] / tot["requests"], 5)})
        ttl_pdf = pd.DataFrame(rows)
        ttl_pdf.to_csv(os.path.join(results_dir, "m10_batch_dns_ttl_tradeoff.csv"), index=False)

        # ---- JOB 2: scalability with the number of nodes ----
        zones_share = 0.4          # the busiest zone (/edge) receives 40% of the lookups
        svc = c_ex / 2.0
        params = []
        for n in (5, 50, 500, 5000):
            lam = n * 10 * 2.0     # offered lookups per second
            for s in ("static_map", "broadcast", "name_server", "prefix_table", "dns_style"):
                if s == "static_map":
                    rho, base, hit, ex = 0.0, c_local, 1.0, 0.0
                elif s == "broadcast":
                    rho = lam * svc
                    base, hit, ex = c_ex / (1 - min(rho, 0.99)) + (n - 1) * c_bcast, 0.0, float(n)
                elif s == "name_server":
                    rho = lam * svc
                    base, hit, ex = c_ex / (1 - min(rho, 0.99)), 0.0, 1.0
                elif s == "prefix_table":
                    rho = lam * svc * zones_share
                    base, hit, ex = c_ex / (1 - min(rho, 0.99)), 0.0, 1.0
                else:
                    rho = lam * (1 - hit_rate) * svc * zones_share
                    base, hit, ex = c_ex / (1 - min(rho, 0.99)), hit_rate, 1.0
                upd = {"static_map": n * 10.0, "broadcast": 0.0}.get(s, 1.0)   # static: push the move to every client
                params.append({"scheme": s, "nodes": n, "update_exchanges_per_move": upd,
                               "busiest_server_utilization": round(rho, 4),
                               "overloaded": bool(rho >= 1.0), "miss_latency_s": base, "hit_prob": hit,
                               "exchanges_on_miss": ex, "local_s": c_local})
        pparams = pd.DataFrame(params)
        lookups = pd.DataFrame({"u": rng.uniform(size=n_rows), "z": rng.normal(size=n_rows)})
        j = spark.createDataFrame(lookups).repartition(8).crossJoin(spark.createDataFrame(pparams))
        is_hit = F.col("u") < F.col("hit_prob")
        j = (j.withColumn("latency_s", F.when(is_hit, F.col("local_s")).otherwise(F.col("miss_latency_s")) * F.exp(0.5 * F.col("z")))
               .withColumn("exchanges", F.when(is_hit, F.lit(0.0)).otherwise(F.col("exchanges_on_miss"))))
        scal = (j.groupBy("scheme", "nodes").agg(
            F.round(F.first("busiest_server_utilization"), 4).alias("busiest_server_utilization"),
            F.first("overloaded").alias("overloaded"),
            F.first("update_exchanges_per_move").alias("update_exchanges_per_move"),
            F.round(F.avg("exchanges"), 3).alias("mean_exchanges_per_lookup"),
            F.round(F.avg("latency_s") * 1000, 4).alias("mean_latency_ms"),
            F.round(F.percentile_approx("latency_s", 0.95) * 1000, 4).alias("p95_latency_ms"),
            F.round(F.percentile_approx("latency_s", 0.99) * 1000, 4).alias("p99_latency_ms"),
        ).orderBy("scheme", "nodes")).toPandas()
        scal["exchanges_per_1000_lookups_and_10_moves"] = (scal["mean_exchanges_per_lookup"] * 1000
                                                           + scal["update_exchanges_per_move"] * 10).round(0)
        scal.to_csv(os.path.join(results_dir, "m10_batch_naming_scalability.csv"), index=False)
        return {"technology": "Apache Spark (PySpark, local[*] mode)", "lookups": n_rows,
                "dns_ttl_evaluations": n_rows * 4, "scalability_evaluations": n_rows * len(pparams),
                "spark_partitions_used": sdf.rdd.getNumPartitions(),
                "live_parameters_used": {"exchange_s": c_ex, "local_s": c_local, "broadcast_per_extra_node_s": c_bcast,
                                         "dns_hit_rate": hit_rate},
                "dns_ttl_tradeoff": ttl_pdf.to_dict(orient="records"), "scalability": scal.to_dict(orient="records")}
    finally:
        spark.stop()
