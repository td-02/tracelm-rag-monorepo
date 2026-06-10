import json
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

COORDINATOR_BASE = "http://localhost:8000"
CLUSTER_STATUS_URL = f"{COORDINATOR_BASE}/cluster/status"
TRACES_URL = f"{COORDINATOR_BASE}/traces"
CACHE_STATS_URL = f"{COORDINATOR_BASE}/cache/stats"
CACHE_INVALIDATE_URL = f"{COORDINATOR_BASE}/cache/invalidate"
BENCHMARK_PATH = Path("benchmark_results.json")

st.set_page_config(page_title="RAG Cluster Dashboard", layout="wide")


@st.cache_data(ttl=5)
def fetch_cluster_status() -> dict[str, Any]:
    with httpx.Client(timeout=10.0) as client:
        resp = client.get(CLUSTER_STATUS_URL)
        resp.raise_for_status()
        return resp.json()


@st.cache_data(ttl=5)
def fetch_traces() -> dict[str, Any]:
    with httpx.Client(timeout=15.0) as client:
        resp = client.get(TRACES_URL)
        resp.raise_for_status()
        return resp.json()


@st.cache_data(ttl=5)
def fetch_cache_stats() -> dict[str, Any]:
    with httpx.Client(timeout=10.0) as client:
        resp = client.get(CACHE_STATS_URL)
        resp.raise_for_status()
        return resp.json()


def invalidate_cache() -> dict[str, Any]:
    with httpx.Client(timeout=10.0) as client:
        resp = client.post(CACHE_INVALIDATE_URL)
        resp.raise_for_status()
        return resp.json()


def load_benchmark_results() -> dict[str, Any] | None:
    if not BENCHMARK_PATH.exists():
        return None
    try:
        return json.loads(BENCHMARK_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def render_cluster_overview() -> None:
    st.title("Cluster Overview")
    st.caption("Auto-refreshes every 5 seconds while this page is open.")
    st.markdown('<meta http-equiv="refresh" content="5">', unsafe_allow_html=True)

    try:
        payload = fetch_cluster_status()
    except Exception as exc:
        st.error(f"Failed to fetch cluster status: {exc}")
        return

    workers = payload.get("workers", {})
    if not workers:
        st.warning("No worker status data available.")
        return

    status_cols = st.columns(len(workers))
    for idx, (worker_id, info) in enumerate(workers.items()):
        degraded = bool(info.get("degraded", True))
        label = "degraded" if degraded else "healthy"
        status_cols[idx].metric(worker_id, label)

    docs_df = pd.DataFrame(
        [
            {
                "worker_id": wid,
                "documents_indexed": int(info.get("documents_indexed", 0) or 0),
            }
            for wid, info in workers.items()
        ]
    )
    fig_docs = px.bar(docs_df, x="worker_id", y="documents_indexed", title="Documents Per Worker")
    st.plotly_chart(fig_docs, use_container_width=True)

    try:
        cache_stats = fetch_cache_stats()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Query Cache", "enabled" if cache_stats.get("enabled") else "disabled")
        c2.metric("Cache Hits", int(cache_stats.get("hits", 0)))
        c3.metric("Cache Misses", int(cache_stats.get("misses", 0)))
        c4.metric("Cache TTL", f"{int(cache_stats.get('ttl_seconds', 0))}s")
        if st.button("Invalidate Cache", use_container_width=True):
            result = invalidate_cache()
            st.cache_data.clear()
            st.success(f"Cache invalidated. New version: {result.get('version')}")
    except Exception as exc:
        st.info(f"Cache stats unavailable: {exc}")

    st.subheader("Memory Usage Per Worker")
    gauge_cols = st.columns(len(workers))
    max_mem = max([float(info.get("memory_usage_mb", 0.0) or 0.0) for info in workers.values()] + [500.0])
    for idx, (worker_id, info) in enumerate(workers.items()):
        mem = float(info.get("memory_usage_mb", 0.0) or 0.0)
        fig_gauge = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=mem,
                title={"text": worker_id},
                gauge={
                    "axis": {"range": [0, max_mem]},
                    "bar": {"color": "crimson" if bool(info.get("degraded", False)) else "seagreen"},
                },
            )
        )
        fig_gauge.update_layout(height=250, margin=dict(l=10, r=10, t=40, b=10))
        gauge_cols[idx].plotly_chart(fig_gauge, use_container_width=True)


def _flatten_trace_for_timeline(trace: dict[str, Any]) -> pd.DataFrame:
    spans = trace.get("spans", {})
    records: list[dict[str, Any]] = []
    if not spans:
        return pd.DataFrame(records)

    min_start = min(float(span.get("start_time", 0.0) or 0.0) for span in spans.values())

    for span_id, span in spans.items():
        start = float(span.get("start_time", 0.0) or 0.0)
        duration = float(span.get("duration", 0.0) or 0.0)
        records.append(
            {
                "span_id": span_id,
                "name": span.get("name", span_id),
                "parent_id": span.get("parent_id"),
                "start_ms": (start - min_start) * 1000.0,
                "end_ms": (start - min_start + duration) * 1000.0,
                "duration_ms": duration * 1000.0,
            }
        )
    return pd.DataFrame(records)


def _build_span_tree(spans: dict[str, Any], root_id: str | None) -> list[dict[str, Any]]:
    if not root_id or root_id not in spans:
        return []

    tree_rows: list[dict[str, Any]] = []

    def walk(span_id: str, depth: int) -> None:
        span = spans[span_id]
        tree_rows.append(
            {
                "span": f"{'  ' * depth}{span.get('name', span_id)}",
                "span_id": span_id,
                "parent": span.get("parent_id"),
                "duration_ms": float(span.get("duration", 0.0) or 0.0) * 1000.0,
                "error": span.get("error"),
            }
        )
        for child_id in span.get("children", []):
            if child_id in spans:
                walk(child_id, depth + 1)

    walk(root_id, 0)
    return tree_rows


def render_trace_explorer() -> None:
    st.title("Trace Explorer")

    try:
        payload = fetch_traces()
    except Exception as exc:
        st.error(f"Failed to fetch traces: {exc}")
        return

    traces = payload.get("traces", [])
    if not traces:
        st.warning("No traces available.")
        return

    options = []
    for i, trace in enumerate(traces):
        trace_id = trace.get("trace_id", f"trace-{i}")
        root_id = trace.get("root_span_id", "unknown")
        options.append((f"{i+1}. {trace_id} (root={root_id})", i))

    selected_label = st.selectbox("Select Trace", [o[0] for o in options])
    selected_index = dict(options)[selected_label]
    selected_trace = traces[selected_index]

    st.subheader("Trace Timeline (Gantt)")
    timeline_df = _flatten_trace_for_timeline(selected_trace)
    if timeline_df.empty:
        st.info("Selected trace has no span data.")
    else:
        fig = px.timeline(
            timeline_df,
            x_start="start_ms",
            x_end="end_ms",
            y="name",
            color="parent_id",
            hover_data=["span_id", "duration_ms"],
            title="Span Timeline (relative ms)",
        )
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Span Tree")
    spans = selected_trace.get("spans", {})
    root_id = selected_trace.get("root_span_id")
    tree_rows = _build_span_tree(spans, root_id)
    if not tree_rows:
        st.info("No rooted span tree found in selected trace.")
    else:
        tree_df = pd.DataFrame(tree_rows)
        st.dataframe(tree_df, use_container_width=True, hide_index=True)


def render_benchmark_results() -> None:
    st.title("Benchmark Results")
    data = load_benchmark_results()
    if not data:
        st.warning("benchmark_results.json not found or invalid.")
        return

    exp1 = data.get("experiment_1_ingestion_speed", {})
    exp2 = data.get("experiment_2_query_latency_under_load", {})
    exp3 = data.get("experiment_3_fault_tolerance", {})
    exp4 = data.get("experiment_4_scatter_gather_overhead", {})
    exp5 = data.get("experiment_5_query_cache_effectiveness", {})

    st.download_button(
        "Download benchmark_results.json",
        data=json.dumps(data, indent=2),
        file_name="benchmark_results.json",
        mime="application/json",
        use_container_width=True,
    )

    summary_cols = st.columns(4)
    summary_cols[0].metric("Ingestion Throughput", f"{float(exp1.get('overall_throughput_docs_per_s', 0.0)):.2f} docs/s")
    summary_cols[1].metric("Shard Imbalance", f"{float(exp1.get('shard_imbalance_pct', 0.0)):.1f}%")
    summary_cols[2].metric("Query P95", f"{float(exp2.get('p95_ms', 0.0)):.1f} ms")
    summary_cols[3].metric("Cache Speedup", f"{float(exp5.get('speedup_ratio', 0.0)):.2f}x")

    st.subheader("Latency Percentiles")
    latency_df = pd.DataFrame(
        {
            "percentile": ["AVG", "P50", "P95", "P99"],
            "latency_ms": [
                float(exp2.get("avg_ms", 0.0)),
                float(exp2.get("p50_ms", 0.0)),
                float(exp2.get("p95_ms", 0.0)),
                float(exp2.get("p99_ms", 0.0)),
            ],
        }
    )
    fig_latency = px.line(latency_df, x="percentile", y="latency_ms", markers=True, title="Query Latency Percentiles")
    st.plotly_chart(fig_latency, use_container_width=True)

    st.subheader("Per-Worker Throughput")
    throughput = exp1.get("per_worker_throughput_docs_per_s", {})
    throughput_df = pd.DataFrame(
        [{"worker_id": k, "throughput_docs_per_s": float(v)} for k, v in throughput.items()]
    )
    if not throughput_df.empty:
        fig_tp = px.bar(throughput_df, x="worker_id", y="throughput_docs_per_s", title="Ingestion Throughput Per Worker")
        st.plotly_chart(fig_tp, use_container_width=True)
    else:
        st.info("No throughput data available.")

    st.subheader("Query Cache Effectiveness")
    cache_df = pd.DataFrame(
        [
            {"mode": "Uncached", "avg_ms": float(exp5.get("uncached", {}).get("avg_ms", 0.0))},
            {"mode": "Cached", "avg_ms": float(exp5.get("cached", {}).get("avg_ms", 0.0))},
        ]
    )
    fig_cache = px.bar(cache_df, x="mode", y="avg_ms", title="Repeated Query Latency: Cache Off vs On")
    st.plotly_chart(fig_cache, use_container_width=True)

    cache_cols = st.columns(3)
    cache_cols[0].metric("Cache Hits", f"{int(exp5.get('cache_hits', 0))}/{int(exp5.get('cache_requests', 0))}")
    cache_cols[1].metric("Cached P95", f"{float(exp5.get('cached', {}).get('p95_ms', 0.0)):.1f} ms")
    cache_cols[2].metric("Uncached P95", f"{float(exp5.get('uncached', {}).get('p95_ms', 0.0)):.1f} ms")

    st.subheader("Fault Tolerance Timeline")
    baseline = float(exp3.get("baseline_latency_ms", 0.0))
    spike = float(exp3.get("failover_window_latency_ms", 0.0))
    recovered = baseline
    timeline_df = pd.DataFrame(
        [
            {"phase": "Pre-failure", "latency_ms": baseline},
            {"phase": "During failover", "latency_ms": spike},
            {"phase": "Post-recovery", "latency_ms": recovered},
        ]
    )
    fig_ft = px.line(timeline_df, x="phase", y="latency_ms", markers=True, title="Fault Tolerance Latency Timeline")
    st.plotly_chart(fig_ft, use_container_width=True)

    st.subheader("Scatter vs Single Worker")
    compare_df = pd.DataFrame(
        [
            {
                "mode": "Scatter-gather",
                "avg_ms": float(exp4.get("scatter_avg_ms", 0.0)),
                "p95_ms": float(exp4.get("scatter_p95_ms", 0.0)),
            },
            {
                "mode": "Single worker",
                "avg_ms": float(exp4.get("single_avg_ms", 0.0)),
                "p95_ms": float(exp4.get("single_p95_ms", 0.0)),
            },
        ]
    )
    fig_compare = px.bar(compare_df, x="mode", y=["avg_ms", "p95_ms"], barmode="group", title="Scatter-Gather Overhead")
    st.plotly_chart(fig_compare, use_container_width=True)

    col1, col2, col3 = st.columns(3)
    col1.metric("Success Rate", f"{float(exp3.get('success_rate', 0.0))*100:.1f}%")
    col2.metric("Recovery Time", f"{float(exp3.get('recovery_time_s', 0.0)):.2f}s")
    col3.metric("Scatter Overhead", f"{float(exp4.get('latency_difference_ms', 0.0)):.2f} ms")


page = st.sidebar.radio(
    "Navigate",
    ["Cluster Overview", "Trace Explorer", "Benchmark Results"],
)

if page == "Cluster Overview":
    render_cluster_overview()
elif page == "Trace Explorer":
    render_trace_explorer()
else:
    render_benchmark_results()
