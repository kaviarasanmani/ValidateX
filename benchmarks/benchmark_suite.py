"""
ValidateX Reproducible Performance Benchmark Suite

Measures validation speed (median ± std over multiple runs after warmup) and memory
utilization across synthetic datasets ranging from 100,000 to 10,000,000 rows.
Automatically records hardware specifications, OS, Python version, and engine versions
so results can be independently reproduced and audited.
"""

from __future__ import annotations

import gc
import json
import os
import platform
import statistics
import sys
import time
import tracemalloc
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import validatex as vx

SEED = 42
WARMUP_RUNS = 1
BENCHMARK_RUNS = 3


def get_process_rss_mb() -> Optional[float]:
    """Return current process Resident Set Size (RSS) in MiB across platforms."""
    try:
        import psutil  # type: ignore

        return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    except ImportError:
        pass

    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes

            class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            pmc = PROCESS_MEMORY_COUNTERS()
            pmc.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            if ctypes.windll.psapi.GetProcessMemoryInfo(handle, ctypes.byref(pmc), pmc.cb):
                return float(pmc.WorkingSetSize) / (1024 * 1024)
        except Exception:
            return None
    else:
        try:
            import resource

            usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if sys.platform == "darwin":
                return float(usage) / (1024 * 1024)
            return float(usage) / 1024.0
        except Exception:
            return None
    return None


def get_total_ram_gb() -> str:
    """Return total system RAM in GB."""
    try:
        import psutil  # type: ignore

        return f"{psutil.virtual_memory().total / (1024 ** 3):.1f} GB"
    except ImportError:
        pass

    if sys.platform == "win32":
        try:
            import ctypes

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            stat = MEMORYSTATUSEX()
            stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
            return f"{stat.ullTotalPhys / (1024 ** 3):.1f} GB"
        except Exception:
            pass
    return "Unknown"


def collect_machine_specs() -> Dict[str, Any]:
    """Collect hardware, OS, and package version metadata for benchmark reproducibility."""
    specs: Dict[str, Any] = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "os": platform.platform(),
        "architecture": platform.machine(),
        "processor": platform.processor() or "Unknown CPU",
        "logical_cores": os.cpu_count() or 1,
        "system_ram": get_total_ram_gb(),
        "python_version": platform.python_version(),
        "rng_seed": SEED,
        "warmup_runs": WARMUP_RUNS,
        "measured_runs": BENCHMARK_RUNS,
        "versions": {
            "validatex": getattr(vx, "__version__", "1.2.0"),
            "pandas": pd.__version__,
            "numpy": np.__version__,
        },
    }
    try:
        import polars as pl

        specs["versions"]["polars"] = pl.__version__
    except ImportError:
        specs["versions"]["polars"] = "not installed"

    try:
        import pyspark

        specs["versions"]["pyspark"] = pyspark.__version__
    except ImportError:
        specs["versions"]["pyspark"] = "not installed"

    return specs


def generate_synthetic_dataset(num_rows: int, seed: int = SEED) -> pd.DataFrame:
    """Generate deterministic synthetic benchmark dataset."""
    rng = np.random.default_rng(seed)
    user_ids = np.arange(1, num_rows + 1, dtype=np.int64)
    ages = rng.integers(18, 90, size=num_rows, dtype=np.int32)
    statuses = rng.choice(["ACTIVE", "INACTIVE", "PENDING"], size=num_rows)
    scores = rng.uniform(0.0, 100.0, size=num_rows)
    return pd.DataFrame(
        {
            "user_id": user_ids,
            "age": ages,
            "email": [f"user_{i}@example.com" for i in range(1, num_rows + 1)]
            if num_rows <= 1_000_000
            else np.char.add("user_", np.char.add(user_ids.astype(str), "@example.com")),
            "status": statuses,
            "score": scores,
        }
    )


def build_benchmark_suite(num_rows: int) -> vx.ExpectationSuite:
    """Build standard 5-rule expectation suite for benchmarking."""
    return (
        vx.ExpectationSuite(f"benchmark_{num_rows}")
        .add("expect_column_to_not_be_null", column="user_id")
        .add("expect_column_values_to_be_unique", column="user_id")
        .add("expect_column_values_to_be_between", column="age", min_value=18, max_value=120)
        .add("expect_column_values_to_be_in_set", column="status", value_set=["ACTIVE", "INACTIVE", "PENDING"])
        .add("expect_table_row_count_to_equal", value=num_rows)
    )


def _measure_engine_runs(
    data: Any,
    suite: vx.ExpectationSuite,
    engine: str,
    warmup: int = WARMUP_RUNS,
    runs: int = BENCHMARK_RUNS,
) -> Dict[str, Any]:
    """Run warmup + timed iterations and measure median time, std dev, and memory."""
    for _ in range(warmup):
        vx.validate(data, suite, engine=engine)

    gc.collect()
    rss_before = get_process_rss_mb()
    tracemalloc.start()

    timings: List[float] = []
    for _ in range(runs):
        t0 = time.perf_counter()
        res = vx.validate(data, suite, engine=engine)
        elapsed = time.perf_counter() - t0
        assert res.success is True, f"Benchmark suite unexpectedly failed on {engine}"
        timings.append(elapsed)

    _, py_peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss_after = get_process_rss_mb()

    py_peak_mb = py_peak_bytes / (1024 * 1024)
    rss_delta_mb = max(0.0, (rss_after - rss_before)) if (rss_before is not None and rss_after is not None) else 0.0
    effective_mem_mb = max(py_peak_mb, rss_delta_mb)

    median_s = statistics.median(timings)
    stdev_s = statistics.stdev(timings) if len(timings) > 1 else 0.0

    return {
        "engine": engine,
        "median_seconds": round(median_s, 4),
        "stdev_seconds": round(stdev_s, 4),
        "runs_seconds": [round(t, 4) for t in timings],
        "python_heap_peak_mb": round(py_peak_mb, 2),
        "rss_delta_mb": round(rss_delta_mb, 2),
        "peak_memory_mb": round(effective_mem_mb, 2),
    }


def run_benchmark(sizes: Optional[List[int]] = None, output_json: Optional[str] = None) -> Dict[str, Any]:
    """Execute the full benchmark suite and print hardware specs + Markdown table."""
    if sizes is None:
        sizes = [100_000, 1_000_000, 10_000_000]

    specs = collect_machine_specs()
    print("⚡ ValidateX Reproducible Performance Benchmark Suite")
    print("=" * 68)
    print(f"  OS / Platform  : {specs['os']}")
    print(f"  CPU            : {specs['processor']} ({specs['logical_cores']} logical cores)")
    print(f"  System RAM     : {specs['system_ram']}")
    print(f"  Python         : {specs['python_version']}")
    print(
        f"  Engine Versions: pandas={specs['versions']['pandas']}, "
        f"polars={specs['versions']['polars']}, pyspark={specs['versions']['pyspark']}"
    )
    print(f"  Methodology    : seed={SEED}, warmup={WARMUP_RUNS}, measured_runs={BENCHMARK_RUNS} (median ± std)")
    print("=" * 68 + "\n")

    spark = None
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("ValidateXBenchmark").master("local[2]").getOrCreate()
    except Exception:
        pass

    all_results: List[Dict[str, Any]] = []

    for size in sizes:
        print(f"🔄 Generating deterministic {size:,} rows dataset (seed={SEED})...")
        df_pd = generate_synthetic_dataset(size, seed=SEED)
        suite = build_benchmark_suite(size)

        # 1. Pandas
        pd_stats = _measure_engine_runs(df_pd, suite, engine="pandas")
        all_results.append({"rows": size, **pd_stats})

        # 2. Polars
        try:
            import polars as pl

            df_pl = pl.from_pandas(df_pd)
            pl_stats = _measure_engine_runs(df_pl, suite, engine="polars")
            all_results.append({"rows": size, **pl_stats})
        except Exception as exc:
            all_results.append(
                {
                    "rows": size,
                    "engine": "polars",
                    "median_seconds": None,
                    "stdev_seconds": None,
                    "peak_memory_mb": None,
                    "error": str(exc),
                }
            )

        # 3. PySpark (up to 1M rows locally)
        if spark and size <= 1_000_000:
            try:
                df_sp = spark.createDataFrame(df_pd).cache()
                df_sp.count()  # Materialize cache before timing validation rules
                sp_stats = _measure_engine_runs(df_sp, suite, engine="spark")
                df_sp.unpersist()
                all_results.append({"rows": size, **sp_stats})
            except Exception as exc:
                all_results.append(
                    {
                        "rows": size,
                        "engine": "spark",
                        "median_seconds": None,
                        "stdev_seconds": None,
                        "peak_memory_mb": None,
                        "error": str(exc),
                    }
                )

    if spark:
        spark.stop()

    print("\n📊 Benchmark Results Summary Table:\n")
    print("| Dataset Size | Engine | Median Time (± Std) | Peak Memory (Heap/RSS) | Expectations |")
    print("|---|---|---|---|---|")
    for r in all_results:
        rows_fmt = f"{r['rows']:,} rows"
        eng_label = {"pandas": "Pandas", "polars": "Polars", "spark": "PySpark"}.get(r["engine"], r["engine"])
        if r.get("median_seconds") is not None:
            time_str = f"{r['median_seconds']:.3f}s ± {r['stdev_seconds']:.3f}s"
            mem_str = f"{r['peak_memory_mb']:.1f} MB (py: {r['python_heap_peak_mb']:.1f} MB)"
        else:
            time_str = "N/A"
            mem_str = "N/A"
        print(f"| {rows_fmt} | {eng_label} | {time_str} | {mem_str} | 5 rules |")

    payload = {"machine_specs": specs, "results": all_results}
    if output_json:
        out_path = Path(output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"\n💾 Saved reproducible benchmark report to: {out_path}")

    return payload


if __name__ == "__main__":
    default_out = str(Path(__file__).resolve().parent / "latest_benchmark_results.json")
    run_benchmark(output_json=default_out)
