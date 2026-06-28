"""
ValidateX Performance Benchmark Suite

Measures validation speed and memory utilization across synthetic datasets ranging from
100,000 to 1,000,000 rows. Compares Pandas vs. Polars execution engines.
"""

import sys
import time
import tracemalloc
import pandas as pd
import numpy as np

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import validatex as vx

def generate_synthetic_dataset(num_rows: int) -> pd.DataFrame:
    """Generate reproducible synthetic benchmark dataset."""
    np.random.seed(42)
    user_ids = np.arange(1, num_rows + 1)
    return pd.DataFrame({
        "user_id": user_ids,
        "age": np.random.randint(18, 90, size=num_rows),
        "email": np.char.add("user_", np.char.add(user_ids.astype(str), "@example.com")),
        "status": np.random.choice(["ACTIVE", "INACTIVE", "PENDING"], size=num_rows),
        "score": np.random.uniform(0.0, 100.0, size=num_rows),
    })

def run_benchmark():
    print("⚡ Running ValidateX Performance Benchmark Suite...\n")
    sizes = [100_000, 1_000_000, 10_000_000]
    results = []

    for size in sizes:
        print(f"🔄 Generating {size:,} rows dataset...")
        df_pd = generate_synthetic_dataset(size)

        # Build suite with 6 representative expectations
        suite = (
            vx.ExpectationSuite(f"benchmark_{size}")
            .add("expect_column_to_not_be_null", column="user_id")
            .add("expect_column_values_to_be_unique", column="user_id")
            .add("expect_column_values_to_be_between", column="age", min_value=18, max_value=120)
            .add("expect_column_values_to_be_in_set", column="status", value_set=["ACTIVE", "INACTIVE", "PENDING"])
            .add("expect_column_values_to_be_positive", column="score")
            .add("expect_table_row_count_to_equal", value=size)
        )

        # 1. Benchmark Pandas engine
        tracemalloc.start()
        t0 = time.perf_counter()
        res_pd = vx.validate(df_pd, suite, engine="pandas")
        t_pd = time.perf_counter() - t0
        _, mem_pd_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        mem_pd_mb = mem_pd_bytes / (1024 * 1024)

        # 2. Benchmark Polars engine
        try:
            import polars as pl
            df_pl = pl.from_pandas(df_pd)
            tracemalloc.start()
            t0 = time.perf_counter()
            res_pl = vx.validate(df_pl, suite, engine="polars")
            t_pl = time.perf_counter() - t0
            _, mem_pl_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            mem_pl_mb = mem_pl_bytes / (1024 * 1024)
        except Exception as e:
            t_pl = 0.0
            mem_pl_mb = 0.0

        results.append({
            "rows": f"{size:,}",
            "pd_time": f"{t_pd:.2f}s",
            "pd_mem": f"{mem_pd_mb:.1f} MB",
            "pl_time": f"{t_pl:.2f}s" if t_pl > 0 else "N/A",
            "pl_mem": f"{mem_pl_mb:.1f} MB" if mem_pl_mb > 0 else "N/A",
        })

    # Print Markdown Benchmark Table
    print("\n📊 Benchmark Results Summary Table:\n")
    print("| Dataset Size | Engine | Execution Time | Peak Memory | Setup / API Lines |")
    print("|---|---|---|---|---|")
    for r in results:
        print(f"| {r['rows']} rows | Pandas | {r['pd_time']} | {r['pd_mem']} | 5 lines |")
        print(f"| {r['rows']} rows | Polars | {r['pl_time']} | {r['pl_mem']} | 5 lines |")

if __name__ == "__main__":
    run_benchmark()
