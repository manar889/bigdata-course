#!/usr/bin/env python3
"""
prepare_tp_data.py — build every TP dataset for the Distributed Systems / Big Data course
from ONE seed file, locally, on the student's machine.

Why this exists:
  Students download ~70 MB (one seed parquet + one 12 KB lookup CSV).
  Everything else — the 2 GB CSV for S1, the 10,000 small files for S6, the dirty
  data for S8 — is GENERATED here. Nobody downloads gigabytes over a home connection,
  and every student ends up with byte-identical inputs (see MANIFEST.json).

Usage:
    python prepare_tp_data.py --out /data                    # normal (needs seed)
    python prepare_tp_data.py --out /data --profile small    # 10x smaller, for smoke tests
    python prepare_tp_data.py --out /data --synthetic        # no seed needed, fabricates one
    python prepare_tp_data.py --out /data --verify           # check an existing build

Run time: ~6-12 min on a 4-core laptop (full profile). Disk: ~4.5 GB.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import gc
import shutil
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SEED = 20260915  # fixed: every student must get identical data

# ---------------------------------------------------------------- profiles
PROFILES = {
    "full": dict(
        s1_target_bytes=2_200_000_000,   # S1: must not fit comfortably in the container's RAM cap
        slice_rows=2_000_000,            # S2/S3/S4: pure-Python + first Spark job
        s6_rows=1_000_000,               # S6: gets written as CSV + JSON + Parquet
        s6_small_files=10_000,           # S6: the small-files pathology
        s7_rows=10_000_000,              # S7: the deliberately pathological job
        s8_rows=1_500_000,               # S8: dirty data
    ),
    "small": dict(
        s1_target_bytes=220_000_000,
        slice_rows=200_000,
        s6_rows=100_000,
        s6_small_files=1_000,
        s7_rows=1_000_000,
        s8_rows=150_000,
    ),
    "tiny": dict(  # CI / teacher smoke test only
        s1_target_bytes=12_000_000,
        slice_rows=20_000,
        s6_rows=10_000,
        s6_small_files=100,
        s7_rows=50_000,
        s8_rows=15_000,
    ),
}

# TLC yellow-taxi schema (2024 vintage). Kept explicit so the synthetic
# fallback is schema-identical to the real thing.
YELLOW_COLS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
]

BOROUGHS = ["Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island", "EWR", "Unknown"]
# Real yellow-taxi pickup distribution is ~90% Manhattan. That skew is the whole point of S2/S7.
BOROUGH_P = [0.902, 0.055, 0.028, 0.009, 0.002, 0.002, 0.002]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} TB"


def dir_size(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


def sha256_head(path: Path, limit: int = 8_000_000) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read(limit))
    return h.hexdigest()[:16]


# ---------------------------------------------------------------- zones
def build_zones(out: Path, zones_csv: Path | None) -> pd.DataFrame:
    """The 265-row taxi zone lookup. This is the small side of the S7 broadcast join."""
    dest = out / "zones" / "taxi_zone_lookup.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)

    if zones_csv and zones_csv.exists():
        # The entrypoint downloads straight into this destination, so src and dest
        # are often the SAME file. shutil.copy raises SameFileError on that.
        if zones_csv.resolve() != dest.resolve():
            shutil.copy(zones_csv, dest)
        df = pd.read_csv(dest)
        # A dropped download can leave an HTML error page with a .csv name. Check the
        # shape before trusting it: a wrong zone table silently destroys the skew that
        # sessions 2 and 7 are built on, and you would not find out until you taught it.
        need = {"LocationID", "Borough"}
        if need.issubset(df.columns) and len(df) > 100:
            log(f"  zones: {len(df)} rows from {dest.name}")
            return df
        log(f"  zones file at {dest} looks wrong "
            f"({len(df)} rows, columns {list(df.columns)[:4]}) -> fabricating instead")
        dest.unlink(missing_ok=True)

    log("zone lookup not supplied -> fabricating a schema-identical one")
    rng = np.random.default_rng(SEED)
    n = 265
    df = pd.DataFrame({
        "LocationID": np.arange(1, n + 1),
        # real lookup has ~69 Manhattan / 69 Queens / 61 Brooklyn / 43 Bronx / 20 SI zones
        "Borough": rng.permutation(np.repeat(
            ["Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island"], [69, 69, 61, 43, 23])),
        "Zone": [f"Zone {i:03d}" for i in range(1, n + 1)],
        "service_zone": rng.choice(["Yellow Zone", "Boro Zone", "Airports", "EWR"],
                                   size=n, p=[0.35, 0.55, 0.08, 0.02]),
    })
    df.to_csv(dest, index=False)
    return df


# ---------------------------------------------------------------- seed
def synthesize_seed(rows: int, zones: pd.DataFrame) -> pd.DataFrame:
    """
    Fabricate a TLC-yellow-shaped seed. Two uses:
      1. offline fallback if cloudfront is unreachable on the day
      2. lets you test this whole pipeline without downloading anything
    The skew, the null rate and the fare distribution are modelled on the real file.
    """
    rng = np.random.default_rng(SEED)
    start = pd.Timestamp("2024-01-01")
    secs = rng.integers(0, 31 * 24 * 3600, size=rows)
    pickup = start + pd.to_timedelta(np.sort(secs), unit="s")
    dur = rng.gamma(shape=2.0, scale=420, size=rows).astype("int64") + 60

    # Draw the borough with the real (heavily skewed) distribution, THEN pick a zone id
    # that actually belongs to that borough. If you skip the second step, widen() joins the
    # lookup table and overwrites the borough with a uniform one -> the skew disappears and
    # S2 and S7 have nothing to teach.
    by_borough = {b: zones.loc[zones["Borough"] == b, "LocationID"].to_numpy()
                  for b in zones["Borough"].unique()}
    avail = [b for b in BOROUGHS if b in by_borough and len(by_borough[b])]
    probs = np.array([BOROUGH_P[BOROUGHS.index(b)] for b in avail], dtype=float)
    probs /= probs.sum()
    chosen = rng.choice(len(avail), size=rows, p=probs)
    pu = np.empty(rows, dtype="int64")
    for i, b in enumerate(avail):
        m = chosen == i
        pu[m] = rng.choice(by_borough[b], size=int(m.sum()))

    dist = np.round(rng.gamma(shape=1.6, scale=2.1, size=rows), 2)
    fare = np.round(3.0 + dist * 2.8 + rng.normal(0, 2.0, size=rows), 2)
    tip = np.round(np.maximum(0, fare * rng.beta(2, 8, size=rows) * 2.2), 2)

    df = pd.DataFrame({
        "VendorID": rng.choice([1, 2, 6], size=rows, p=[0.32, 0.66, 0.02]).astype("int32"),
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + pd.to_timedelta(dur, unit="s"),
        "passenger_count": rng.choice([1, 2, 3, 4, 5, 6], size=rows,
                                      p=[0.72, 0.14, 0.05, 0.03, 0.04, 0.02]).astype("float64"),
        "trip_distance": dist,
        "RatecodeID": rng.choice([1.0, 2.0, 3.0, 4.0, 5.0], size=rows,
                                 p=[0.93, 0.04, 0.01, 0.01, 0.01]),
        "store_and_fwd_flag": rng.choice(["N", "Y"], size=rows, p=[0.98, 0.02]),
        "PULocationID": pu.astype("int32"),
        "DOLocationID": rng.integers(1, 266, size=rows).astype("int32"),
        "payment_type": rng.choice([1, 2, 3, 4], size=rows, p=[0.78, 0.19, 0.02, 0.01]).astype("int64"),
        "fare_amount": fare,
        "extra": np.round(rng.choice([0.0, 1.0, 2.5, 3.5], size=rows), 2),
        "mta_tax": np.full(rows, 0.5),
        "tip_amount": tip,
        "tolls_amount": np.round(rng.choice([0.0, 6.94], size=rows, p=[0.94, 0.06]), 2),
        "improvement_surcharge": np.full(rows, 1.0),
        "congestion_surcharge": np.round(rng.choice([0.0, 2.5], size=rows, p=[0.12, 0.88]), 2),
        "airport_fee": np.round(rng.choice([0.0, 1.75], size=rows, p=[0.93, 0.07]), 2),
    })
    df["total_amount"] = np.round(
        df.fare_amount + df.extra + df.mta_tax + df.tip_amount + df.tolls_amount
        + df.improvement_surcharge + df.congestion_surcharge + df.airport_fee, 2)
    return df[YELLOW_COLS]


def load_seed(seed_path: Path | None, synthetic: bool, rows_needed: int,
              zones: pd.DataFrame) -> pd.DataFrame:
    if synthetic or seed_path is None or not seed_path.exists():
        if not synthetic:
            log(f"WARNING: seed not found at {seed_path} -> falling back to synthetic")
        n = max(rows_needed, 3_000_000 if rows_needed > 500_000 else rows_needed)
        log(f"synthesizing seed: {n:,} rows")
        return synthesize_seed(n, zones)
    log(f"reading seed: {seed_path}")
    df = pq.read_table(seed_path).to_pandas()

    # TLC is not consistent with itself. The 2024 files ship "Airport_fee" with a
    # capital A; other months ship "airport_fee". 2025+ adds "cbd_congestion_fee".
    # Match case-insensitively and rename to one canonical spelling.
    lower = {c.lower(): c for c in df.columns}
    renames = {lower[c.lower()]: c for c in YELLOW_COLS if c.lower() in lower
               and lower[c.lower()] != c}
    if renames:
        log(f"  normalising column names: {renames}")
        df = df.rename(columns=renames)

    # Only these are actually load-bearing. Everything else gets a sane default,
    # so one retired surcharge column does not kill the whole build.
    REQUIRED = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
                "DOLocationID", "trip_distance", "fare_amount"]
    hard_missing = [c for c in REQUIRED if c not in df.columns]
    if hard_missing:
        sys.exit(f"FATAL: seed is missing required columns {hard_missing}.\n"
                 f"Is this a yellow_tripdata file? Got: {list(df.columns)}")

    DEFAULTS = {"VendorID": 1, "passenger_count": 1.0, "RatecodeID": 1.0,
                "store_and_fwd_flag": "N", "payment_type": 1, "extra": 0.0,
                "mta_tax": 0.0, "tip_amount": 0.0, "tolls_amount": 0.0,
                "improvement_surcharge": 0.0, "total_amount": 0.0,
                "congestion_surcharge": 0.0, "airport_fee": 0.0}
    filled = [c for c in YELLOW_COLS if c not in df.columns]
    for c in filled:
        df[c] = DEFAULTS.get(c, 0.0)
    if filled:
        log(f"  seed has no {filled} -> filled with defaults (harmless: "
            f"no TP depends on these)")

    return df[YELLOW_COLS]


# ---------------------------------------------------------------- widening
def widen(df: pd.DataFrame, zones: pd.DataFrame) -> pd.DataFrame:
    """
    Turn the 19-column raw file into a ~34-column curated table.

    Needed because S6's TP says 'read 1 column out of 30' and raw yellow has 19.
    Every added column is a real derivation a data engineer would actually write —
    no padding with junk.
    """
    z = zones.rename(columns={"LocationID": "PULocationID", "Borough": "pickup_borough",
                              "Zone": "pickup_zone", "service_zone": "pickup_service_zone"})
    out = df.merge(z[["PULocationID", "pickup_borough", "pickup_zone", "pickup_service_zone"]],
                   on="PULocationID", how="left")
    zd = zones.rename(columns={"LocationID": "DOLocationID", "Borough": "dropoff_borough",
                               "Zone": "dropoff_zone"})
    out = out.merge(zd[["DOLocationID", "dropoff_borough", "dropoff_zone"]],
                    on="DOLocationID", how="left")

    pu = pd.to_datetime(out["tpep_pickup_datetime"])
    do = pd.to_datetime(out["tpep_dropoff_datetime"])
    out["pickup_date"] = pu.dt.date.astype("string")
    out["pickup_year"] = pu.dt.year.astype("int16")
    out["pickup_month"] = pu.dt.month.astype("int8")
    out["pickup_day"] = pu.dt.day.astype("int8")
    out["pickup_hour"] = pu.dt.hour.astype("int8")
    out["pickup_dayofweek"] = pu.dt.dayofweek.astype("int8")
    out["is_weekend"] = out["pickup_dayofweek"].isin([5, 6])
    out["trip_duration_sec"] = (do - pu).dt.total_seconds().astype("float64")
    out["trip_duration_min"] = (out["trip_duration_sec"] / 60).round(2)
    out["avg_speed_mph"] = np.where(
        out["trip_duration_sec"] > 0,
        (out["trip_distance"] / (out["trip_duration_sec"] / 3600)).round(2), np.nan)
    out["tip_pct"] = np.where(out["fare_amount"] > 0,
                              (out["tip_amount"] / out["fare_amount"] * 100).round(2), np.nan)
    out["fare_per_mile"] = np.where(out["trip_distance"] > 0,
                                    (out["fare_amount"] / out["trip_distance"]).round(2), np.nan)
    out["is_airport_trip"] = out["airport_fee"].fillna(0) > 0
    out["payment_label"] = out["payment_type"].map(
        {1: "credit_card", 2: "cash", 3: "no_charge", 4: "dispute"}).fillna("other")
    out["pickup_borough"] = out["pickup_borough"].fillna("Unknown")
    out["dropoff_borough"] = out["dropoff_borough"].fillna("Unknown")

    # Low-cardinality strings as categories. Without this the frame is ~2.5 GB of
    # Python string objects in a 4 GB container and the build is OOM-killed at S6.
    for c in ("pickup_borough", "dropoff_borough", "pickup_zone", "dropoff_zone",
              "pickup_service_zone", "payment_label", "store_and_fwd_flag",
              "pickup_date"):
        if c in out.columns:
            out[c] = out[c].astype("category")
    return out


# ---------------------------------------------------------------- builders
def build_s1(out: Path, wide: pd.DataFrame, target_bytes: int) -> dict:
    """
    S1: one CSV that does not fit. Written by repeating the slice until we hit the target
    size, so the file size is deterministic across machines (the whole TP is a measurement).
    """
    d = out / "s1"; d.mkdir(parents=True, exist_ok=True)
    dest = d / "trips_big.csv"
    if dest.exists():
        dest.unlink()

    step = 250_000
    header = True
    written = 0
    reps = 0
    while written < target_bytes:
        start = (reps * step) % max(1, len(wide))
        chunk = wide.iloc[start:start + step]
        if len(chunk) == 0:
            chunk = wide.head(step)
        chunk.to_csv(dest, mode="a", index=False, header=header)
        header = False
        written = dest.stat().st_size
        reps += 1
        if reps % 4 == 0:
            log(f"  s1: {human(written)} / {human(target_bytes)}")
    log(f"  s1: {dest.name} = {human(dest.stat().st_size)}")
    return {"path": "s1/trips_big.csv", "bytes": dest.stat().st_size, "repeats": reps}


def build_slice(out: Path, wide: pd.DataFrame, rows: int) -> dict:
    """S2/S3/S4 share one file. The 'same problem, three times' thread depends on that."""
    d = out / "shared"; d.mkdir(parents=True, exist_ok=True)
    dest = d / "trips_slice.parquet"
    sl = wide.head(rows)
    sl.to_parquet(dest, index=False, compression="snappy")

    # pure-Python sessions (S2, S3) also get a CSV: no pyarrow required to read it
    csv_dest = d / "trips_slice.csv"
    sl.head(min(rows, 400_000)).to_csv(csv_dest, index=False)
    return {"parquet": human(dest.stat().st_size), "csv": human(csv_dest.stat().st_size),
            "rows": int(len(sl))}


def build_s6(out: Path, wide: pd.DataFrame, rows: int, n_small: int) -> dict:
    """S6: the same data in three formats, plus partitioned, plus the small-files pathology."""
    d = out / "s6"; d.mkdir(parents=True, exist_ok=True)
    base = wide.head(rows)

    base.to_parquet(d / "trips.parquet", index=False, compression="snappy")
    base.to_csv(d / "trips.csv", index=False)
    # Streamed in chunks. to_json() on the whole frame builds the entire JSON
    # document as a single Python string first — roughly 1.2 GB, which is what
    # killed the container.
    with open(d / "trips.json", "w") as fh:
        for i in range(0, len(base), 100_000):
            txt = base.iloc[i:i + 100_000].to_json(
                orient="records", lines=True, date_format="iso")
            fh.write(txt if txt.endswith("\n") else txt + "\n")

    # partitioned by pickup_date (partition pruning target)
    part = d / "trips_partitioned"
    if part.exists():
        shutil.rmtree(part)
    pq.write_to_dataset(pa.Table.from_pandas(base, preserve_index=False),
                        root_path=str(part), partition_cols=["pickup_date"])

    # non-partitioned copy of the SAME rows, so the pruning comparison is honest
    base.to_parquet(d / "trips_flat.parquet", index=False, compression="snappy")

    # the small-files disaster, pre-generated (students learn nothing from watching the loop)
    small = d / "trips_smallfiles"
    if small.exists():
        shutil.rmtree(small)
    small.mkdir(parents=True)
    per = max(1, len(base) // n_small)
    log(f"  s6: writing {n_small:,} small files (~{per} rows each) — this is the slow part")
    for i in range(n_small):
        base.iloc[i * per:(i + 1) * per].to_parquet(
            small / f"part-{i:05d}.parquet", index=False, compression="snappy")
        if (i + 1) % 2000 == 0:
            log(f"    {i + 1:,}/{n_small:,}")

    return {
        "csv": human((d / "trips.csv").stat().st_size),
        "json": human((d / "trips.json").stat().st_size),
        "parquet": human((d / "trips.parquet").stat().st_size),
        "partitioned": human(dir_size(part)),
        "smallfiles": human(dir_size(small)),
        "smallfile_count": n_small,
        "rows": int(len(base)),
    }


def build_s7(out: Path, wide: pd.DataFrame, zones: pd.DataFrame, rows: int) -> dict:
    """
    S7: the pathological job's input.
    Big side = trips (skewed on pickup_borough, ~90% Manhattan — this is REAL skew, not injected).
    Small side = zones (265 rows) -> the broadcast-join fix.
    """
    d = out / "s7"; d.mkdir(parents=True, exist_ok=True)
    # ~1 file per calendar day => ~30 input tasks in the scan stage, so the Spark UI
    # actually shows a task distribution instead of a single bar.
    root = d / "trips_skewed"
    if root.exists():
        shutil.rmtree(root)

    # Written rep by rep. pd.concat([wide] * reps) built the whole 10M-row frame in
    # memory first (~4 GB) and was OOM-killed. Each rep lands as additional files
    # inside the same day partitions, which is what gives Spark ~30 input tasks.
    written = 0
    rep = 0
    while written < rows:
        take = min(len(wide), rows - written)
        chunk = wide.iloc[:take]
        pq.write_to_dataset(
            pa.Table.from_pandas(chunk, preserve_index=False),
            root_path=str(root), partition_cols=["pickup_day"],
            basename_template=f"part-{rep:03d}-{{i}}.parquet",
            existing_data_behavior="overwrite_or_ignore")
        written += take
        rep += 1
        log(f"  s7: {written:,}/{rows:,} rows")
    zones.to_parquet(d / "zones.parquet", index=False)

    share = wide["pickup_borough"].value_counts(normalize=True)
    return {"rows": int(written), "bytes": human(dir_size(root)),
            "top_key": str(share.index[0]), "top_key_share": round(float(share.iloc[0]), 4)}


def build_s8(out: Path, wide: pd.DataFrame, rows: int) -> dict:
    """
    S8: dirty data with a KNOWN, documented defect set.
    You must be able to grade 'did the quality gate catch it', which means you must
    know exactly what is wrong and how much of it there is.
    """
    d = out / "s8"; d.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(SEED + 1)
    df = wide.head(rows).copy().reset_index(drop=True)
    # pickup_date is categorical now; the defect values below are not in its
    # categories, so assignment would raise. Back to plain strings for this table.
    df["pickup_date"] = df["pickup_date"].astype(str)
    n = len(df)
    manifest = {}

    # 1. nulls in a key column
    idx = rng.choice(n, size=int(n * 0.012), replace=False)
    df.loc[idx, "PULocationID"] = np.nan
    manifest["null_PULocationID"] = len(idx)

    # 3. out-of-range fares (negative and absurd)
    neg = rng.choice(n, size=int(n * 0.004), replace=False)
    df.loc[neg, "fare_amount"] = -df.loc[neg, "fare_amount"].abs()
    manifest["negative_fare"] = len(neg)
    huge = rng.choice(n, size=int(n * 0.001), replace=False)
    df.loc[huge, "fare_amount"] = 99_999.99
    manifest["absurd_fare"] = len(huge)

    # 4. impossible passenger counts
    zero = rng.choice(n, size=int(n * 0.03), replace=False)
    df.loc[zero, "passenger_count"] = 0
    manifest["zero_passengers"] = len(zero)

    # 5. dates outside the period (the classic TLC defect: 2002 and 2098 timestamps)
    bad = rng.choice(n, size=int(n * 0.002), replace=False)
    half = len(bad) // 2
    df.loc[bad[:half], "pickup_date"] = "2002-01-01"
    df.loc[bad[half:], "pickup_date"] = "2098-06-15"
    manifest["out_of_range_date"] = len(bad)

    # 6. dropoff before pickup
    rev = rng.choice(n, size=int(n * 0.0015), replace=False)
    df.loc[rev, "trip_duration_sec"] = -df.loc[rev, "trip_duration_sec"].abs()
    manifest["negative_duration"] = len(rev)

    # 7. malformed date strings (forces a parse-failure path, not just a filter)
    mal = rng.choice(n, size=int(n * 0.001), replace=False)
    df.loc[mal, "pickup_date"] = "31/02/2024"
    manifest["malformed_date_string"] = len(mal)

    # 8. schema drift: a column that only exists for part of the data.
    #    Mirrors the real TLC change (cbd_congestion_fee appears from 2025).
    df["cbd_congestion_fee"] = np.nan
    late = df.index[int(n * 0.7):]
    df.loc[late, "cbd_congestion_fee"] = 0.75
    manifest["schema_drift_column"] = "cbd_congestion_fee (null for first 70% of rows)"

    # duplicates LAST, so they are exact copies of the rows as finally written
    dup_idx = rng.choice(n, size=int(n * 0.02), replace=False)
    dups = df.iloc[dup_idx].copy()
    manifest["duplicate_rows"] = len(dups)

    dirty = pd.concat([df, dups], ignore_index=True).sample(frac=1, random_state=SEED)
    dirty.to_parquet(d / "trips_dirty.parquet", index=False)

    manifest["measured"] = {
        "total_rows": int(len(dirty)),
        "null_PULocationID": int(dirty["PULocationID"].isna().sum()),
        "fare_lt_0": int((dirty["fare_amount"] < 0).sum()),
        "fare_gt_10000": int((dirty["fare_amount"] > 10_000).sum()),
        "passenger_count_eq_0": int((dirty["passenger_count"] == 0).sum()),
        "duration_lt_0": int((dirty["trip_duration_sec"] < 0).sum()),
        "exact_duplicates": int(dirty.duplicated().sum()),
        "unparseable_or_out_of_range_date": int(
            (~dirty["pickup_date"].astype(str).str.match(r"^202[4-6]-")).sum()),
    }
    manifest["total_rows_written"] = int(len(dirty))
    manifest["clean_rows_expected"] = int(len(df) - sum(
        manifest[k] for k in ["null_PULocationID", "negative_fare", "absurd_fare",
                              "zero_passengers", "out_of_range_date", "negative_duration",
                              "malformed_date_string"]))
    manifest["_note"] = ("clean_rows_expected is an UPPER bound: defect indices were drawn "
                         "independently so a few rows carry two defects. Use it as a sanity "
                         "range, not an exact grading key.")
    (d / "defect_manifest.json").write_text(json.dumps(manifest, indent=2))

    # second day, same schema — for the idempotency / re-run TP in S8 TODO 3
    day2 = wide.iloc[rows:rows * 2].copy()
    if len(day2) > 0:
        day2["cbd_congestion_fee"] = 0.75
        day2.to_parquet(d / "trips_dirty_day2.parquet", index=False)

    return manifest


# ---------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="/data", type=Path)
    ap.add_argument("--seed-file", default=None, type=Path,
                    help="yellow_tripdata_YYYY-MM.parquet")
    ap.add_argument("--zones-file", default=None, type=Path, help="taxi_zone_lookup.csv")
    ap.add_argument("--profile", choices=list(PROFILES), default="full")
    ap.add_argument("--synthetic", action="store_true",
                    help="fabricate the seed instead of reading one (offline fallback)")
    ap.add_argument("--force", action="store_true", help="rebuild even if MANIFEST.json exists")
    ap.add_argument("--verify", action="store_true", help="only check an existing build")
    args = ap.parse_args()

    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = out / "MANIFEST.json"

    if args.verify:
        if not manifest_path.exists():
            print("FAIL: no MANIFEST.json — data was never built")
            return 1
        m = json.loads(manifest_path.read_text())
        missing = [p for p in m["expected_paths"] if not (out / p).exists()]
        if missing:
            print(f"FAIL: missing {missing}")
            return 1
        print(f"OK: build '{m['profile']}' complete, {len(m['expected_paths'])} paths present")
        return 0

    if manifest_path.exists() and not args.force:
        log("MANIFEST.json already present — nothing to do (use --force to rebuild)")
        return 0

    p = PROFILES[args.profile]
    t0 = time.time()
    log(f"profile={args.profile}  out={out}")

    zones = build_zones(out, args.zones_file)
    rows_needed = max(p["slice_rows"], p["s6_rows"], p["s8_rows"] * 2)
    raw = load_seed(args.seed_file, args.synthetic, rows_needed, zones)
    log(f"seed rows: {len(raw):,}")

    log("widening to the curated schema")
    wide = widen(raw, zones)
    del raw
    gc.collect()
    log(f"  curated frame in memory: {wide.memory_usage(deep=True).sum() / 1024**3:.2f} GB")
    log(f"curated columns: {len(wide.columns)}")
    if len(wide.columns) < 30:
        log(f"WARNING: only {len(wide.columns)} columns — S6 says 'one column out of 30'")

    share = wide["pickup_borough"].value_counts(normalize=True)
    log(f"dominant pickup_borough: {share.index[0]} = {share.iloc[0]:.1%}")
    if share.iloc[0] < 0.50:
        log("*** WARNING: dominant key is under 50%. S2 (skew) and S7 (straggler) both "
            "depend on one key dominating. Check your seed/zone files before teaching. ***")

    report = {"profile": args.profile, "dominant_key": str(share.index[0]),
              "dominant_key_share": round(float(share.iloc[0]), 4), "seed_constant": SEED,
              "curated_columns": len(wide.columns),
              "column_names": list(wide.columns)}

    # gc between stages. Each builder leaves large temporaries behind; without an
    # explicit collect the cumulative pressure segfaults the run at S8 even though
    # every stage passes in isolation.
    def rss_gb() -> float:
        try:
            with open("/proc/self/status") as fh:
                return int(fh.read().split("VmRSS:")[1].split()[0]) / 1024 ** 2
        except Exception:
            return float("nan")

    def stage(name: str, fn):
        log(name)
        result = fn()
        gc.collect()
        log(f"  RSS after stage: {rss_gb():.2f} GB")
        return result

    report["s1"] = stage("S1: oversized CSV",
                         lambda: build_s1(out, wide, p["s1_target_bytes"]))
    report["shared"] = stage("S2/S3/S4: shared slice",
                             lambda: build_slice(out, wide, p["slice_rows"]))
    report["s6"] = stage("S6: formats, partitions, small files",
                         lambda: build_s6(out, wide, p["s6_rows"], p["s6_small_files"]))
    report["s7"] = stage("S7: pathological job input",
                         lambda: build_s7(out, wide, zones, p["s7_rows"]))
    report["s8"] = stage("S8: dirty data",
                         lambda: build_s8(out, wide, p["s8_rows"]))

    report["expected_paths"] = [
        "zones/taxi_zone_lookup.csv", "s1/trips_big.csv",
        "shared/trips_slice.parquet", "shared/trips_slice.csv",
        "s6/trips.csv", "s6/trips.json", "s6/trips.parquet", "s6/trips_flat.parquet",
        "s6/trips_partitioned", "s6/trips_smallfiles",
        "s7/trips_skewed", "s7/zones.parquet",
        "s8/trips_dirty.parquet", "s8/defect_manifest.json",
    ]
    report["total_bytes"] = dir_size(out)
    report["total_human"] = human(report["total_bytes"])
    report["build_seconds"] = round(time.time() - t0, 1)
    report["fingerprint"] = sha256_head(out / "shared" / "trips_slice.parquet")

    manifest_path.write_text(json.dumps(report, indent=2, default=str))
    log(f"DONE in {report['build_seconds']}s — {report['total_human']} on disk")
    log(f"fingerprint {report['fingerprint']} (should match every other student's)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
