#!/usr/bin/env python3
"""
spark_evidence.py — turn Spark event logs into the S7 before/after table.

Two jobs:
  1. Colab students have NO Spark UI. S7 ("open the UI, find the straggler") is
     otherwise impossible for them. This reads the same numbers off the event log.
  2. Docker students need before/after evidence in the project appendix. A UI
     screenshot is fine; a reproducible table is better, and this generates it.

    from spark_evidence import stage_report, skew_report
    stage_report()                 # every stage: tasks, max vs median, shuffle bytes
    skew_report(stage_id=3)        # task duration distribution for one stage
"""
from __future__ import annotations

import glob
import json
import os
import statistics
from pathlib import Path

DEFAULT_DIR = os.environ.get("SPARK_EVENT_DIR", "/tmp/spark-events")


def _latest_log(event_dir: str | Path = DEFAULT_DIR) -> Path:
    files = [Path(p) for p in glob.glob(str(Path(event_dir) / "*"))
             if Path(p).is_file() and not str(p).endswith(".crc")]
    if not files:
        raise FileNotFoundError(
            f"No event logs in {event_dir}. Start Spark with:\n"
            f"  .config('spark.eventLog.enabled','true')\n"
            f"  .config('spark.eventLog.dir','file://{event_dir}')")
    return max(files, key=lambda p: p.stat().st_mtime)


def _events(path: Path):
    with open(path, "r", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue  # in-progress logs can end mid-line


def _collect(event_dir=DEFAULT_DIR):
    path = _latest_log(event_dir)
    stages: dict[int, dict] = {}
    for ev in _events(path):
        kind = ev.get("Event", "")

        if kind == "SparkListenerStageCompleted":
            info = ev.get("Stage Info", {})
            sid = info.get("Stage ID")
            stages.setdefault(sid, {}).update(
                name=(info.get("Stage Name") or "").split(" at ")[0],
                details=(info.get("Details") or "").splitlines()[0][:80],
            )

        elif kind == "SparkListenerTaskEnd":
            sid = ev.get("Stage ID")
            st = stages.setdefault(sid, {})
            st.setdefault("durations", [])
            st.setdefault("shuffle_read", 0)
            st.setdefault("shuffle_write", 0)
            st.setdefault("spill_disk", 0)
            st.setdefault("records_read", 0)

            ti = ev.get("Task Info", {}) or {}
            launch, finish = ti.get("Launch Time", 0), ti.get("Finish Time", 0)
            if finish and launch and finish >= launch:
                st["durations"].append((finish - launch) / 1000.0)

            tm = ev.get("Task Metrics", {}) or {}
            sr = tm.get("Shuffle Read Metrics", {}) or {}
            sw = tm.get("Shuffle Write Metrics", {}) or {}
            st["shuffle_read"] += (sr.get("Remote Bytes Read", 0) or 0) + \
                                  (sr.get("Local Bytes Read", 0) or 0)
            st["shuffle_write"] += sw.get("Shuffle Bytes Written", 0) or 0
            st["spill_disk"] += tm.get("Disk Bytes Spilled", 0) or 0
            st["records_read"] += (tm.get("Input Metrics", {}) or {}).get("Records Read", 0) or 0
    return path, stages


def _mb(n: float) -> str:
    return f"{n / 1024**2:,.1f} MB" if n else "-"


def stage_report(event_dir=DEFAULT_DIR, as_dict: bool = False):
    """
    Per stage: task count, median task time, max task time, skew ratio, shuffle bytes.

    skew_ratio = max task duration / median task duration.
    Anything above ~3 is the straggler you are looking for in S7.
    """
    path, stages = _collect(event_dir)
    rows = []
    for sid in sorted(k for k in stages if k is not None):
        st = stages[sid]
        d = st.get("durations") or []
        if not d:
            continue
        med = statistics.median(d)
        mx = max(d)
        rows.append({
            "stage": sid,
            "name": st.get("name", "?"),
            "tasks": len(d),
            "median_s": round(med, 2),
            "max_s": round(mx, 2),
            "total_s": round(sum(d), 1),
            "skew_ratio": round(mx / med, 1) if med > 0 else None,
            "shuffle_read": st.get("shuffle_read", 0),
            "shuffle_write": st.get("shuffle_write", 0),
            "spill_disk": st.get("spill_disk", 0),
        })

    if as_dict:
        return rows

    print(f"event log: {path.name}")
    print(f"{'stg':>3} {'tasks':>6} {'median':>8} {'max':>8} {'skew':>6} "
          f"{'shuf read':>12} {'shuf write':>12} {'spill':>10}  name")
    print("-" * 104)
    for r in rows:
        flag = "  <-- STRAGGLER" if (r["skew_ratio"] or 0) >= 3 else ""
        print(f"{r['stage']:>3} {r['tasks']:>6} {r['median_s']:>7.2f}s {r['max_s']:>7.2f}s "
              f"{str(r['skew_ratio']) + 'x':>6} {_mb(r['shuffle_read']):>12} "
              f"{_mb(r['shuffle_write']):>12} {_mb(r['spill_disk']):>10}  {r['name']}{flag}")
    tot = sum(r["shuffle_write"] for r in rows)
    print("-" * 104)
    print(f"total shuffle written: {_mb(tot)}   "
          f"(this is the number your optimization has to move)")
    return rows


def skew_report(stage_id: int, event_dir=DEFAULT_DIR, buckets: int = 10):
    """Task-duration histogram for one stage. The text version of the UI's task table."""
    _, stages = _collect(event_dir)
    d = sorted((stages.get(stage_id) or {}).get("durations") or [])
    if not d:
        print(f"stage {stage_id}: no task data")
        return
    lo, hi = d[0], d[-1]
    width = (hi - lo) / buckets or 1
    print(f"stage {stage_id}: {len(d)} tasks, {lo:.2f}s .. {hi:.2f}s, "
          f"median {statistics.median(d):.2f}s")
    for i in range(buckets):
        a, b = lo + i * width, lo + (i + 1) * width
        n = sum(1 for x in d if (a <= x < b or (i == buckets - 1 and x == hi)))
        print(f"  {a:6.2f}-{b:6.2f}s | {'#' * min(n, 60)}{'' if n <= 60 else '...'} {n}")
    print("\nOne bar far to the right with count 1 = your straggler. "
          "That task is holding the whole stage.")


def before_after(label: str, seconds: float, event_dir=DEFAULT_DIR, _store: list = []):
    """
    Record one run for the S7 deliverable ("a filled 4-row before/after table").
    Call after each variant; print the accumulated table at the end.
    """
    rows = stage_report(event_dir, as_dict=True)
    _store.append({
        "variant": label,
        "wall_s": round(seconds, 2),
        "stages": len(rows),
        "max_skew": max((r["skew_ratio"] or 0) for r in rows) if rows else 0,
        "shuffle_write_mb": round(sum(r["shuffle_write"] for r in rows) / 1024**2, 1),
    })
    print(f"\n{'variant':<28} {'wall':>8} {'stages':>7} {'max skew':>9} {'shuffle MB':>11}")
    print("-" * 68)
    for r in _store:
        print(f"{r['variant']:<28} {r['wall_s']:>7.2f}s {r['stages']:>7} "
              f"{r['max_skew']:>8}x {r['shuffle_write_mb']:>11}")
    return _store


if __name__ == "__main__":
    stage_report()
