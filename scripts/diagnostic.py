#!/usr/bin/env python3
"""
diagnostic.py — the mandatory pre-S1 check.

Prints ONE line the student pastes into the shared sheet, and one banner they
screenshot. Checks everything that has ever broken an online Spark course:
Java, Spark session start, a real shuffle, the data build, the Spark UI port,
and the CPU/RAM the container actually got (not what the laptop has).

    python diagnostic.py --name "Nom Prenom"
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import subprocess
import sys
import time
from pathlib import Path

GREEN, RED, YELL, BOLD, OFF = "\033[92m", "\033[91m", "\033[93m", "\033[1m", "\033[0m"
results: list[tuple[str, bool, str]] = []


def check(label: str, fn):
    try:
        ok, detail = fn()
    except Exception as e:  # noqa: BLE001
        ok, detail = False, f"{type(e).__name__}: {e}"
    results.append((label, ok, detail))
    mark = f"{GREEN}PASS{OFF}" if ok else f"{RED}FAIL{OFF}"
    print(f"  [{mark}] {label:<28} {detail}")
    return ok


def c_python():
    v = sys.version_info
    ok = (v.major, v.minor) == (3, 11)
    return ok, f"{platform.python_version()}" + ("" if ok else "  <-- PySpark 3.5 needs 3.11")


def c_java():
    out = subprocess.run(["java", "-version"], capture_output=True, text=True).stderr
    line = out.splitlines()[0] if out else "not found"
    ok = '"17' in line or "17." in line
    return ok, line.strip() + ("" if ok else "  <-- Spark 3.5 wants Java 17")


def c_resources():
    import psutil
    cpus = os.cpu_count()
    # cgroup v2 limit = what the CONTAINER got, which is what matters.
    limit = Path("/sys/fs/cgroup/memory.max")
    if limit.exists():
        raw = limit.read_text().strip()
        mem_gb = float("inf") if raw == "max" else int(raw) / 1024**3
    else:
        mem_gb = psutil.virtual_memory().total / 1024**3
    ok = cpus is not None and cpus >= 4 and 3.0 <= mem_gb <= 6.0
    warn = "" if ok else f"  <-- expected 4 CPU / 4 GB; measurements will not match the class"
    return ok, f"{cpus} CPU, {mem_gb:.1f} GB{warn}"


def c_spark():
    from pyspark.sql import SparkSession
    t0 = time.time()
    s = SparkSession.builder.master("local[*]").appName("diagnostic").getOrCreate()
    # a real shuffle, not just a count: this is what actually fails on broken installs
    n = s.range(200_000).selectExpr("id % 97 as k").groupBy("k").count().count()
    globals()["_spark"] = s
    return n == 97, f"Spark {s.version}, shuffle OK in {time.time() - t0:.1f}s"


def c_spark_ui():
    s = globals().get("_spark")
    if s is None:
        return False, "no session"
    url = s.sparkContext.uiWebUrl or ""
    with socket.socket() as sk:
        sk.settimeout(2)
        reachable = sk.connect_ex(("127.0.0.1", 4040)) == 0
    return reachable, (f"{url}  -> open http://localhost:4040 in your browser"
                       if reachable else "port 4040 not listening (is it published?)")


def c_duckdb():
    import duckdb
    v = duckdb.sql("select 42").fetchone()[0]
    return v == 42, f"duckdb {duckdb.__version__}"


def c_data():
    d = Path(os.environ.get("DATA_DIR", "/data"))
    mf = d / "MANIFEST.json"
    if not mf.exists():
        return False, f"no MANIFEST.json under {d} — run `docker compose run lab prepare`"
    m = json.loads(mf.read_text())
    missing = [p for p in m["expected_paths"] if not (d / p).exists()]
    return not missing, (f"profile={m['profile']} {m['total_human']} "
                         f"fingerprint={m['fingerprint']}" if not missing
                         else f"missing {missing[:3]}")


def c_skew():
    """The one content check: if the dominant key is not dominant, S2 and S7 have no lesson."""
    d = Path(os.environ.get("DATA_DIR", "/data"))
    mf = d / "MANIFEST.json"
    if not mf.exists():
        return False, "no manifest"
    m = json.loads(mf.read_text())
    share = m.get("dominant_key_share", 0)
    return share >= 0.5, f"{m.get('dominant_key')} = {share:.1%} of rows"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", default=os.environ.get("STUDENT_NAME", "UNNAMED"))
    args = ap.parse_args()

    print(f"\n{BOLD}Environment diagnostic — {args.name}{OFF}")
    print(f"  host: {platform.platform()}\n")

    check("Python version", c_python)
    check("Java version", c_java)
    check("CPU / memory limit", c_resources)
    check("Spark session + shuffle", c_spark)
    check("Spark UI reachable", c_spark_ui)
    check("DuckDB", c_duckdb)
    check("TP data built", c_data)
    check("Skew present in data", c_skew)

    s = globals().get("_spark")
    if s is not None:
        s.stop()

    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    line = (f"{args.name} | {passed}/{total} | py{platform.python_version()} | "
            f"{os.cpu_count()}cpu | {platform.system()}")

    if passed == total:
        print(f"\n{GREEN}{BOLD}{'='*62}{OFF}")
        print(f"{GREEN}{BOLD}  ALL CHECKS PASSED — screenshot this box and send it{OFF}")
        print(f"{GREEN}{BOLD}  {line}{OFF}")
        print(f"{GREEN}{BOLD}{'='*62}{OFF}\n")
        return 0

    print(f"\n{RED}{BOLD}{'='*62}{OFF}")
    print(f"{RED}{BOLD}  {total - passed} CHECK(S) FAILED — send this screenshot NOW,{OFF}")
    print(f"{RED}{BOLD}  not the night before session 1.{OFF}")
    print(f"{RED}{BOLD}  {line}{OFF}")
    print(f"{RED}{BOLD}{'='*62}{OFF}\n")
    for label, ok, detail in results:
        if not ok:
            print(f"  {YELL}{label}{OFF}: {detail}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
