# Course environment — Distributed Systems → Big Data

```
docker/      Dockerfile, compose, Spark + Jupyter config
scripts/     prepare_tp_data.py, diagnostic.py, spark_evidence.py, build_and_test.sh
notebooks/   00_diagnostic.ipynb, 00_colab_fallback.ipynb
DATASETS.md  which dataset for which TP, and why
```

## Testing status — read this first

| Item | State |
|---|---|
| `prepare_tp_data.py` | **Executed end-to-end.** Generates all 14 artifacts, 38 columns, 90.6% Manhattan skew, 31 day-partitions, defect counts verified against the written file. |
| `spark_evidence.py` | **Executed** against a synthetic event log containing a known straggler. Correctly reports 20.4× skew ratio and the one-task-far-right histogram. |
| Colab memory-cap helper | **Executed** both ways: completes under a generous cap, raises a clean `MemoryError` under a tight one. No kernel kill. |
| Notebooks | Valid `nbformat` v4; every code cell parses as Python. |
| `docker-compose.yml` | Valid YAML; port 4040 and the 4 GB / 4 CPU limits asserted. |
| Shell scripts | `bash -n` clean. |
| **Docker image build** | **NOT BUILT.** No Docker daemon was available. |
| **Windows / macOS / Linux** | **NOT TESTED.** Nobody can test three OSes from one Linux container. |

So: the Dockerfile is written against pins I verified exist on PyPI (PySpark 3.5.9,
Python 3.11, Java 17), and the build has a `RUN` step that fails the build if Spark
cannot start a session. But **your checklist item "built and tested on Windows, macOS
and Linux" is not done until you run `scripts/build_and_test.sh` on three machines.**
That script exists precisely so it is 10 minutes per machine instead of an afternoon.

```bash
chmod +x scripts/build_and_test.sh
./scripts/build_and_test.sh            # full: build, resources, shuffle, data, ports
./scripts/build_and_test.sh --quick    # skip the ~8 min data build
./scripts/build_and_test.sh --push     # multi-arch (amd64 + arm64) to your registry
```

It prints a sign-off line per machine. Three lines, paste them into the checklist.

## Quick start (what students get)

```bash
git clone <your repo> && cd bigdata-course
docker compose -f docker/docker-compose.yml up
# first run: ~10 min (builds ~4.5 GB of TP data, once)
# JupyterLab  -> http://localhost:8888   (no token)
# Spark UI    -> http://localhost:4040   (only while a SparkSession is alive)
```

Then open `notebooks/00_diagnostic.ipynb` and send the screenshot.

## Per-OS traps, ranked by how often they will bite you

**1. Docker Desktop memory (Windows + macOS).** Default allocation is often 2 GB.
The compose file asks for 4. The container is OOM-killed mid-TP with no useful error.
`build_and_test.sh` checks this before it builds anything.

**2. WSL2 memory (Windows).** WSL2 grabs a fraction of host RAM regardless of Docker
Desktop settings. Student creates `C:\Users\<name>\.wslconfig`:
```ini
[wsl2]
memory=6GB
processors=4
```
then `wsl --shutdown`. Put this in the pre-S1 email, not in the S1 chat.

**3. Apple Silicon (arm64).** The image must be built multi-arch or every Mac student
runs it under emulation — silently, at roughly a third of the speed. Their S1 timings
then disagree with everyone's and they conclude their laptop is broken. `--push` builds
both architectures.

**4. File ownership (Linux).** The container runs as UID 1000. If the student's UID
differs, bind-mounted notebooks become unwritable. Fix:
`docker compose build --build-arg USER_UID=$(id -u) --build-arg USER_GID=$(id -g)`

**5. Port 4040.** If it is not published, S7 does not happen. It is in the compose file
and `build_and_test.sh` asserts it. Do not let anyone "simplify" the ports list.

**6. Line endings (Windows).** `entrypoint.sh` with CRLF fails as
`exec format error`. Commit a `.gitattributes`:
```
*.sh text eol=lf
```

## Why these version pins

| Pin | Reason |
|---|---|
| Python 3.11 | PySpark 3.5.x does not support 3.12. Bump Python and Spark stops importing. |
| Java 17 | Spark 3.5 is certified on 8/11/17. |
| PySpark 3.5.9 | Matches *Learning Spark, 2nd Ed.* — your S4/S6/S9 reading. Spark 4.x changes `explain()` output and UI labels enough to invalidate your S7 screenshots mid-semester. Do not upgrade during a live course. |
| `spark.sql.shuffle.partitions = 16` | Default 200 produces 200 tiny tasks and a UI that teaches nothing. 16 makes a straggler one obviously-long bar. |
| `spark.sql.adaptive.enabled = false` | **Deliberate.** AQE auto-fixes skew. Leave it on and the skew you built S2 around is gone before students see it. They switch it on themselves in S7 and watch it work — that's the lesson. |
| `mem_limit: 4g`, `cpus: 4` | S1 is a *measurement* exercise. Unpinned, a 32 GB laptop and an 8 GB laptop produce different answers and the debrief comparison collapses. |

## Where this design departs from the course architecture

Three places, each with a reason:

1. **Datasets are generated, not downloaded.** Students pull ~60 MB and build ~4.5 GB
   locally. See `DATASETS.md`.
2. **The container is resource-pinned.** Your S1 TP asks students to measure time and
   memory. That is only comparable if the box is the same size for everyone.
3. **The data build is folded into the diagnostic.** Your checklist has "diagnostic
   exercise" and "datasets hosted" as separate items. Merging them means students chase
   one thing, and a student who skipped the data build cannot produce a green box.
