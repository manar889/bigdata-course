# RUNBOOK — from zero to a working course environment

Written assuming you have never used Docker. Follow in order. Do not skip step 2.

**Total time: about 3 hours, spread over 2 days.** Do not try to do it in one sitting
the night before you email the students.

---

## Mental model first (2 minutes, worth it)

Three words, and then everything below makes sense.

- **Image** — a frozen, complete computer: Linux + Python 3.11 + Java 17 + Spark +
  Jupyter, already installed and working. You build it once. It never changes.
- **Container** — a running copy of an image. Start it, stop it, delete it, start
  another. Deleting a container does not damage the image.
- **Volume** — a disk that survives when a container is deleted. Your 4.5 GB of TP
  data lives here, so students build it once and not every morning.

That's it. Everything else is syntax.

**One rule that will save you an hour:** every command below is run from the **project
root** — the folder containing `docker/`, `scripts/`, `notebooks/`. Not from inside
`docker/`. If a command fails with "no such file", check where you are: `pwd`.

---

## Day 1

### Step 1 — Put the project on disk

Unzip what I gave you somewhere sensible and rename the folder `bigdata-course`.

```bash
cd ~/bigdata-course
ls
# expected: DATASETS.md  README.md  RUNBOOK.md  docker  notebooks  scripts
```

Create two folders that do not exist yet but that the compose file expects:

```bash
mkdir -p solutions data-seed
```

If you skip `solutions`, Docker silently creates it as a root-owned folder on Linux and
you will not be able to write to it later.

Make the scripts executable (macOS/Linux only):

```bash
chmod +x scripts/*.sh docker/entrypoint.sh
```

---

### Step 2 — Install Docker and give it enough memory

**This is the step people get wrong, and the failure is invisible.** Docker Desktop
ships with a small default memory allocation. Our container asks for 4 GB. If Docker
only has 2 GB, the container is killed mid-exercise with a useless error message.

**macOS / Windows:** install Docker Desktop from https://www.docker.com/products/docker-desktop/
Then: **Settings → Resources** → set **Memory: 6 GB**, **CPUs: 4**. Apply & Restart.

**Windows also needs this.** WSL2 grabs host memory independently of the Docker
Desktop setting. Create the file `C:\Users\<YourName>\.wslconfig` with exactly:

```ini
[wsl2]
memory=6GB
processors=4
```

Then in PowerShell: `wsl --shutdown`, and restart Docker Desktop.

**Linux:** install `docker-ce` and `docker-compose-plugin` from
https://docs.docker.com/engine/install/ — no memory setting needed, it uses host RAM.
Then `sudo usermod -aG docker $USER` and log out/in so you can run docker without sudo.

Verify:

```bash
docker run --rm alpine sh -c "nproc; free -g | head -2"
```

You want to see **4 or more CPUs** and **5+ GB**. If you see 2 GB, go back and fix it.
Nothing below will work properly until this line is right.

> **Windows note:** run all the `bash` commands in this runbook from **Git Bash**, not
> PowerShell or CMD. Git Bash comes with Git for Windows. The `.sh` scripts will not
> run in PowerShell.

---

### Step 3 — Build the image (first real command)

```bash
cd ~/bigdata-course
docker compose -f docker/docker-compose.yml build
```

Note: `docker compose` with a **space**. The old `docker-compose` with a hyphen is a
different, older tool — if you have it, ignore it.

**This takes 5–10 minutes and downloads ~1.5 GB.** It will print a lot. Do not press
Ctrl-C because it looks stuck; downloading Spark is genuinely slow.

**Your success signal is this line near the end:**

```
BUILD CHECK OK — Spark 3.5.9
```

That line means Spark actually started and ran a job inside the image. I put that check
in the build on purpose: if Spark is broken, the build fails *now*, on your machine, not
at 09:00 in front of 30 students.

If the build fails, jump to Troubleshooting at the bottom.

---

### Step 4 — Smoke test with tiny data (2 minutes, do this before the big one)

Do not build 4.5 GB of data yet. Prove the plumbing works with a toy version first.

```bash
docker compose -f docker/docker-compose.yml run --rm \
  -e DATA_PROFILE=tiny lab prepare
```

`run --rm` means: start a container, do one job, delete the container. The data survives
in the volume.

Expected last lines:

```
[HH:MM:SS] DONE in 3.1s — 39.3 MB on disk
[HH:MM:SS] fingerprint 82be5321aec1f7ca
```

Now run the diagnostic against it:

```bash
docker compose -f docker/docker-compose.yml run --rm lab diagnostic
```

You should get a green **ALL CHECKS PASSED** box. If "CPU / memory limit" is red,
your step 2 is wrong — fix it before continuing.

---

### Step 5 — Build the real data (10 minutes)

Wipe the tiny data and build the real thing:

```bash
docker volume rm bigdata-course_course-data
docker compose -f docker/docker-compose.yml run --rm \
  -e DATA_PROFILE=full \
  -e SEED_URL=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet \
  lab prepare
```

This downloads 48 MB from NYC's CDN and generates ~4.5 GB locally. The slow part is
writing 10,000 tiny Parquet files for session 6 — that is supposed to be slow, it's the
pathology you are teaching.

Expected:

```
dominant pickup_borough: Manhattan = 89.x%
DONE in ~600s — 4.4 GB on disk
```

**If "dominant pickup_borough" is under 50%, stop and tell me.** Sessions 2 and 7 both
depend on one key dominating. That number is the health check for the whole course.

If the download fails (CDN down, no internet), add `--synthetic` — the script fabricates
schema-identical data with the same skew and every TP still works.

---

### Step 6 — Start it and look at it

```bash
docker compose -f docker/docker-compose.yml up
```

Leave this terminal running. Open a browser:

- **http://localhost:8888** — JupyterLab. No password, no token.
- Open `notebooks/00_diagnostic.ipynb`, run all cells, confirm the green box.
- Run the cell that starts a SparkSession, then open **http://localhost:4040** —
  that's the Spark UI. Click **Stages**. This is the screen session 7 lives in.
  **It only exists while a SparkSession is alive.** When you call `spark.stop()`, the
  page dies. That confuses everyone the first time.

To stop: `Ctrl-C` in that terminal, or `docker compose -f docker/docker-compose.yml down`
from another one. `down` deletes the container, **not** the data.

Day 1 ends here. You now have a working environment.

---

## Day 2

### Step 7 — Publish the image so students don't build it

30 students building the image = 30 downloads of 1.5 GB and 30 chances to fail. Build
once, push once, they pull a finished thing.

GitHub Container Registry is free and needs no extra account.

1. Create a **Personal Access Token (classic)** at
   https://github.com/settings/tokens with the scope `write:packages`.
2. Log in (replace `YOURNAME`, lowercase):

```bash
echo "YOUR_TOKEN" | docker login ghcr.io -u YOURNAME --password-stdin
```

3. Replace the placeholder in **three files** — search for `REPLACE-ME`:
   - `docker/docker-compose.yml`
   - `docker/docker-compose.student.yml`
   - `scripts/build_and_test.sh`

   Change `ghcr.io/REPLACE-ME/bigdata-course:1.0.0` → `ghcr.io/yourname/bigdata-course:1.0.0`

4. Also replace `REPLACE-ME` in `notebooks/00_colab_fallback.ipynb` (Step 2 cell,
   `REPO_RAW`) with your GitHub repo URL, or the Colab fallback cannot fetch the scripts.

5. Build for **both** chip architectures and push:

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -f docker/Dockerfile \
  -t ghcr.io/yourname/bigdata-course:1.0.0 --push .
```

**The `--platform` flag is not optional.** Students with Apple Silicon Macs (M1/M2/M3)
need the arm64 build. Without it their Mac runs the amd64 image under emulation —
silently, at about a third of the speed. Their session 1 timings then disagree with
everyone else's and they conclude their laptop is broken.

This build takes longer (it builds twice). 15–25 minutes is normal.

6. Make the package public: GitHub → your profile → Packages → `bigdata-course` →
   Package settings → Change visibility → Public. Otherwise students need a token.

7. Verify from a clean angle:

```bash
docker rmi ghcr.io/yourname/bigdata-course:1.0.0
docker compose -f docker/docker-compose.student.yml pull
```

---

### Step 8 — The three-OS sign-off

This is your checklist item "built and tested on Windows, macOS and Linux". You cannot
do it from one machine. Borrow two.

On each machine: install Docker (step 2), clone the repo, then:

```bash
./scripts/build_and_test.sh --quick
```

It checks Docker's memory allocation, builds, verifies Python/Java/Spark versions, runs
a real shuffle, builds the data, and — the one that matters — **confirms the Spark UI is
reachable on port 4040**. It prints one sign-off line:

```
Darwin / arm64 | docker 27.1.1 | 11 passed, 0 failed | 2026-09-20
```

Collect three lines. Paste them into your pre-semester checklist. That item is then
genuinely done, not assumed.

If you only have your own machine: run it on yours, and ask two students who reply early
to run it and send you the line. That is worth more than your own third test.

---

### Step 9 — What you send students, 10 days before S1

Put the repo on GitHub (public, no data files in it — the `.dockerignore` already keeps
data out). Then send one message:

> **Before our first session — 20 minutes, deadline [date, 48h before S1]**
>
> 1. Install Docker Desktop: https://www.docker.com/products/docker-desktop/
> 2. **Docker Desktop → Settings → Resources → Memory: 6 GB, CPUs: 4.** Apply & Restart.
>    *(Windows users, there is an extra step in the README — do it.)*
> 3. In a terminal:
>    ```
>    git clone https://github.com/yourname/bigdata-course
>    cd bigdata-course
>    mkdir -p solutions
>    docker compose -f docker/docker-compose.student.yml up
>    ```
>    First run takes ~15 minutes. It downloads the environment and builds your datasets.
>    Let it finish.
> 4. Open http://localhost:8888, open `notebooks/00_diagnostic.ipynb`, run all cells.
> 5. **Send me a screenshot of the box at the bottom** — green or red, send it either way.
>
> If it will not run on your machine, tell me and I will send you the Colab version.
> Do not stay silent. A problem 10 days early is a five-minute fix.

Then, **48 hours before S1**, list who has not sent a screenshot and **message them
privately, one by one**. Not a group announcement. The students who need chasing are
exactly the ones who ignore group announcements.

---

## Daily commands you will actually use

| What you want | Command (from project root) |
|---|---|
| Start | `docker compose -f docker/docker-compose.yml up` |
| Start in background | `docker compose -f docker/docker-compose.yml up -d` |
| Stop | `docker compose -f docker/docker-compose.yml down` |
| Shell inside the container | `docker compose -f docker/docker-compose.yml run --rm lab shell` |
| Rebuild after editing the Dockerfile | `docker compose -f docker/docker-compose.yml build` |
| Rebuild the data from scratch | `docker volume rm bigdata-course_course-data` then step 5 |
| See what's running | `docker ps` |
| Reclaim disk space | `docker system prune` (does **not** touch named volumes) |

---

## Troubleshooting

**"docker: command not found"** — Docker Desktop is not installed, or not running. On
macOS/Windows the whale icon must be in the menu bar/tray.

**"Cannot connect to the Docker daemon"** — Docker Desktop is installed but not started.
Start it and wait for the whale to stop animating.

**"port is already allocated"** — something else uses 8888 (often a local Jupyter).
Either close it, or edit the compose file: `"8889:8888"` and use localhost:8889.
**Never change the `4040:4040` line** — session 7 needs that exact port.

**Build fails downloading packages** — usually transient network. Re-run the build; Docker
caches completed steps, so it resumes rather than restarting.

**Container starts then immediately exits** — on Windows, usually CRLF line endings in
`entrypoint.sh`. The `.gitattributes` I included prevents this, but if you edited the file
in Notepad, re-save it with LF endings (VS Code: bottom-right corner, click `CRLF` → `LF`).

**"exec format error"** — same CRLF issue, or you pulled an image built for the wrong
architecture. Rebuild with `--platform`.

**JupyterLab loads but Spark cells hang** — almost always memory. Re-run step 2's verify
command. 2 GB looks like a hang, not an error.

**localhost:4040 shows nothing** — there is no live SparkSession. Run a Spark cell first.
The UI exists only while a session is alive.

**Everything is slow on a Mac** — you are running the amd64 image under emulation. Check
with `docker image inspect ghcr.io/yourname/bigdata-course:1.0.0 | grep Architecture`.
It should say `arm64` on Apple Silicon.

**Nuclear option, safe:**

```bash
docker compose -f docker/docker-compose.yml down -v   # -v also deletes the data volume
docker compose -f docker/docker-compose.yml build --no-cache
```

Then redo steps 4 and 5. You lose 20 minutes, not your work — your notebooks live in the
`notebooks/` folder on your real disk, not inside the container.

---

## Two things to do before you teach

1. **Break it on purpose, once.** Set `mem_limit: 2g` in the compose file, restart, and
   run the session 1 notebook. Watch what the failure actually looks like. When a student
   describes that exact symptom in week one, you will recognise it in ten seconds instead
   of debugging live in front of everyone.

2. **Run the session 7 exercise yourself, end to end, before session 4.** It is the
   highest-value session in your course and the one that depends on the most moving parts
   (Spark UI reachable, skew present, event logs writing). If it is broken, you want to
   know in week 4, not week 7.
