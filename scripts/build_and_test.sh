#!/usr/bin/env bash
# =============================================================================
# build_and_test.sh — build the image and prove it works, per OS.
#
# "Tested on Windows, macOS and Linux" is a claim only you can make, on three
# actual machines. This script is what you run on each one. It prints a
# sign-off line; paste all three into the pre-semester checklist.
#
#   ./scripts/build_and_test.sh            # build + full test
#   ./scripts/build_and_test.sh --push     # also push multi-arch to a registry
#   ./scripts/build_and_test.sh --quick    # skip the data build (tiny profile)
# =============================================================================
set -uo pipefail

IMAGE="${IMAGE:-ghcr.io/manar889/bigdata-course:1.1.0}"
PROFILE="full"
PUSH=0
for a in "$@"; do
  [ "$a" = "--push" ]  && PUSH=1
  [ "$a" = "--quick" ] && PROFILE="tiny"
done

PASS=0; FAIL=0
ok()   { printf '  \033[92m[PASS]\033[0m %s\n' "$1"; PASS=$((PASS+1)); }
bad()  { printf '  \033[91m[FAIL]\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }
note() { printf '\n\033[1m%s\033[0m\n' "$1"; }

cd "$(dirname "$0")/.." || exit 1

note "0. Host"
OS="$(uname -s)"; ARCH="$(uname -m)"
echo "  $OS / $ARCH"
[ -n "${WSL_DISTRO_NAME:-}" ] && echo "  WSL2: ${WSL_DISTRO_NAME}"
docker version --format '  docker {{.Server.Version}}' 2>/dev/null || { bad "docker not running"; exit 1; }

note "1. Docker resource allocation"
# THE most common Windows/macOS failure: Docker Desktop is given 2 GB, the
# compose file asks for 4 GB, and the container is OOM-killed mid-TP with no
# useful error. Check before building, not after.
TOTAL_KB=$(docker run --rm alpine sh -c 'free -k 2>/dev/null | awk "/Mem:/{print \$2}"' 2>/dev/null || echo 0)
TOTAL_GB=$(( TOTAL_KB / 1024 / 1024 ))
CPUS=$(docker run --rm alpine nproc 2>/dev/null || echo 0)
echo "  Docker sees ${TOTAL_GB} GB / ${CPUS} CPU"
if [ "$TOTAL_GB" -ge 5 ] && [ "$CPUS" -ge 4 ]; then
  ok "enough headroom for a 4 GB / 4 CPU container"
else
  bad "Docker has ${TOTAL_GB} GB / ${CPUS} CPU — need >=5 GB and >=4 CPU"
  echo "     macOS/Windows: Docker Desktop -> Settings -> Resources"
  echo "     WSL2: create C:\\Users\\<you>\\.wslconfig with:"
  echo "       [wsl2]"
  echo "       memory=6GB"
  echo "       processors=4"
  echo "     then: wsl --shutdown"
fi

note "2. Build"
if [ "$PUSH" = "1" ]; then
  docker buildx build --platform linux/amd64,linux/arm64 \
    -f docker/Dockerfile -t "$IMAGE" --push . && ok "multi-arch build+push" || bad "build"
else
  docker build -f docker/Dockerfile -t "$IMAGE" . && ok "build" || bad "build"
fi

note "3. Inside-container checks"
run() { docker run --rm --memory=4g --cpus=4 "$IMAGE" bash -lc "$1" 2>&1; }
run 'python -c "import sys;print(sys.version)"'          | grep -q "3.11" && ok "Python 3.11"  || bad "Python 3.11"
run 'java -version'                                      | grep -q "17\." && ok "Java 17"      || bad "Java 17"
run 'python -c "import pyspark;print(pyspark.__version__)"' | grep -q "3.5" && ok "PySpark 3.5" || bad "PySpark 3.5"
run 'python -c "from pyspark.sql import SparkSession as S;s=S.builder.master(\"local[2]\").getOrCreate();print(s.range(100).selectExpr(\"id%%7 k\").groupBy(\"k\").count().count())"' \
    | grep -q "^7$" && ok "real shuffle executes" || bad "real shuffle executes"
run 'id -u' | grep -q "^1000$" && ok "runs as uid 1000 (not root)" || bad "non-root uid"

note "4. Data build (profile=$PROFILE)"
docker volume rm -f bigdata-test-data >/dev/null 2>&1
docker run --rm --memory=4g --cpus=4 -v bigdata-test-data:/data \
  -e DATA_PROFILE="$PROFILE" "$IMAGE" prepare >/tmp/prep.log 2>&1 \
  && ok "prepare completed" || { bad "prepare failed"; tail -20 /tmp/prep.log; }
SKEW=$(docker run --rm -v bigdata-test-data:/data "$IMAGE" \
  python -c "import json;print(json.load(open('/data/MANIFEST.json'))['dominant_key_share'])" 2>/dev/null)
awk -v s="${SKEW:-0}" 'BEGIN{exit !(s>=0.5)}' && ok "skew present (${SKEW})" \
  || bad "skew missing (${SKEW}) — S2 and S7 have no lesson"

note "5. Ports (the S7 killer)"
docker rm -f bigdata-test >/dev/null 2>&1
docker run -d --name bigdata-test --memory=4g --cpus=4 \
  -p 18888:8888 -p 14040:4040 -v bigdata-test-data:/data "$IMAGE" lab >/dev/null
for i in $(seq 1 60); do curl -fs http://localhost:18888/api >/dev/null 2>&1 && break; sleep 2; done
curl -fs http://localhost:18888/api >/dev/null 2>&1 && ok "JupyterLab reachable" || bad "JupyterLab reachable"
docker exec -d bigdata-test python -c "
from pyspark.sql import SparkSession; import time
s=SparkSession.builder.master('local[2]').appName('uitest').getOrCreate()
s.range(2000000).selectExpr('id%997 k').groupBy('k').count().count(); time.sleep(45)"
sleep 25
curl -fs http://localhost:14040 >/dev/null 2>&1 && ok "Spark UI reachable on 4040" \
  || bad "Spark UI NOT reachable — S7 cannot be taught"
docker rm -f bigdata-test >/dev/null 2>&1

note "SIGN-OFF  (paste this into the pre-semester checklist)"
printf '  %s / %s | docker %s | %d passed, %d failed | %s\n' \
  "$OS" "$ARCH" "$(docker version --format '{{.Server.Version}}')" "$PASS" "$FAIL" "$(date -u +%Y-%m-%d)"
[ "$FAIL" -eq 0 ] || exit 1
