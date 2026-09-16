#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${DATA_DIR:-/data}"
SEED_URL="${SEED_URL:-}"          # set in docker-compose.yml
ZONES_URL="${ZONES_URL:-}"
PROFILE="${DATA_PROFILE:-full}"

banner() { printf '\n\033[1m%s\033[0m\n' "$*"; }

fetch() {  # fetch <url> <dest>   — resumable, fails loudly
  local url="$1" dest="$2"
  [ -s "$dest" ] && { echo "  already have $(basename "$dest")"; return 0; }
  echo "  downloading $(basename "$dest") ..."
  curl -fL --retry 5 --retry-delay 3 -C - -o "$dest.part" "$url" \
    && mv "$dest.part" "$dest"
}

prepare_data() {
  mkdir -p "$DATA_DIR/seed" "$DATA_DIR/zones"
  if [ -f "$DATA_DIR/MANIFEST.json" ]; then
    echo "  data already built (MANIFEST.json present)"
    return 0
  fi

  local seed="$DATA_DIR/seed/yellow_seed.parquet"
  local zones="$DATA_DIR/zones/taxi_zone_lookup.csv"
  local synth=""

  if [ -n "$SEED_URL" ]; then
    fetch "$SEED_URL"  "$seed"  || synth="--synthetic"
    fetch "$ZONES_URL" "$zones" || true
  else
    echo "  no SEED_URL set -> building from synthetic data"
    synth="--synthetic"
  fi
  [ -s "$seed" ] || synth="--synthetic"

  banner "Building TP datasets (first run only — 6-12 min, then never again)"
  rc=0
  python /opt/course/prepare_tp_data.py \
      --out "$DATA_DIR" --profile "$PROFILE" \
      ${seed:+--seed-file "$seed"} ${zones:+--zones-file "$zones"} $synth || rc=$?
  if [ "$rc" -ne 0 ]; then
    echo
    echo "############################################################"
    echo "#  DATA BUILD FAILED (exit $rc)"
    if [ "$rc" -eq 137 ]; then
      echo "#  Exit 137 = OUT OF MEMORY. Docker killed it."
      echo "#  Give Docker Desktop more memory (Settings -> Resources),"
      echo "#  or rebuild with a smaller profile:"
      echo "#    docker compose -f docker/docker-compose.yml run --rm \\"
      echo "#      -e DATA_PROFILE=small lab prepare"
    fi
    echo "#  JupyterLab will NOT start until this succeeds."
    echo "############################################################"
    exit "$rc"
  fi
}

case "${1:-lab}" in
  lab)
    prepare_data
    banner "JupyterLab -> http://localhost:8888   (no token)"
    echo   "Spark UI   -> http://localhost:4040   (only while a SparkSession is alive)"
    exec jupyter lab --config=/etc/jupyter/jupyter_server_config.py
    ;;
  prepare)  prepare_data ;;
  diagnostic)
    prepare_data
    exec python /opt/course/diagnostic.py
    ;;
  shell)    exec /bin/bash ;;
  *)        exec "$@" ;;
esac
