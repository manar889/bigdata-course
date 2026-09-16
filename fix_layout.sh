#!/usr/bin/env bash
# =============================================================================
# fix_layout.sh — repair a flattened project folder.
#
# If you downloaded the files one by one, they all landed at the root and the
# Dockerfile's COPY paths (docker/..., scripts/..., notebooks/...) cannot find
# anything. This moves every file where it belongs and then verifies.
#
#   bash fix_layout.sh            # check only, changes nothing
#   bash fix_layout.sh --apply    # actually move the files
# =============================================================================
set -uo pipefail
APPLY=0; [ "${1:-}" = "--apply" ] && APPLY=1

declare -A DEST=(
  [Dockerfile]=docker
  [docker-compose.yml]=docker
  [docker-compose.student.yml]=docker
  [entrypoint.sh]=docker
  [jupyter_server_config.py]=docker
  [requirements.txt]=docker
  [spark-defaults.conf]=docker
  [.dockerignore]=docker
  [prepare_tp_data.py]=scripts
  [diagnostic.py]=scripts
  [spark_evidence.py]=scripts
  [build_and_test.sh]=scripts
  [00_diagnostic.ipynb]=notebooks
  [00_colab_fallback.ipynb]=notebooks
  [README.md]=.
  [RUNBOOK.md]=.
  [DATASETS.md]=.
  [.gitattributes]=.
)

moved=0
for f in "${!DEST[@]}"; do
  d="${DEST[$f]}"
  [ "$d" = "." ] && continue
  if [ -f "$f" ] && [ ! -f "$d/$f" ]; then
    if [ "$APPLY" = "1" ]; then
      mkdir -p "$d" && mv "$f" "$d/$f" && echo "  moved  $f -> $d/"
    else
      echo "  WOULD MOVE  $f -> $d/"
    fi
    moved=$((moved+1))
  fi
done

if [ "$APPLY" = "1" ]; then
  mkdir -p docker scripts notebooks solutions data-seed
  touch solutions/.gitkeep
  chmod +x scripts/*.sh docker/entrypoint.sh 2>/dev/null
fi

echo
echo "Verifying required layout:"
REQ=(
  docker/Dockerfile docker/docker-compose.yml docker/docker-compose.student.yml
  docker/entrypoint.sh docker/jupyter_server_config.py docker/requirements.txt
  docker/spark-defaults.conf
  scripts/prepare_tp_data.py scripts/diagnostic.py scripts/spark_evidence.py
  scripts/build_and_test.sh
  notebooks/00_diagnostic.ipynb notebooks/00_colab_fallback.ipynb
  solutions
)
missing=0
for p in "${REQ[@]}"; do
  if [ -e "$p" ]; then printf '  \033[92mOK  \033[0m %s\n' "$p"
  else printf '  \033[91mMISS\033[0m %s\n' "$p"; missing=$((missing+1)); fi
done

echo
if [ "$missing" -eq 0 ]; then
  echo "Layout is correct. Build from THIS folder (not from docker/):"
  echo "  docker compose -f docker/docker-compose.yml build"
elif [ "$APPLY" = "0" ] && [ "$moved" -gt 0 ]; then
  echo "Run again with --apply to move the files."
else
  echo "$missing file(s) genuinely missing — re-download the archive."
  exit 1
fi
