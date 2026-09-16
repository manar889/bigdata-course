#!/usr/bin/env bash
# =============================================================================
# fetch_pyspark.sh — download the PySpark tarball OUTSIDE Docker, resumably.
#
# THE PROBLEM THIS SOLVES
#   PySpark publishes no wheel. pip must fetch a 303 MB .tar.gz, and pip cannot
#   resume a broken download — one dropped connection and it starts from zero,
#   then fails a hash check. On a slow or unstable link that can be unwinnable.
#
#   curl -C - RESUMES. Run this as many times as you need; each run continues
#   where the last one stopped. Then Docker installs from the local file and
#   downloads nothing.
#
#   Run it, let it fail, run it again. It will finish.
#
#   bash scripts/fetch_pyspark.sh
# =============================================================================
set -uo pipefail

VERSION="${PYSPARK_VERSION:-3.5.9}"
URL="https://files.pythonhosted.org/packages/95/ce/81e53e729790556e3983e95de1a7d5df91a34adfcd34b5a5ab0e0c6e9b33/pyspark-3.5.9.tar.gz"
SHA256="ea27adc39ddac9413b8951e45aa748cbed6c785971b81386efc41938f6243d93"

cd "$(dirname "$0")/.." || exit 1
mkdir -p docker/vendor
DEST="docker/vendor/pyspark-${VERSION}.tar.gz"

sha_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum    >/dev/null 2>&1; then shasum -a 256 "$1" | cut -d' ' -f1
  else python3 -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$1"
  fi
}

if [ -f "$DEST" ] && [ "$(sha_of "$DEST")" = "$SHA256" ]; then
  echo "Already have a verified pyspark-${VERSION}.tar.gz — nothing to do."
  exit 0
fi

echo "Downloading pyspark ${VERSION} (303 MB, resumable)."
echo "If it stops, just run this script again. Progress is kept."
echo

# -C -  resume from wherever the file currently ends
# --retry / --retry-all-errors  keep trying without you watching
# --speed-time/--speed-limit    give up on a stalled socket instead of hanging
curl -L -C - \
     --retry 20 --retry-delay 5 --retry-all-errors \
     --speed-time 60 --speed-limit 1024 \
     --progress-bar \
     -o "$DEST" "$URL"
RC=$?

if [ ! -f "$DEST" ]; then
  echo "Download produced no file. Check your connection and re-run."
  exit 1
fi

GOT="$(sha_of "$DEST")"
SIZE=$(( $(wc -c < "$DEST") / 1024 / 1024 ))

if [ "$GOT" = "$SHA256" ]; then
  echo
  echo "VERIFIED — ${SIZE} MB, sha256 matches."
  echo "Now build. Docker will install from this file and download nothing:"
  echo "  docker compose -f docker/docker-compose.yml build"
  exit 0
fi

echo
echo "INCOMPLETE — ${SIZE} MB so far (need 303 MB), curl exit ${RC}."
echo "This is normal on an unstable link. Run this script again to continue:"
echo "  bash scripts/fetch_pyspark.sh"
exit 1
