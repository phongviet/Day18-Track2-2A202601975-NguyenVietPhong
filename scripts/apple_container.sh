#!/usr/bin/env bash
# Run the Spark path on Apple's `container` runtime (macOS 15+, Apple silicon).
#
#   https://github.com/apple/container
#
# WHY THIS EXISTS
# ---------------
# `docker/docker-compose.yml` remains the primary Spark path and is unchanged.
# Apple's `container` cannot run it: there is no compose plugin
# (`container compose` -> "Plugin 'container-compose' not found") and no
# Docker API socket, so neither `docker` nor `docker compose` can drive it.
# This script brings up the same three-service stack with plain `container run`.
#
# THE ONE REAL DIFFERENCE
# -----------------------
# Compose resolves the service name `minio` over its built-in DNS. Apple's
# `container` has no name resolution unless you create a DNS domain, and
# `container system dns create` requires sudo. So instead of asking for root we
# read MinIO's IP with `container inspect` and inject it as MINIO_ENDPOINT.
# scripts/spark_session.py honours that variable and defaults to
# http://minio:9000, so the compose path behaves exactly as before.
#
# Usage: scripts/apple_container.sh {up|down|clean|smoke|data|status|logs|shell}
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

MINIO_NAME="lakehouse-minio"
SPARK_NAME="lakehouse-spark"
MINIO_IMAGE="docker.io/minio/minio:latest"
MC_IMAGE="docker.io/minio/mc:latest"
SPARK_IMAGE="quay.io/jupyter/pyspark-notebook:spark-3.5.0"

MINIO_DATA="${MINIO_DATA:-$REPO/_minio-data}"
MINIO_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-minioadmin}"
BUCKETS="lakehouse bronze silver gold"
JUPYTER_TOKEN="${JUPYTER_TOKEN:-lakehouse}"

# Apple `container` defaults every container to 1 GB / 4 CPUs, which is NOT
# enough for Spark: at 1 GB the JVM thrashes on the 1M-row Bronze job until the
# guest stops responding to `container exec` altogether. Compose never hit this
# because it inherited whatever the Docker VM had. Set them explicitly.
SPARK_CPUS="${SPARK_CPUS:-4}"
SPARK_MEM="${SPARK_MEM:-6g}"
MINIO_CPUS="${MINIO_CPUS:-2}"
MINIO_MEM="${MINIO_MEM:-2g}"

# The image exposes pyspark ONLY through PYTHONPATH (its 10spark-config.sh
# hook). Extend it, never replace it — replacing it is what broke the compose
# path for a year. py4j version tracks the pinned image tag.
SPARK_PYTHONPATH="/workspace/scripts:/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip"

# Colour only when stdout is a terminal. Otherwise the escape codes end up in
# CI logs and break downstream `grep`/`sed` with "illegal byte sequence".
if [ -t 1 ]; then C_INFO=$'\033[36m'; C_WARN=$'\033[33m'; C_ERR=$'\033[31m'; C_OFF=$'\033[0m'
else C_INFO=''; C_WARN=''; C_ERR=''; C_OFF=''; fi

log()  { printf '%s>>%s %s\n' "$C_INFO" "$C_OFF" "$*"; }
warn() { printf '%s!!%s %s\n' "$C_WARN" "$C_OFF" "$*" >&2; }
die()  { printf '%sxx%s %s\n' "$C_ERR" "$C_OFF" "$*" >&2; exit 1; }

require_container() {
  command -v container >/dev/null 2>&1 \
    || die "Apple 'container' not installed. brew install container"
  if ! container system status >/dev/null 2>&1; then
    log "Starting container system services ..."
    # `container system start` PROMPTS for a kernel on a fresh install and dies
    # with "failed to read user input" when there is no TTY. Install it first.
    container system kernel set --recommended >/dev/null 2>&1 || true
    container system start >/dev/null 2>&1 \
      || die "could not start container services; run: container system start"
  fi
}

# Match the container id EXACTLY. A grep over the raw JSON would also hit
# image names, labels and digests — and this machine may well have unrelated
# containers running that we must never touch.
exists() {
  container list -a --format json 2>/dev/null | python3 -c '
import json, sys
want = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(1)
ids = {c.get("configuration", {}).get("id") for c in data}
sys.exit(0 if want in ids else 1)
' "$1"
}

remove_if_exists() {
  if exists "$1"; then
    container stop "$1" >/dev/null 2>&1 || true
    container rm "$1" >/dev/null 2>&1 || true
  fi
}

minio_ip() {
  container inspect "$MINIO_NAME" 2>/dev/null | python3 -c '
import json, sys
d = json.load(sys.stdin)
d = d[0] if isinstance(d, list) else d
addr = d.get("status", {}).get("networks", [{}])[0].get("ipv4Address", "")
print(addr.split("/")[0])
'
}

wait_for_minio() {
  log "Waiting for MinIO to answer its health check ..."
  for _ in $(seq 1 40); do
    if curl -fsS --max-time 3 "http://127.0.0.1:9000/minio/health/live" >/dev/null 2>&1; then
      log "MinIO healthy."
      return 0
    fi
    sleep 2
  done
  container logs "$MINIO_NAME" 2>&1 | tail -20 || true
  die "MinIO did not become healthy in 80s"
}

up() {
  require_container
  mkdir -p "$MINIO_DATA"

  remove_if_exists "$MINIO_NAME"
  log "Starting MinIO ($MINIO_IMAGE) ..."
  container run -d --name "$MINIO_NAME" \
    -c "$MINIO_CPUS" -m "$MINIO_MEM" \
    -p 9000:9000 -p 9001:9001 \
    -e "MINIO_ROOT_USER=$MINIO_USER" \
    -e "MINIO_ROOT_PASSWORD=$MINIO_PASS" \
    -v "$MINIO_DATA:/data" \
    "$MINIO_IMAGE" server /data --console-address ":9001" >/dev/null

  wait_for_minio

  local ip
  ip="$(minio_ip)"
  [ -n "$ip" ] || die "could not determine MinIO IP from 'container inspect'"
  log "MinIO IP: $ip  (injected as MINIO_ENDPOINT — no DNS domain needed)"
  log "Spark container: ${SPARK_CPUS} cpus / ${SPARK_MEM} (override with SPARK_MEM=8g)"

  log "Creating buckets: $BUCKETS"
  container run --rm --entrypoint sh "$MC_IMAGE" -c "
    mc alias set local http://$ip:9000 $MINIO_USER $MINIO_PASS >/dev/null
    for b in $BUCKETS; do mc mb --ignore-existing local/\$b >/dev/null; done
    echo 'Buckets ready: $BUCKETS'
  " || die "bucket creation failed"

  remove_if_exists "$SPARK_NAME"
  log "Starting Spark/Jupyter ($SPARK_IMAGE) ..."
  container run -d --name "$SPARK_NAME" \
    -c "$SPARK_CPUS" -m "$SPARK_MEM" \
    -p 8888:8888 -p 4040:4040 \
    -v "$REPO:/workspace" \
    -w /workspace \
    -e "MINIO_ENDPOINT=http://$ip:9000" \
    -e "MINIO_ROOT_USER=$MINIO_USER" \
    -e "MINIO_ROOT_PASSWORD=$MINIO_PASS" \
    -e "AWS_ACCESS_KEY_ID=$MINIO_USER" \
    -e "AWS_SECRET_ACCESS_KEY=$MINIO_PASS" \
    -e "AWS_REGION=us-east-1" \
    -e "PYTHONPATH=$SPARK_PYTHONPATH" \
    -e "SPARK_IVY_DIR=/home/jovyan/.cache/ivy" \
    "$SPARK_IMAGE" bash -c '
      set -e
      export PATH="$HOME/.local/bin:$PATH"
      echo ">> Installing Python deps (first run only) ..."
      pip install --quiet --user \
        delta-spark==3.2.0 \
        jupytext==1.16.4 \
        faker==30.3.0 \
        "deltalake>=1.0,<2.0" \
        "pyiceberg[sql-sqlite,pyarrow]>=0.9,<1.0" \
        "duckdb>=1.1,<2.0" \
        "polars>=1.13.2,<2.0"
      echo ">> Converting .py -> .ipynb (best effort) ..."
      for d in /workspace/notebooks /workspace/notebooks-spark; do
        [ -d "$d" ] || continue
        cd "$d" || continue
        for f in *.py; do
          ipynb="${f%.py}.ipynb"
          [ -f "$ipynb" ] || python -m jupytext --to notebook "$f" 2>/dev/null \
            || echo "   (skipped $f - mount not writable by uid $(id -u))"
        done
      done
      cd /workspace
      echo ">> Starting Jupyter ..."
      start-notebook.py --IdentityProvider.token='"$JUPYTER_TOKEN"' \
        --ServerApp.root_dir=/workspace
    ' >/dev/null

  # Two separate waits. Jupyter announcing itself is NOT the same as the stack
  # being usable: `pip install --user` is still running when Jupyter starts, so
  # a smoke test fired too early fails with "No module named 'delta'". Gate on
  # the import actually succeeding, and FAIL if it never does — an "up" that
  # reports success while the stack is half-built is worse than an error.
  log "Waiting for Jupyter ..."
  jupyter_up=false
  for _ in $(seq 1 80); do
    if container logs "$SPARK_NAME" 2>&1 | grep -q "is running at"; then
      jupyter_up=true; break
    fi
    sleep 3
  done
  $jupyter_up || { container logs "$SPARK_NAME" 2>&1 | tail -20
                   die "Jupyter did not start within 240s"; }

  log "Waiting for in-container deps (pip install --user, first run only) ..."
  deps_ready=false
  for _ in $(seq 1 80); do
    if container exec "$SPARK_NAME" python -c 'import delta, pyspark' >/dev/null 2>&1; then
      deps_ready=true; break
    fi
    sleep 5
  done
  $deps_ready || die "delta/pyspark still not importable after 400s; check: $0 logs"

  echo
  log "Stack is up and ready."
  echo "   Jupyter  http://localhost:8888/lab?token=$JUPYTER_TOKEN"
  echo "   MinIO    http://localhost:9001  ($MINIO_USER / $MINIO_PASS)"
  echo
  echo "   Next:  scripts/apple_container.sh smoke"
}

down() {
  require_container
  log "Stopping stack (MinIO data in $MINIO_DATA is kept) ..."
  remove_if_exists "$SPARK_NAME"
  remove_if_exists "$MINIO_NAME"
  log "Down."
}

clean() {
  down
  log "Removing MinIO data dir $MINIO_DATA"
  rm -rf "$MINIO_DATA"
  log "Clean."
}

smoke() {
  require_container
  exists "$SPARK_NAME" || die "stack not running — run: scripts/apple_container.sh up"
  log "Running scripts/verify.py inside the Spark container ..."
  container exec "$SPARK_NAME" python /workspace/scripts/verify.py
}

data() {
  require_container
  exists "$SPARK_NAME" || die "stack not running — run: scripts/apple_container.sh up"
  log "Generating 1M-row Bronze via Spark ..."
  container exec "$SPARK_NAME" python /workspace/scripts/generate_data.py
}

status() {
  require_container
  container list -a
  echo
  if exists "$MINIO_NAME"; then echo "MinIO IP: $(minio_ip)"; fi
}

logs()  { require_container; container logs "${2:-$SPARK_NAME}"; }
shell() { require_container; container exec -ti "$SPARK_NAME" bash; }

case "${1:-}" in
  up)     up ;;
  down)   down ;;
  clean)  clean ;;
  smoke)  smoke ;;
  data)   data ;;
  status) status ;;
  logs)   logs "$@" ;;
  shell)  shell ;;
  *) cat <<EOF
Run the Day 18 Spark path on Apple's \`container\` runtime.

  up      Start MinIO + buckets + Spark/Jupyter
  smoke   Run scripts/verify.py in the Spark container
  data    Generate the 1M-row Bronze table via Spark
  status  Show containers and MinIO's IP
  logs    Tail container logs (default: $SPARK_NAME)
  shell   Open a shell in the Spark container
  down    Stop and remove containers (MinIO data kept)
  clean   Same as down, plus delete $MINIO_DATA

The docker-compose path is unchanged; use \`make spark-up\` for that.
EOF
     exit 1 ;;
esac
