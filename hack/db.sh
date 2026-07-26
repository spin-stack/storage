#!/usr/bin/env bash
# PostgreSQL lifecycle for the schema tooling (ADR-0019, pgschema).
#
# pgschema is state-based: it computes DDL by diffing internal/schema/schema.sql
# against a *live* database, so every schema task needs one running. This script is
# the only place one is started, which is what keeps the rule that no task assumes a
# PostgreSQL on the developer's machine — and it always starts the same pinned
# Postgres 18 image the integration lane uses, so `task db:plan` and the tests can
# never disagree about what the server supports.
#
# Invoked only from Taskfile targets (task db:dev:up / db:dev:down / db:verify).
#
# Subcommands:
#   up      start (or reuse) the long-lived development database
#   down    remove it
#   verify  on a throwaway empty database: apply schema.sql, then assert a plan
#           against the result has nothing left to do
set -euo pipefail

POSTGRES_IMAGE=${POSTGRES_IMAGE:?POSTGRES_IMAGE is required}
DEVDB_CONTAINER=${DEVDB_CONTAINER:-spin-storage-devdb}
DEVDB_PORT=${DEVDB_PORT:-55432}
PGSCHEMA=${PGSCHEMA:-.tools/bin/pgschema}
SCHEMA_FILE=${SCHEMA_FILE:-internal/schema/schema.sql}

DB_USER=cp
DB_PASSWORD=cp
DB_NAME=cp

# wait_ready <container> — block until the server accepts connections, or fail
# loudly with its log rather than letting the next command report a refused socket.
wait_ready() {
  local c=$1 i
  for ((i = 0; i < 60; i++)); do
    if docker exec "$c" pg_isready -q -U "$DB_USER" -d "$DB_NAME" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "postgres in $c never became ready; last log lines:" >&2
  docker logs --tail 30 "$c" >&2
  return 1
}

run_postgres() {
  docker run "$@" \
    -e POSTGRES_USER="$DB_USER" \
    -e POSTGRES_PASSWORD="$DB_PASSWORD" \
    -e POSTGRES_DB="$DB_NAME" \
    "$POSTGRES_IMAGE"
}

# conn_flags <port> — target *and* plan connection flags. --plan-* is never
# omitted: without it pgschema falls back to an embedded PostgreSQL it downloads at
# run time, which is neither pinned nor the version this project targets.
conn_flags() {
  local port=$1
  echo "--host localhost --port $port --db $DB_NAME --user $DB_USER --sslmode disable" \
    "--plan-host localhost --plan-port $port --plan-db $DB_NAME --plan-user $DB_USER" \
    "--plan-password $DB_PASSWORD --plan-sslmode disable"
}

cmd_up() {
  if [ "$(docker inspect -f '{{.State.Running}}' "$DEVDB_CONTAINER" 2>/dev/null)" = "true" ]; then
    echo "development database $DEVDB_CONTAINER already running on port $DEVDB_PORT"
    return 0
  fi
  docker rm -f "$DEVDB_CONTAINER" >/dev/null 2>&1 || true
  run_postgres -d --name "$DEVDB_CONTAINER" -p "127.0.0.1:$DEVDB_PORT:5432" >/dev/null
  wait_ready "$DEVDB_CONTAINER"
  echo "development database $DEVDB_CONTAINER ready on port $DEVDB_PORT ($POSTGRES_IMAGE)"
}

cmd_down() {
  docker rm -f "$DEVDB_CONTAINER" >/dev/null 2>&1 || true
  echo "development database $DEVDB_CONTAINER removed"
}

# The check that replaces atlas.sum (ADR-0019): rather than checksumming the bytes
# of a migration chain, build the schema the way production would and compare the
# *result* against the declared state. An unrepresentable construct, a statement
# pgschema applies but cannot read back, or an edit to schema.sql that no plan can
# express all surface here as a non-empty second plan.
cmd_verify() {
  test -x "$PGSCHEMA" || {
    echo "missing $PGSCHEMA — run: task tools" >&2
    exit 1
  }

  local cid
  cid=$(run_postgres -d --rm -p 127.0.0.1::5432)
  # shellcheck disable=SC2064 # $cid must expand now, not at trap time.
  trap "docker rm -f $cid >/dev/null 2>&1 || true" EXIT
  wait_ready "$cid"

  local port flags tmp
  port=$(docker port "$cid" 5432/tcp | head -1)
  port=${port##*:}
  read -r -a flags <<<"$(conn_flags "$port")"
  tmp=$(mktemp -d)
  # shellcheck disable=SC2064
  trap "docker rm -f $cid >/dev/null 2>&1 || true; rm -rf $tmp" EXIT

  echo "==> applying $SCHEMA_FILE to an empty $POSTGRES_IMAGE"
  PGPASSWORD=$DB_PASSWORD "$PGSCHEMA" apply "${flags[@]}" \
    --file "$SCHEMA_FILE" --auto-approve --no-color >"$tmp/apply.log" || {
    cat "$tmp/apply.log" >&2
    exit 1
  }

  echo "==> re-planning: the declared state must already be reached"
  PGPASSWORD=$DB_PASSWORD "$PGSCHEMA" plan "${flags[@]}" \
    --file "$SCHEMA_FILE" --no-color \
    --output-sql "$tmp/plan.sql" --output-json "$tmp/plan.json"

  # Two independent readings of "nothing to do": no DDL in the SQL rendering, and a
  # plan with no groups in the JSON one. Either alone would pass on a tool that
  # silently produced an empty file.
  if [ -s "$tmp/plan.sql" ] && grep -q '[^[:space:]]' "$tmp/plan.sql"; then
    echo "FAIL: applying $SCHEMA_FILE does not reach the state it declares." >&2
    echo "A plan against the result still wants to run:" >&2
    cat "$tmp/plan.sql" >&2
    exit 1
  fi
  if ! grep -q '"groups": *null' "$tmp/plan.json"; then
    echo "FAIL: the plan against the applied schema is not empty:" >&2
    cat "$tmp/plan.json" >&2
    exit 1
  fi
  echo "OK: $SCHEMA_FILE applies to an empty database and the result matches it exactly"
}

case "${1:-}" in
up) cmd_up ;;
down) cmd_down ;;
verify) cmd_verify ;;
*)
  echo "usage: $0 up|down|verify" >&2
  exit 2
  ;;
esac
