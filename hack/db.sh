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
# Invoked only from Taskfile targets (task db:dev:up / db:dev:down / db:init / db:verify).
#
# Subcommands:
#   up      start (or reuse) the long-lived development database
#   down    remove it
#   init    apply schema.sql to a database that does not have it yet
#   verify  run `init` against a throwaway empty database — the check that replaces
#           atlas.sum, and the reason `init` is exercised by CI on every run
set -euo pipefail

POSTGRES_IMAGE=${POSTGRES_IMAGE:?POSTGRES_IMAGE is required}
DEVDB_CONTAINER=${DEVDB_CONTAINER:-spin-storage-devdb}
DEVDB_PORT=${DEVDB_PORT:-55432}
PGSCHEMA=${PGSCHEMA:-.tools/bin/pgschema}
SCHEMA_FILE=${SCHEMA_FILE:-internal/schema/schema.sql}

DB_USER=cp
DB_PASSWORD=cp
DB_NAME=cp

# The database `init` applies the declared state to. It defaults to the development
# database `up` starts, which is what makes `task db:dev:up && task db:init` the whole of
# a newcomer's set-up; `verify` overrides it with its own throwaway.
TARGET_HOST=${TARGET_HOST:-localhost}
TARGET_PORT=${TARGET_PORT:-$DEVDB_PORT}
TARGET_DB=${TARGET_DB:-$DB_NAME}
TARGET_USER=${TARGET_USER:-$DB_USER}
TARGET_PASSWORD=${PGPASSWORD:-$DB_PASSWORD}

# One scratch directory and one container handle, cleaned up by one trap: the subcommands
# nest (verify calls init), and two functions each installing their own EXIT trap means
# the inner one silently replaces the outer and leaks whatever it was there to remove.
scratch=$(mktemp -d)
cid=
cleanup() {
  rm -rf "$scratch"
  [ -n "$cid" ] && docker rm -f "$cid" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

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

# conn_flags — target *and* plan connection flags for `plan` and `apply`. --plan-* is
# never omitted: without it pgschema falls back to an embedded PostgreSQL it downloads at
# run time, which is neither pinned nor the version this project targets.
conn_flags() {
  echo "--host $TARGET_HOST --port $TARGET_PORT --db $TARGET_DB --user $TARGET_USER --sslmode disable" \
    "--plan-host $TARGET_HOST --plan-port $TARGET_PORT --plan-db $TARGET_DB --plan-user $TARGET_USER" \
    "--plan-password $TARGET_PASSWORD --plan-sslmode disable"
}

# dump_flags — the same target, for `dump`, which has no plan database and rejects the
# --plan-* flags.
dump_flags() {
  echo "--host $TARGET_HOST --port $TARGET_PORT --db $TARGET_DB --user $TARGET_USER --sslmode disable"
}

need_pgschema() {
  test -x "$PGSCHEMA" || {
    echo "missing $PGSCHEMA — run: task tools" >&2
    exit 1
  }
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

# assert_declared_state_reached — re-plan against the target and require the plan to be
# empty. Two independent readings of "nothing to do": no DDL in the SQL rendering, and no
# groups in the JSON one. Either alone would pass on a tool that silently produced an
# empty file.
assert_declared_state_reached() {
  local flags
  read -r -a flags <<<"$(conn_flags)"
  PGPASSWORD=$TARGET_PASSWORD "$PGSCHEMA" plan "${flags[@]}" \
    --file "$SCHEMA_FILE" --no-color \
    --output-sql "$scratch/plan.sql" --output-json "$scratch/plan.json"

  if [ -s "$scratch/plan.sql" ] && grep -q '[^[:space:]]' "$scratch/plan.sql"; then
    echo "FAIL: applying $SCHEMA_FILE does not reach the state it declares." >&2
    echo "A plan against the result still wants to run:" >&2
    cat "$scratch/plan.sql" >&2
    exit 1
  fi
  if ! grep -q '"groups": *null' "$scratch/plan.json"; then
    echo "FAIL: the plan against the applied schema is not empty:" >&2
    cat "$scratch/plan.json" >&2
    exit 1
  fi
}

# target_is_empty — true when the target's schema holds no objects. pgschema's own dump is
# what answers it, so "empty" means empty *to the tool that is about to write into it*,
# and no psql is needed anywhere. Comment lines are the whole of a dump of nothing.
target_is_empty() {
  local flags
  read -r -a flags <<<"$(dump_flags)"
  if ! PGPASSWORD=$TARGET_PASSWORD "$PGSCHEMA" dump "${flags[@]}" >"$scratch/dump.sql" 2>"$scratch/dump.err"; then
    cat "$scratch/dump.err" >&2
    echo "could not read $TARGET_DB on $TARGET_HOST:$TARGET_PORT. If it does not exist yet, 'task db:dev:up' starts the development one." >&2
    exit 1
  fi
  ! grep -v '^--' "$scratch/dump.sql" | grep -q '[^[:space:]]'
}

# cmd_init — the only supported way to take an *empty* database to the declared state.
# db:plan/db:apply are for changes: they diff schema.sql against an existing database and
# the reviewable artefact is the plan.
#
# The emptiness gate is the load-bearing part, not a convenience. "Apply a saved plan,
# never a recomputed one" is about diffs — a recomputed diff can contain a DROP nobody
# read. Against an empty database the only plan is "create everything in schema.sql", it is
# determined by the file already under review, and it can destroy nothing. A non-empty
# database is refused; re-running against one this already initialized finds nothing to do.
cmd_init() {
  need_pgschema
  local flags
  read -r -a flags <<<"$(conn_flags)"

  if ! target_is_empty; then
    echo "==> $TARGET_DB is not empty; checking whether it already matches $SCHEMA_FILE"
    PGPASSWORD=$TARGET_PASSWORD "$PGSCHEMA" plan "${flags[@]}" \
      --file "$SCHEMA_FILE" --no-color \
      --output-sql "$scratch/pre.sql" --output-json "$scratch/pre.json" >/dev/null
    if ! grep -q '[^[:space:]]' "$scratch/pre.sql"; then
      echo "OK: $TARGET_DB already holds exactly what $SCHEMA_FILE declares; nothing to do"
      return 0
    fi
    cat >&2 <<EOF
FAIL: $TARGET_DB on $TARGET_HOST:$TARGET_PORT is not empty and does not match $SCHEMA_FILE.
db:init only ever creates; taking a database that already holds objects to the declared
state is a *diff*, and a diff is reviewed as a plan and applied from the file:

  task db:plan -- <name>            # writes migrations/<ts>_<name>.{sql,json}
  task db:apply PLAN=migrations/<ts>_<name>.json

What a plan against it wants to run right now:
EOF
    cat "$scratch/pre.sql" >&2
    exit 1
  fi

  echo "==> applying $SCHEMA_FILE to the empty database $TARGET_DB on $TARGET_HOST:$TARGET_PORT"
  PGPASSWORD=$TARGET_PASSWORD "$PGSCHEMA" apply "${flags[@]}" \
    --file "$SCHEMA_FILE" --auto-approve --no-color >"$scratch/apply.log" || {
    cat "$scratch/apply.log" >&2
    exit 1
  }

  # Never "applied, therefore correct": the same check db:verify is named for, run against
  # the database a human is about to use.
  echo "==> re-planning: the declared state must already be reached"
  assert_declared_state_reached
  echo "OK: $TARGET_DB matches $SCHEMA_FILE exactly"
}

# The check that replaces atlas.sum (ADR-0019): rather than checksumming the bytes of a
# migration chain, build the schema the way a new database is built and compare the
# *result* against the declared state. An unrepresentable construct, a statement pgschema
# applies but cannot read back, or an edit to schema.sql that no plan can express all
# surface here as a non-empty second plan.
#
# It runs `init` rather than its own apply, so the command CI proves is the command a
# human runs. Before, the two were separate code paths and only one of them was ever
# executed by anything.
cmd_verify() {
  need_pgschema

  cid=$(run_postgres -d --rm -p 127.0.0.1::5432)
  wait_ready "$cid"

  TARGET_HOST=localhost
  TARGET_PORT=$(docker port "$cid" 5432/tcp | head -1)
  TARGET_PORT=${TARGET_PORT##*:}
  TARGET_DB=$DB_NAME
  TARGET_USER=$DB_USER
  TARGET_PASSWORD=$DB_PASSWORD

  cmd_init
  echo "OK: $SCHEMA_FILE applies to an empty database and the result matches it exactly"
}

case "${1:-}" in
up) cmd_up ;;
down) cmd_down ;;
init) cmd_init ;;
verify) cmd_verify ;;
*)
  echo "usage: $0 up|down|init|verify" >&2
  exit 2
  ;;
esac
