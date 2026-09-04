#!/bin/sh
set -eu

usage() {
    cat <<'EOF'
Usage:
  run_custom.sh --backend rustkv|leveldb --workload NAME \
    --threads 1|10|100|1000 --records N --output-dir ABS_PATH

Workloads:
  random_get, range_scan, single_put, batch_put, single_delete, batch_delete

N must be at least 100 and divisible by 100. One invocation executes exactly
one selected Backend/workload/thread-count/record-count combination.
EOF
}

BACKEND=
WORKLOAD=
THREADS=
RECORDS=
OUTPUT_DIR=

while [ "$#" -gt 0 ]; do
    case "$1" in
        --backend|--workload|--threads|--records|--output-dir)
            if [ "$#" -lt 2 ]; then
                echo "option $1 requires a value" >&2
                usage >&2
                exit 2
            fi
            OPTION=$1
            VALUE=$2
            shift 2
            case "$OPTION" in
                --backend) BACKEND=$VALUE ;;
                --workload) WORKLOAD=$VALUE ;;
                --threads) THREADS=$VALUE ;;
                --records) RECORDS=$VALUE ;;
                --output-dir) OUTPUT_DIR=$VALUE ;;
            esac
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [ -z "$BACKEND" ] || [ -z "$WORKLOAD" ] || [ -z "$THREADS" ] \
    || [ -z "$RECORDS" ] || [ -z "$OUTPUT_DIR" ]; then
    echo "backend, workload, threads, records and output-dir are all required" >&2
    usage >&2
    exit 2
fi

case "$BACKEND" in
    rustkv|leveldb) ;;
    *)
        echo "--backend must be rustkv or leveldb" >&2
        exit 2
        ;;
esac

case "$WORKLOAD" in
    random_get|range_scan|single_put|batch_put|single_delete|batch_delete) ;;
    *)
        echo "unknown --workload: $WORKLOAD" >&2
        exit 2
        ;;
esac

case "$THREADS" in
    1|10|100|1000) ;;
    *)
        echo "--threads must be one of 1, 10, 100, 1000" >&2
        exit 2
        ;;
esac

case "$RECORDS" in
    ''|*[!0-9]*)
        echo "--records must be an integer" >&2
        exit 2
        ;;
esac

case "$OUTPUT_DIR" in
    /*) ;;
    *)
        echo "--output-dir must be an absolute path" >&2
        exit 2
        ;;
esac

if [ -e "$OUTPUT_DIR" ]; then
    echo "output directory already exists: $OUTPUT_DIR" >&2
    exit 1
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CRATE_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
RUSTKV_DIR=$(CDPATH= cd -- "$CRATE_DIR/.." && pwd)
RUSTKV_COMMIT=$(git -C "$RUSTKV_DIR" rev-parse HEAD)
if [ -n "$(git -C "$RUSTKV_DIR" status --porcelain)" ]; then
    WORKTREE_STATE=dirty
else
    WORKTREE_STATE=clean
fi

cleanup_workspace() {
    if [ -f "$OUTPUT_DIR/parameters.txt" ] \
        && grep -q '^mode=custom$' "$OUTPUT_DIR/parameters.txt" \
        && [ -d "$OUTPUT_DIR/workspace" ]; then
        rm -rf "$OUTPUT_DIR/workspace"
    fi
}
trap cleanup_workspace EXIT
trap 'cleanup_workspace; exit 129' HUP
trap 'cleanup_workspace; exit 130' INT
trap 'cleanup_workspace; exit 143' TERM

cd "$CRATE_DIR"
cargo build --release --locked --bin kv_bench
"$CRATE_DIR/target/release/kv_bench" custom-run \
    --output-dir "$OUTPUT_DIR" \
    --backend "$BACKEND" \
    --workload "$WORKLOAD" \
    --threads "$THREADS" \
    --records "$RECORDS" \
    --rustkv-commit "$RUSTKV_COMMIT" \
    --worktree-state "$WORKTREE_STATE"

test -f "$OUTPUT_DIR/result.csv"
test -f "$OUTPUT_DIR/result.md"
test -f "$OUTPUT_DIR/parameters.txt"
test ! -e "$OUTPUT_DIR/workspace"
grep -q '^custom,' "$OUTPUT_DIR/result.csv"
grep -q '^formal_result=false$' "$OUTPUT_DIR/parameters.txt"

trap - EXIT HUP INT TERM
echo "custom benchmark passed; output retained at $OUTPUT_DIR"
