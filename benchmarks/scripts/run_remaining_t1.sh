#!/bin/sh
set -eu

usage() {
    cat <<'EOF'
Usage:
  run_remaining_t1.sh [--output-root ABS_PATH] [--dry-run]

The matrix is fixed to:
  backends: rustkv, leveldb
  records: 10000 (1w), 100000 (10w), 1000000 (100w), 10000000 (1000w)
  threads=1: random_get, range_scan, single_delete, batch_delete
  threads=10/100/1000: all six workloads

At records=10000 and threads=1000, range_scan, batch_put and batch_delete
are skipped for both backends because only 100 requests exist for 1000 workers.
The script executes 170 valid RunUnits and reports the six explicit skips.

The default output root is $HOME/work/result. Each successful existing result
is skipped; an incomplete or mismatched existing output directory stops the run.
EOF
}

OUTPUT_ROOT=${HOME}/work/result
DRY_RUN=false

while [ "$#" -gt 0 ]; do
    case "$1" in
        --output-root)
            if [ "$#" -lt 2 ]; then
                echo "option --output-root requires a value" >&2
                usage >&2
                exit 2
            fi
            OUTPUT_ROOT=$2
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
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

case "$OUTPUT_ROOT" in
    /*) ;;
    *)
        echo "--output-root must be an absolute path" >&2
        exit 2
        ;;
esac

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CUSTOM_RUN="$SCRIPT_DIR/run_custom.sh"
test -x "$CUSTOM_RUN" || {
    echo "run_custom.sh is missing or not executable: $CUSTOM_RUN" >&2
    exit 1
}

if [ "$DRY_RUN" = false ]; then
    mkdir -p -- "$OUTPUT_ROOT"
fi

is_complete_result() {
    result_dir=$1
    expected_backend=$2
    expected_workload=$3
    expected_records=$4
    expected_threads=$5

    test -f "$result_dir/parameters.txt" \
        && test -f "$result_dir/result.csv" \
        && test -f "$result_dir/result.md" \
        && test ! -e "$result_dir/workspace" \
        && grep -qx 'mode=custom' "$result_dir/parameters.txt" \
        && grep -qx 'formal_result=false' "$result_dir/parameters.txt" \
        && grep -qx "record_count=$expected_records" "$result_dir/parameters.txt" \
        && grep -qx "backend=$expected_backend" "$result_dir/parameters.txt" \
        && grep -qx "workload=$expected_workload" "$result_dir/parameters.txt" \
        && grep -qx "threads=$expected_threads" "$result_dir/parameters.txt" \
        && test "$(wc -l < "$result_dir/result.csv" | tr -d ' ')" -eq 2 \
        && awk -F, -v backend="$expected_backend" \
            -v workload="$expected_workload" -v records="$expected_records" \
            -v threads="$expected_threads" '
                NR == 2 && $1 == "custom" && $3 == records && $8 == backend \
                    && $9 == workload && $10 == threads && $20 == 0 \
                    && $21 == "true" {
                    valid = 1
                }
                END { exit(valid ? 0 : 1) }
            ' "$result_dir/result.csv"
}

TOTAL=170
CURRENT=0
SKIPPED=0

run_selected() {
    BACKEND=$1
    WORKLOAD=$2
    RECORDS=$3
    SCALE=$4
    THREADS=$5

    CURRENT=$((CURRENT + 1))
    OUTPUT_DIR="$OUTPUT_ROOT/${BACKEND}_${WORKLOAD}_${SCALE}_t${THREADS}"

    if [ "$DRY_RUN" = true ]; then
        printf '[%d/%d] %s %s records=%s threads=%s output=%s\n' \
            "$CURRENT" "$TOTAL" "$BACKEND" "$WORKLOAD" \
            "$RECORDS" "$THREADS" "$OUTPUT_DIR"
        return
    fi

    if [ -e "$OUTPUT_DIR" ]; then
        if is_complete_result \
            "$OUTPUT_DIR" "$BACKEND" "$WORKLOAD" "$RECORDS" "$THREADS"; then
            printf '[%d/%d] skip completed %s\n' \
                "$CURRENT" "$TOTAL" "$OUTPUT_DIR"
            return
        fi
        echo "existing output is incomplete or mismatched: $OUTPUT_DIR" >&2
        echo "preserving it and stopping; inspect it before choosing a new root or removing it" >&2
        exit 1
    fi

    printf '[%d/%d] start backend=%s workload=%s records=%s threads=%s\n' \
        "$CURRENT" "$TOTAL" "$BACKEND" "$WORKLOAD" "$RECORDS" "$THREADS"
    "$CUSTOM_RUN" \
        --backend "$BACKEND" \
        --workload "$WORKLOAD" \
        --threads "$THREADS" \
        --records "$RECORDS" \
        --output-dir "$OUTPUT_DIR"
    printf '[%d/%d] completed %s\n' "$CURRENT" "$TOTAL" "$OUTPUT_DIR"
}

for RECORD_SPEC in 10000:1w 100000:10w 1000000:100w 10000000:1000w; do
    RECORDS=${RECORD_SPEC%%:*}
    SCALE=${RECORD_SPEC#*:}
    for WORKLOAD in random_get range_scan single_delete batch_delete; do
        for BACKEND in leveldb rustkv; do
            run_selected "$BACKEND" "$WORKLOAD" "$RECORDS" "$SCALE" 1
        done
    done
done

for RECORD_SPEC in 10000:1w 100000:10w 1000000:100w 10000000:1000w; do
    RECORDS=${RECORD_SPEC%%:*}
    SCALE=${RECORD_SPEC#*:}
    for THREADS in 10 100 1000; do
        for WORKLOAD in \
            random_get range_scan single_put batch_put single_delete batch_delete; do
            for BACKEND in leveldb rustkv; do
                if [ "$RECORDS" = 10000 ] && [ "$THREADS" = 1000 ]; then
                    case "$WORKLOAD" in
                        range_scan|batch_put|batch_delete)
                            SKIPPED=$((SKIPPED + 1))
                            printf '[skip unsupported %d/6] backend=%s workload=%s records=%s threads=%s: only 100 requests\n' \
                                "$SKIPPED" "$BACKEND" "$WORKLOAD" \
                                "$RECORDS" "$THREADS"
                            continue
                            ;;
                    esac
                fi
                run_selected \
                    "$BACKEND" "$WORKLOAD" "$RECORDS" "$SCALE" "$THREADS"
            done
        done
    done
done

test "$CURRENT" -eq "$TOTAL"
test "$SKIPPED" -eq 6
if [ "$DRY_RUN" = true ]; then
    echo "dry-run listed 170 executable RunUnits and 6 explicit skips: $OUTPUT_ROOT"
else
    echo "all 170 executable custom benchmarks completed; skipped=6: $OUTPUT_ROOT"
fi
