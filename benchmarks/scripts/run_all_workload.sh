#!/bin/sh
set -eu

usage() {
    cat <<'EOF'
Usage:
  run_all_workload.sh [--output-root ABS_PATH] [--dry-run]

The matrix is fixed to:
  backends: rustkv, leveldb
  workloads: random_get, range_scan, single_put, batch_put, single_delete, batch_delete
  records: 10000 (1w), 100000 (10w), 1000000 (100w), 10000000 (1000w)
  threads: 1, 10, 100, 1000
  repetitions: 3 per valid combination

At records=10000 and threads=1000, range_scan, batch_put and batch_delete
are skipped for both backends because only 100 requests exist for 1000 workers.
The script executes 558 RunUnits under 186 combination directories and reports
six unsupported combinations (18 omitted repetitions).

The default output root is $HOME/work/result. Each successful existing repetition
is skipped; an incomplete or mismatched combination/repetition stops the run.
EOF
}

OUTPUT_ROOT=${HOME}/work/result
DRY_RUN=false
REPETITIONS=3

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

is_matching_combination() {
    combination_dir=$1
    expected_backend=$2
    expected_workload=$3
    expected_records=$4
    expected_threads=$5
    metadata="$combination_dir/combination.txt"

    test -d "$combination_dir" \
        && test ! -L "$combination_dir" \
        && test -f "$metadata" \
        && test "$(wc -l < "$metadata" | tr -d ' ')" -eq 6 \
        && grep -qx 'mode=custom-repetitions' "$metadata" \
        && grep -qx "record_count=$expected_records" "$metadata" \
        && grep -qx "backend=$expected_backend" "$metadata" \
        && grep -qx "workload=$expected_workload" "$metadata" \
        && grep -qx "threads=$expected_threads" "$metadata" \
        && grep -qx "repetitions=$REPETITIONS" "$metadata"
}

prepare_combination() {
    combination_dir=$1
    expected_backend=$2
    expected_workload=$3
    expected_records=$4
    expected_threads=$5

    if [ -e "$combination_dir" ]; then
        if ! is_matching_combination \
            "$combination_dir" "$expected_backend" "$expected_workload" \
            "$expected_records" "$expected_threads"; then
            echo "existing combination directory is incomplete or mismatched: $combination_dir" >&2
            echo "preserving it and stopping; inspect it before choosing a new root or removing it" >&2
            exit 1
        fi
        return
    fi

    mkdir -- "$combination_dir"
    {
        echo 'mode=custom-repetitions'
        echo "record_count=$expected_records"
        echo "backend=$expected_backend"
        echo "workload=$expected_workload"
        echo "threads=$expected_threads"
        echo "repetitions=$REPETITIONS"
    } > "$combination_dir/combination.txt"
}

TOTAL=558
CURRENT=0
SKIPPED_COMBINATIONS=0

run_combination() {
    BACKEND=$1
    WORKLOAD=$2
    RECORDS=$3
    SCALE=$4
    THREADS=$5

    case "$WORKLOAD" in
        range_scan|batch_put|batch_delete) REQUESTS=$((RECORDS / 100)) ;;
        *) REQUESTS=$RECORDS ;;
    esac

    if [ "$REQUESTS" -lt "$THREADS" ]; then
        SKIPPED_COMBINATIONS=$((SKIPPED_COMBINATIONS + 1))
        printf '[skip unsupported %d/6] backend=%s workload=%s records=%s threads=%s: only %s requests; omitted repetitions=3\n' \
            "$SKIPPED_COMBINATIONS" "$BACKEND" "$WORKLOAD" \
            "$RECORDS" "$THREADS" "$REQUESTS"
        return
    fi

    COMBINATION_DIR="$OUTPUT_ROOT/${BACKEND}_${WORKLOAD}_${SCALE}_t${THREADS}"
    if [ "$DRY_RUN" = false ]; then
        prepare_combination \
            "$COMBINATION_DIR" "$BACKEND" "$WORKLOAD" "$RECORDS" "$THREADS"
    fi

    REPETITION=1
    while [ "$REPETITION" -le "$REPETITIONS" ]; do
        CURRENT=$((CURRENT + 1))
        OUTPUT_DIR="$COMBINATION_DIR/repetition_${REPETITION}"

        if [ "$DRY_RUN" = true ]; then
            printf '[%d/%d] %s %s records=%s threads=%s repetition=%s output=%s\n' \
                "$CURRENT" "$TOTAL" "$BACKEND" "$WORKLOAD" \
                "$RECORDS" "$THREADS" "$REPETITION" "$OUTPUT_DIR"
            REPETITION=$((REPETITION + 1))
            continue
        fi

        if [ -e "$OUTPUT_DIR" ]; then
            if is_complete_result \
                "$OUTPUT_DIR" "$BACKEND" "$WORKLOAD" "$RECORDS" "$THREADS"; then
                printf '[%d/%d] skip completed repetition=%s %s\n' \
                    "$CURRENT" "$TOTAL" "$REPETITION" "$OUTPUT_DIR"
                REPETITION=$((REPETITION + 1))
                continue
            fi
            echo "existing repetition output is incomplete or mismatched: $OUTPUT_DIR" >&2
            echo "preserving it and stopping; inspect it before choosing a new root or removing it" >&2
            exit 1
        fi

        printf '[%d/%d] start backend=%s workload=%s records=%s threads=%s repetition=%s\n' \
            "$CURRENT" "$TOTAL" "$BACKEND" "$WORKLOAD" \
            "$RECORDS" "$THREADS" "$REPETITION"
        "$CUSTOM_RUN" \
            --backend "$BACKEND" \
            --workload "$WORKLOAD" \
            --threads "$THREADS" \
            --records "$RECORDS" \
            --output-dir "$OUTPUT_DIR"
        printf '[%d/%d] completed repetition=%s %s\n' \
            "$CURRENT" "$TOTAL" "$REPETITION" "$OUTPUT_DIR"
        REPETITION=$((REPETITION + 1))
    done
}

for RECORD_SPEC in 10000:1w 100000:10w 1000000:100w 10000000:1000w; do
    RECORDS=${RECORD_SPEC%%:*}
    SCALE=${RECORD_SPEC#*:}
    for THREADS in 1 10 100 1000; do
        for WORKLOAD in \
            random_get range_scan single_put batch_put single_delete batch_delete; do
            for BACKEND in leveldb rustkv; do
                run_combination \
                    "$BACKEND" "$WORKLOAD" "$RECORDS" "$SCALE" "$THREADS"
            done
        done
    done
done

test "$CURRENT" -eq "$TOTAL"
test "$SKIPPED_COMBINATIONS" -eq 6
if [ "$DRY_RUN" = true ]; then
    echo "dry-run listed 558 executable RunUnits in 186 combinations and 6 unsupported combinations (18 omitted repetitions): $OUTPUT_ROOT"
else
    echo "all 558 executable custom benchmarks completed in 186 combinations; skipped combinations=6; omitted repetitions=18: $OUTPUT_ROOT"
fi
