#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

if [ "$#" -gt 1 ]; then
    echo "usage: $0 [absolute-output-directory]" >&2
    exit 2
fi

if [ "$#" -eq 1 ]; then
    OUTPUT_DIR=$1
    case "$OUTPUT_DIR" in
        /*) ;;
        *)
            echo "smoke output directory must be absolute" >&2
            exit 2
            ;;
    esac
else
    SMOKE_PARENT=$(mktemp -d "${TMPDIR:-/tmp}/kv-bench-smoke.XXXXXX")
    OUTPUT_DIR="$SMOKE_PARENT/output"
fi

cd "$SCRIPT_DIR/.."
cargo run --release --locked -- smoke --output-dir "$OUTPUT_DIR"

test -f "$OUTPUT_DIR/raw-smoke.csv"
test -f "$OUTPUT_DIR/report/report.md"
grep -q '^smoke,' "$OUTPUT_DIR/raw-smoke.csv"
grep -q 'Smoke（非正式性能结果）' "$OUTPUT_DIR/report/report.md"
grep -q '每个 RunUnit 使用独立新目录' "$OUTPUT_DIR/report/report.md"
test "$(wc -l < "$OUTPUT_DIR/raw-smoke.csv" | tr -d ' ')" -eq 25

echo "smoke verification passed; output retained at $OUTPUT_DIR"
