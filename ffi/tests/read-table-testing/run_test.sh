#!/bin/bash

set -euxo pipefail

OUT_FILE=$(mktemp)
pwd
ls -l
./read_table "$1" > "$OUT_FILE"
cat "$OUT_FILE"
diff -s "$OUT_FILE" "$2"
DIFF_EXIT_CODE=$?
echo "Diff exited with $DIFF_EXIT_CODE"
rm "$OUT_FILE"
exit "$DIFF_EXIT_CODE"

