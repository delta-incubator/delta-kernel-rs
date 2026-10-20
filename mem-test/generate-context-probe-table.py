#!/usr/bin/env python3
"""Generate a local Delta log for the context-memory metadata replay probe."""

import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table", type=Path)
    parser.add_argument("--commits", type=int, default=100)
    parser.add_argument("--files-per-commit", type=int, default=100)
    args = parser.parse_args()
    if args.commits <= 0 or args.files_per_commit <= 0:
        parser.error("commit and file counts must be positive")
    log = args.table / "_delta_log"
    log.mkdir(parents=True)
    schema = json.dumps({
        "type": "struct",
        "fields": [{"name": "value", "type": "integer", "nullable": True, "metadata": {}}],
    })
    for commit in range(args.commits):
        actions = []
        if commit == 0:
            actions = [
                {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
                {"metaData": {
                    "id": "memory-probe",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": schema,
                    "partitionColumns": [],
                    "configuration": {},
                }},
            ]
        for item in range(args.files_per_commit):
            index = commit * args.files_per_commit + item
            actions.append({"add": {
                "path": str(index) + ".parquet",
                "partitionValues": {},
                "size": 1_000_000,
                "modificationTime": commit * 1000,
                "dataChange": True,
                "stats": json.dumps({
                    "numRecords": 1000,
                    "minValues": {"value": index},
                    "maxValues": {"value": index + 999},
                    "nullCount": {"value": 0},
                }),
            }})
        (log / f"{commit:020}.json").write_text(
            "".join(json.dumps(action) + "\n" for action in actions), encoding="utf-8"
        )


if __name__ == "__main__":
    main()
