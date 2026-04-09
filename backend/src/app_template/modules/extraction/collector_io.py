#!/usr/bin/env python3
"""JSON/CSV IO helpers used by collector main workflow."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count


def write_json(path: Path, rows: List[Dict[str, Any]]) -> int:
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(rows)


def read_projects_file(path: Path) -> List[Dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        rows: List[Dict[str, Any]] = []
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                rows.append(parsed)
        return rows
    if suffix == ".json":
        parsed = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(parsed, list):
            raise RuntimeError(f"Expected JSON array in file: {path}")
        return [row for row in parsed if isinstance(row, dict)]
    raise RuntimeError(f"Unsupported input file format (use .json or .jsonl): {path}")




