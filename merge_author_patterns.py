from __future__ import annotations

import argparse
import csv
from pathlib import Path

import infer_core


FIELDNAMES = [
    "source",
    "author_raw",
    "author_first_last",
    "author_last_first",
    "title_example",
    "language",
    "source_author_id",
    "source_work_id",
    "source_url",
    "confidence",
    "notes",
]
SOURCE_PRIORITY = {"lubimyczytac": 0, "openlibrary": 1}
CONFIDENCE_PRIORITY = {"high": 2, "medium": 1, "low": 0}


def row_sort_key(row: dict[str, str]) -> tuple[int, int, int]:
    source = (row.get("source") or "").strip().lower()
    confidence = (row.get("confidence") or "").strip().lower()
    author_len = len((row.get("author_first_last") or "").strip())
    return (
        SOURCE_PRIORITY.get(source, 99),
        -CONFIDENCE_PRIORITY.get(confidence, -1),
        -author_len,
    )


def pick_best_value(rows: list[dict[str, str]], field: str) -> str:
    candidates = [((row.get(field) or "").strip(), row) for row in rows]
    candidates = [(value, row) for value, row in candidates if value]
    if not candidates:
        return ""
    candidates.sort(key=lambda item: row_sort_key(item[1]))
    if field in {"author_raw", "author_first_last", "author_last_first"}:
        candidates.sort(key=lambda item: ("..." in item[0], row_sort_key(item[1]), -len(item[0])))
    return candidates[0][0]


def merge_joined(rows: list[dict[str, str]], field: str, *, pair_with_source: bool = False) -> str:
    seen: set[str] = set()
    parts: list[str] = []
    for row in sorted(rows, key=row_sort_key):
        raw_value = (row.get(field) or "").strip()
        if not raw_value:
            continue
        values = [part.strip() for part in raw_value.replace(" | ", "|").split("|")]
        for value in values:
            if not value:
                continue
            if pair_with_source:
                source = (row.get("source") or "").strip()
                source_value = f"{source}:{value}" if source else value
                if source_value not in seen:
                    seen.add(source_value)
                    parts.append(source_value)
            else:
                if value not in seen:
                    seen.add(value)
                    parts.append(value)
    return " | ".join(parts)


def merge_group(rows: list[dict[str, str]]) -> dict[str, str]:
    best_first_last = pick_best_value(rows, "author_first_last")
    best_last_first = pick_best_value(rows, "author_last_first")
    best_raw = pick_best_value(rows, "author_raw")
    best_title = pick_best_value(rows, "title_example")
    confidence = "high" if any((row.get("confidence") or "").strip().lower() == "high" for row in rows) else "medium"
    return {
        "source": merge_joined(rows, "source"),
        "author_raw": best_raw,
        "author_first_last": best_first_last,
        "author_last_first": best_last_first,
        "title_example": best_title,
        "language": merge_joined(rows, "language"),
        "source_author_id": merge_joined(rows, "source_author_id", pair_with_source=True),
        "source_work_id": merge_joined(rows, "source_work_id", pair_with_source=True),
        "source_url": merge_joined(rows, "source_url"),
        "confidence": confidence,
        "notes": merge_joined(rows, "notes"),
    }


def build_group_key(row: dict[str, str]) -> str:
    for field in ("author_first_last", "author_last_first", "author_raw"):
        value = (row.get(field) or "").strip()
        if value:
            key = infer_core.author_key(value)
            if key:
                return key
    return ""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scala author_patterns.csv do jednego wpisu na autora.")
    parser.add_argument("--input", default="author_patterns.csv", help="Wejściowy CSV.")
    parser.add_argument("--output", default="author_patterns.csv", help="Wyjściowy CSV.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()

    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    groups: dict[str, list[dict[str, str]]] = {}
    ungrouped: list[dict[str, str]] = []
    for row in rows:
        key = build_group_key(row)
        if not key:
            ungrouped.append(row)
            continue
        groups.setdefault(key, []).append(row)

    merged_rows = [merge_group(group_rows) for _, group_rows in sorted(groups.items(), key=lambda item: item[0])]
    merged_rows.extend(ungrouped)
    merged_rows.sort(key=lambda row: infer_core.author_key(row.get("author_first_last") or row.get("author_raw") or ""))

    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(merged_rows)

    print(f"input_rows={len(rows)}")
    print(f"merged_rows={len(merged_rows)}")
    print(f"collapsed={len(rows) - len(merged_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
