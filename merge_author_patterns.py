from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import tempfile
from pathlib import Path

import infer_core


FIELDNAMES = ["source", "author_raw", "author_first_last", "author_last_first"]
AUTHOR_NAME_FIELDS = {"author_raw", "author_first_last", "author_last_first"}
SOURCE_PRIORITY = {"lc": 0, "lubimyczytac": 0, "ol": 1, "openlibrary": 1}
CONFIDENCE_PRIORITY = {"high": 2, "medium": 1, "low": 0}
INSERT_BATCH_SIZE = 10000
YEAR_TOKEN_RE = re.compile(r"^\d{3,4}(?:[-/]\d{2,4})?$")
NON_WORD_RE = re.compile(r"[\W_]+", re.UNICODE)
NOISE_NAME_TOKENS = {"author", "joint", "unknown", "anonymous", "various"}
CORPORATE_NAME_TOKENS = {
    "academy",
    "administration",
    "agency",
    "assembly",
    "association",
    "authority",
    "board",
    "brake",
    "bureau",
    "center",
    "centre",
    "church",
    "city",
    "college",
    "commission",
    "committee",
    "company",
    "conference",
    "congress",
    "corporation",
    "council",
    "county",
    "court",
    "dept",
    "department",
    "director",
    "division",
    "encyclopedia",
    "encyclopaedia",
    "faculty",
    "federation",
    "foundation",
    "general",
    "government",
    "governor",
    "group",
    "health",
    "hospital",
    "india",
    "industries",
    "institute",
    "judicial",
    "laboratory",
    "land",
    "league",
    "legal",
    "legislative",
    "library",
    "limited",
    "ltd",
    "manufacturing",
    "media",
    "ministry",
    "municipal",
    "museum",
    "music",
    "office",
    "park",
    "planning",
    "popular",
    "press",
    "province",
    "public",
    "publishing",
    "records",
    "record",
    "recrd",
    "reforms",
    "revenue",
    "rural",
    "sales",
    "samiti",
    "scientific",
    "society",
    "state",
    "steam",
    "study",
    "superintendent",
    "tax",
    "team",
    "trust",
    "university",
    "women",
    "workshop",
    "world",
    "writers",
}


def split_joined(raw_value: str) -> list[str]:
    return [part.strip() for part in raw_value.replace(" | ", "|").split("|") if part.strip()]


def compact_source_token(source: str) -> str:
    lowered = (source or "").strip().lower()
    if lowered in {"lubimyczytac", "lc"}:
        return "lc"
    if lowered in {"openlibrary", "ol"}:
        return "ol"
    return lowered


def source_rank(source: str) -> int:
    return SOURCE_PRIORITY.get(compact_source_token(source), 99)


def confidence_rank(confidence: str) -> int:
    return CONFIDENCE_PRIORITY.get((confidence or "").strip().lower(), -1)


def row_sort_key(row: dict[str, str]) -> tuple[int, int, int]:
    return (
        int(row.get("_source_rank", source_rank(row.get("source", "")))),
        -int(row.get("_confidence_rank", confidence_rank(row.get("confidence", "")))),
        -int(row.get("_author_len", len((row.get("author_first_last") or "").strip()))),
    )


def author_text_quality(value: str) -> tuple[int, int, int]:
    tokens = [token for token in infer_core.clean(value).split() if token]
    has_digits = any(any(character.isdigit() for character in token) for token in tokens)
    one_letter_tokens = sum(len(token) == 1 for token in tokens)
    return (
        1 if has_digits else 0,
        one_letter_tokens,
        -len(value),
    )


def build_value_priority(row: dict[str, str], field: str) -> tuple[int, ...]:
    value = (row.get(field) or "").strip()
    source_value, confidence_value, author_len = row_sort_key(row)
    if field in AUTHOR_NAME_FIELDS:
        text_quality = author_text_quality(value)
        return ("..." in value, text_quality[0], text_quality[1], source_value, confidence_value, text_quality[2])
    return (source_value, confidence_value, author_len, 0)


def choose_better_value(current: tuple[tuple[int, ...], str] | None, row: dict[str, str], field: str) -> tuple[tuple[int, ...], str] | None:
    value = (row.get(field) or "").strip()
    if not value:
        return current
    candidate = (build_value_priority(row, field), value)
    if current is None or candidate[0] < current[0]:
        return candidate
    return current


def merge_joined_value(
    seen: set[str],
    parts: list[str],
    row: dict[str, str],
    field: str,
    *,
    pair_with_source: bool = False,
) -> None:
    raw_value = (row.get(field) or "").strip()
    if not raw_value:
        return
    values = split_joined(raw_value)
    if not pair_with_source:
        for value in values:
            if value not in seen:
                seen.add(value)
                parts.append(value)
        return

    source_values = [compact_source_token(value) for value in split_joined((row.get("source") or "").strip())]
    for value in values:
        if ":" in value:
            combined = value
        elif len(source_values) == 1 and source_values[0]:
            combined = f"{source_values[0]}:{value}"
        else:
            combined = value
        if combined not in seen:
            seen.add(combined)
            parts.append(combined)


def build_group_key(row: dict[str, str]) -> str:
    for field in ("author_first_last", "author_last_first", "author_raw"):
        value = (row.get(field) or "").strip()
        if value:
            key = infer_core.author_key(value)
            if key:
                return key
    return ""


def compact_source_value(source: str) -> str:
    parts = sorted({compact_source_token(part) for part in split_joined(source)}, key=source_rank)
    return " | ".join(part for part in parts if part)


def compact_author_raw(author_raw: str, author_first_last: str, author_last_first: str) -> str:
    raw = (author_raw or "").strip()
    if raw in {"", author_first_last.strip(), author_last_first.strip()}:
        return ""
    return raw


def strip_name_noise(value: str) -> str:
    tokens = [token for token in infer_core.clean(value).split() if token]
    cleaned_tokens: list[str] = []
    for token in tokens:
        normalized = token.strip("()[]{}.,;:")
        if not normalized:
            continue
        year_candidate = normalized.strip("-")
        if YEAR_TOKEN_RE.match(year_candidate):
            continue
        if normalized.lower() in NOISE_NAME_TOKENS:
            continue
        cleaned_tokens.append(normalized)
    return " ".join(cleaned_tokens)


def is_plausible_author_name(value: str) -> bool:
    cleaned = strip_name_noise(value)
    tokens = [token for token in cleaned.split() if token]
    if not 2 <= len(tokens) <= 6:
        return False

    letter_token_count = 0
    total_letters = 0
    digit_count = 0
    for token in tokens:
        stripped = NON_WORD_RE.sub("", token)
        if not stripped:
            continue
        letters = sum(character.isalpha() for character in stripped)
        digits = sum(character.isdigit() for character in stripped)
        if letters:
            letter_token_count += 1
            total_letters += letters
        digit_count += digits

    if letter_token_count < 2:
        return False
    if total_letters < 4:
        return False
    lowered_tokens = {token.lower() for token in tokens}
    if lowered_tokens and lowered_tokens.issubset(NOISE_NAME_TOKENS):
        return False
    corporate_hits = sum(token.lower() in CORPORATE_NAME_TOKENS for token in tokens)
    if corporate_hits >= 1 and len(tokens) >= 3:
        return False
    if digit_count > 0:
        return False
    return True


def init_db(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.execute("PRAGMA temp_store = FILE")
    connection.execute(
        """
        CREATE TABLE rows (
            group_key TEXT NOT NULL,
            source TEXT NOT NULL,
            author_raw TEXT NOT NULL,
            author_first_last TEXT NOT NULL,
            author_last_first TEXT NOT NULL,
            source_rank INTEGER NOT NULL,
            confidence_rank INTEGER NOT NULL,
            author_len INTEGER NOT NULL
        )
        """
    )


def ingest_rows(connection: sqlite3.Connection, input_path: Path, *, progress_every: int) -> tuple[int, int]:
    batch: list[tuple[str, str, str, str, str, int, int, int]] = []
    input_rows = 0
    grouped_rows = 0
    skipped_rows = 0
    insert_sql = (
        "INSERT INTO rows (group_key, source, author_raw, author_first_last, author_last_first, "
        "source_rank, confidence_rank, author_len) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            input_rows += 1
            author_raw = strip_name_noise((row.get("author_raw") or "").strip())
            author_first_last = strip_name_noise((row.get("author_first_last") or "").strip())
            author_last_first = strip_name_noise((row.get("author_last_first") or "").strip())
            if not any(
                is_plausible_author_name(candidate)
                for candidate in (author_first_last, author_last_first, author_raw)
                if candidate
            ):
                skipped_rows += 1
                if progress_every > 0 and input_rows % progress_every == 0:
                    print(f"ingest_rows={input_rows}")
                continue
            group_key = build_group_key(
                {
                    "author_raw": author_raw,
                    "author_first_last": author_first_last,
                    "author_last_first": author_last_first,
                }
            )
            if group_key:
                grouped_rows += 1
            source = (row.get("source") or "").strip()
            batch.append(
                (
                    group_key,
                    compact_source_value(source),
                    author_raw,
                    author_first_last,
                    author_last_first,
                    source_rank(source),
                    confidence_rank(row.get("confidence") or ""),
                    len(author_first_last),
                )
            )
            if len(batch) >= INSERT_BATCH_SIZE:
                connection.executemany(insert_sql, batch)
                connection.commit()
                batch.clear()
            if progress_every > 0 and input_rows % progress_every == 0:
                print(f"ingest_rows={input_rows}")
    if batch:
        connection.executemany(insert_sql, batch)
        connection.commit()
    connection.execute(
        "CREATE INDEX idx_rows_group_sort ON rows(group_key, source_rank, confidence_rank DESC, author_len DESC)"
    )
    connection.execute("CREATE INDEX idx_rows_group_key ON rows(group_key)")
    connection.commit()
    return input_rows, grouped_rows, skipped_rows


def finalize_group(rows: list[dict[str, str]]) -> dict[str, str]:
    best_values: dict[str, tuple[tuple[int, ...], str] | None] = {
        "author_raw": None,
        "author_first_last": None,
        "author_last_first": None,
    }
    joined_source = (set(), [])

    for row in rows:
        for field in best_values:
            best_values[field] = choose_better_value(best_values[field], row, field)
        merge_joined_value(*joined_source, row, "source")

    author_first_last = best_values["author_first_last"][1] if best_values["author_first_last"] else ""
    author_last_first = best_values["author_last_first"][1] if best_values["author_last_first"] else ""
    author_raw = best_values["author_raw"][1] if best_values["author_raw"] else ""

    return {
        "source": compact_source_value(" | ".join(joined_source[1])),
        "author_raw": compact_author_raw(author_raw, author_first_last, author_last_first),
        "author_first_last": author_first_last,
        "author_last_first": author_last_first,
    }


def iter_grouped_rows(connection: sqlite3.Connection):
    query = """
        SELECT group_key, source, author_raw, author_first_last, author_last_first,
               source_rank, confidence_rank, author_len
        FROM rows
        WHERE group_key <> ''
        ORDER BY group_key, source_rank ASC, confidence_rank DESC, author_len DESC, rowid ASC
    """
    current_key = ""
    current_group: list[dict[str, str]] = []
    for record in connection.execute(query):
        row = {
            "group_key": record[0],
            "source": record[1],
            "author_raw": record[2],
            "author_first_last": record[3],
            "author_last_first": record[4],
            "_source_rank": str(record[5]),
            "_confidence_rank": str(record[6]),
            "_author_len": str(record[7]),
        }
        group_key = row["group_key"]
        if current_key and group_key != current_key:
            yield finalize_group(current_group)
            current_group = []
        current_key = group_key
        current_group.append(row)
    if current_group:
        yield finalize_group(current_group)


def iter_ungrouped_rows(connection: sqlite3.Connection):
    query = """
        SELECT source, author_raw, author_first_last, author_last_first
        FROM rows
        WHERE group_key = ''
        ORDER BY rowid ASC
    """
    for record in connection.execute(query):
        yield {
            "source": record[0],
            "author_raw": record[1],
            "author_first_last": record[2],
            "author_last_first": record[3],
        }


def merge_to_output(connection: sqlite3.Connection, output_path: Path, *, progress_every: int) -> int:
    output_rows = 0
    temp_output_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with temp_output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in iter_grouped_rows(connection):
            writer.writerow(row)
            output_rows += 1
            if progress_every > 0 and output_rows % progress_every == 0:
                print(f"merged_rows_written={output_rows}")
        for row in iter_ungrouped_rows(connection):
            writer.writerow(row)
            output_rows += 1
            if progress_every > 0 and output_rows % progress_every == 0:
                print(f"merged_rows_written={output_rows}")
    temp_output_path.replace(output_path)
    return output_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scala author_patterns.csv do jednego wpisu na autora.")
    parser.add_argument("--input", default="author_patterns.csv", help="Wejsciowy CSV.")
    parser.add_argument("--output", default="author_patterns.csv", help="Wyjsciowy CSV.")
    parser.add_argument("--temp-db", default="", help="Opcjonalna sciezka do tymczasowej bazy SQLite.")
    parser.add_argument("--progress-every", type=int, default=500000, help="Co ile rekordow wypisywac postep.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()

    if args.temp_db:
        db_path = Path(args.temp_db).resolve()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(db_path)
        cleanup_db = False
    else:
        temp_dir = tempfile.TemporaryDirectory(prefix="author-merge-", dir=str(output_path.parent))
        db_path = Path(temp_dir.name) / "merge.sqlite3"
        connection = sqlite3.connect(db_path)
        cleanup_db = True

    try:
        init_db(connection)
        input_rows, grouped_rows, skipped_rows = ingest_rows(connection, input_path, progress_every=args.progress_every)
        output_rows = merge_to_output(connection, output_path, progress_every=args.progress_every)
    finally:
        connection.close()
        if cleanup_db:
            temp_dir.cleanup()

    print(f"input_rows={input_rows}")
    print(f"grouped_rows={grouped_rows}")
    print(f"skipped_rows={skipped_rows}")
    print(f"merged_rows={output_rows}")
    print(f"collapsed={input_rows - output_rows}")
    print(f"db_path={db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
