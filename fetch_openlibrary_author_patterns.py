from __future__ import annotations

import argparse
import csv
import gzip
import json
import time
from io import TextIOWrapper
from pathlib import Path

import requests


DUMP_URL = "https://openlibrary.org/data/ol_dump_authors_latest.txt.gz"
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
PARTICLES = {
    "al",
    "ap",
    "ben",
    "bin",
    "da",
    "dal",
    "de",
    "del",
    "della",
    "der",
    "des",
    "di",
    "dos",
    "du",
    "el",
    "ibn",
    "la",
    "le",
    "st",
    "van",
    "von",
    "y",
}


def first_last_to_last_first(name: str) -> str:
    tokens = [token for token in name.split() if token]
    if len(tokens) <= 1:
        return name
    surname_start = len(tokens) - 1
    while surname_start - 1 >= 1 and tokens[surname_start - 1].lower().rstrip(".") in PARTICLES:
        surname_start -= 1
    surname = tokens[surname_start:]
    given = tokens[:surname_start]
    if not given:
        return name
    return " ".join(surname + given)


def read_existing_non_openlibrary_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [row for row in reader if (row.get("source") or "").strip().lower() != "openlibrary"]


def build_source_url(key: str) -> str:
    normalized = (key or "").strip()
    if not normalized:
        return ""
    if normalized.startswith("/"):
        return f"https://openlibrary.org{normalized}"
    return f"https://openlibrary.org/{normalized.lstrip('/')}"


def build_row_from_dump_record(
    record: dict,
    *,
    fallback_key: str,
    revision: str,
    modified_at: str,
) -> dict[str, str] | None:
    author_key = (record.get("key") or fallback_key or "").strip()
    name = (record.get("name") or record.get("personal_name") or "").strip()
    if not author_key or not name:
        return None
    author_id = author_key.split("/")[-1]
    return {
        "source": "openlibrary",
        "author_raw": name,
        "author_first_last": name,
        "author_last_first": first_last_to_last_first(name),
        "title_example": "",
        "language": "",
        "source_author_id": author_id,
        "source_work_id": "",
        "source_url": build_source_url(author_key),
        "confidence": "high",
        "notes": f"authors-dump revision={revision} modified={modified_at}",
    }


def parse_dump_line(line: str) -> dict[str, str] | None:
    parts = line.rstrip("\n").split("\t", 4)
    if len(parts) != 5:
        return None
    record_type, fallback_key, revision, modified_at, payload = parts
    if record_type.strip() != "/type/author":
        return None
    record = json.loads(payload)
    return build_row_from_dump_record(
        record,
        fallback_key=fallback_key.strip(),
        revision=revision.strip(),
        modified_at=modified_at.strip(),
    )


def open_dump_response(
    session: requests.Session,
    dump_url: str,
    timeout: float,
    retries: int,
    sleep_seconds: float,
) -> requests.Response:
    delay = max(sleep_seconds, 0.5)
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = session.get(dump_url, stream=True, timeout=timeout)
            if response.status_code == 429:
                response.close()
                time.sleep(delay)
                delay = min(delay * 2.0, 30.0)
                last_error = requests.HTTPError("429 while opening Open Library authors dump", response=response)
                continue
            response.raise_for_status()
            response.raw.decode_content = False
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(delay)
            delay = min(delay * 2.0, 30.0)
    if last_error is None:
        raise RuntimeError("Nie udalo sie otworzyc dumpa autorow Open Library")
    raise last_error


def iter_dump_lines(response: requests.Response) -> TextIOWrapper:
    gzip_stream = gzip.GzipFile(fileobj=response.raw)
    return TextIOWrapper(gzip_stream, encoding="utf-8")


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pobiera autorow z Open Library do CSV.")
    parser.add_argument("--output", default="author_patterns.csv", help="Sciezka do pliku wyjsciowego CSV.")
    parser.add_argument("--dump-url", default=DUMP_URL, help="URL do oficjalnego dumpa autorow Open Library.")
    parser.add_argument("--limit-authors", type=int, default=0, help="Opcjonalny limit autorow do pobrania podczas testow.")
    parser.add_argument("--timeout", type=float, default=60.0, help="Timeout pojedynczego requestu.")
    parser.add_argument("--sleep", type=float, default=0.2, help="Odstep po nieudanej probie otwarcia dumpa.")
    parser.add_argument("--retries", type=int, default=6, help="Liczba ponowien po bledzie sieci.")
    parser.add_argument("--progress-every", type=int, default=100000, help="Co ile autorow wypisac postep.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_path = Path(args.output).resolve()
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    preserved_rows = read_existing_non_openlibrary_rows(output_path)
    dump_rows = 0
    skipped_rows = 0

    with temp_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(preserved_rows)
        with open_dump_response(session, args.dump_url, args.timeout, args.retries, args.sleep) as response:
            text_stream = iter_dump_lines(response)
            with text_stream:
                for line_number, line in enumerate(text_stream, start=1):
                    row = parse_dump_line(line)
                    if row is None:
                        skipped_rows += 1
                        continue
                    writer.writerow(row)
                    dump_rows += 1
                    if args.progress_every > 0 and dump_rows % args.progress_every == 0:
                        print(f"openlibrary_rows={dump_rows} line={line_number}")
                    if args.limit_authors > 0 and dump_rows >= args.limit_authors:
                        break

    temp_path.replace(output_path)

    print(f"output={output_path}")
    print(f"openlibrary_rows={dump_rows}")
    print(f"preserved_rows={len(preserved_rows)}")
    print(f"skipped_rows={skipped_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
