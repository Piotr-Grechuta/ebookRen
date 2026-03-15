from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

import requests


SEARCH_URL = "https://openlibrary.org/search/authors.json"
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


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def fetch_authors_page(
    session: requests.Session,
    query: str,
    offset: int,
    limit: int,
    timeout: float,
    retries: int,
    sleep_seconds: float,
) -> dict:
    delay = max(sleep_seconds, 0.5)
    last_error: Exception | None = None
    params = {"q": query, "limit": limit, "offset": offset}
    for attempt in range(retries + 1):
        try:
            response = session.get(SEARCH_URL, params=params, timeout=timeout)
            if response.status_code == 429:
                time.sleep(delay)
                delay = min(delay * 2.0, 30.0)
                last_error = requests.HTTPError(f"429 for query={query} offset={offset}", response=response)
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(delay)
            delay = min(delay * 2.0, 30.0)
    if last_error is None:
        raise RuntimeError(f"Nie udalo sie pobrac query={query} offset={offset}")
    raise last_error


def build_row(doc: dict, *, query: str, offset: int) -> dict[str, str] | None:
    key = (doc.get("key") or "").strip()
    name = (doc.get("name") or "").strip()
    if not key or not name:
        return None
    author_id = key.split("/")[-1]
    top_work = (doc.get("top_work") or "").strip()
    return {
        "source": "openlibrary",
        "author_raw": name,
        "author_first_last": name,
        "author_last_first": first_last_to_last_first(name),
        "title_example": top_work,
        "language": "en",
        "source_author_id": author_id,
        "source_work_id": "",
        "source_url": f"https://openlibrary.org{key}" if key.startswith("/") else f"https://openlibrary.org/{key}",
        "confidence": "high",
        "notes": f"search-authors-api q={query} offset={offset}",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pobiera autorow z Open Library do CSV.")
    parser.add_argument("--output", default="author_patterns.csv", help="Sciezka do pliku wyjsciowego CSV.")
    parser.add_argument("--queries", default="abcdefghijklmnopqrstuvwxyz", help="Znaki zapytan do search/authors.")
    parser.add_argument("--pages-per-query", type=int, default=5, help="Ile stron po 100 wynikow pobrac dla kazdego zapytania.")
    parser.add_argument("--page-size", type=int, default=100, help="Rozmiar strony API.")
    parser.add_argument("--timeout", type=float, default=20.0, help="Timeout pojedynczego requestu.")
    parser.add_argument("--sleep", type=float, default=0.2, help="Odstep miedzy requestami.")
    parser.add_argument("--retries", type=int, default=6, help="Liczba ponowien po bledzie sieci.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_path = Path(args.output).resolve()
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    preserved_rows = read_existing_non_openlibrary_rows(output_path)
    collected_rows: list[dict[str, str]] = []
    seen_author_ids: set[str] = set()
    request_count = 0

    for query in args.queries:
        for page_index in range(args.pages_per_query):
            offset = page_index * args.page_size
            payload = fetch_authors_page(
                session,
                query,
                offset,
                args.page_size,
                args.timeout,
                args.retries,
                args.sleep,
            )
            docs = payload.get("docs", [])
            request_count += 1
            for doc in docs:
                row = build_row(doc, query=query, offset=offset)
                if row is None:
                    continue
                author_id = row["source_author_id"]
                if author_id in seen_author_ids:
                    continue
                seen_author_ids.add(author_id)
                collected_rows.append(row)
            if args.sleep > 0:
                time.sleep(args.sleep)

    all_rows = preserved_rows + collected_rows
    write_rows(output_path, all_rows)

    print(f"output={output_path}")
    print(f"openlibrary_rows={len(collected_rows)}")
    print(f"preserved_rows={len(preserved_rows)}")
    print(f"requests={request_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
