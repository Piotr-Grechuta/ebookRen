from __future__ import annotations

import argparse
import csv
import html
import re
import time
from pathlib import Path
from typing import Iterable

import requests


LIST_URL = (
    "https://lubimyczytac.pl/autorzy?page={page}"
    "&listId=authorsList&orderBy=booksToReadAmountDesc"
    "&tab=All&showSearch=1&showFirstLetter=0&paginatorType=Standard"
)
AUTHOR_LINK_RE = re.compile(
    r'<a[^>]+class=["\'][^"\']*authorAllBooks__singleTextAuthor[^"\']*["\']'
    r'[^>]+href=["\'](?P<href>/autor/(?P<author_id>\d+)/(?P<slug>[^"\']+))["\']'
    r'[^>]*>(?P<label>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")
SPACE_RE = re.compile(r"\s+")
INITIAL_RE = re.compile(r"^[a-z]$", re.IGNORECASE)
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


def clean_text(value: str) -> str:
    return SPACE_RE.sub(" ", TAG_RE.sub(" ", html.unescape(value or ""))).strip()


def format_slug_token(token: str) -> str:
    if not token:
        return ""
    if token.lower() in PARTICLES:
        return token.lower()
    return token.capitalize()


def slug_to_first_last(slug: str) -> str:
    raw_tokens = [token for token in slug.strip("/").split("-") if token]
    words: list[str] = []
    index = 0
    while index < len(raw_tokens):
        token = raw_tokens[index]
        if INITIAL_RE.match(token):
            initials: list[str] = []
            while index < len(raw_tokens) and INITIAL_RE.match(raw_tokens[index]):
                initials.append(raw_tokens[index].upper())
                index += 1
            words.append(".".join(initials) + ".")
            continue
        words.append(format_slug_token(token))
        index += 1
    return " ".join(part for part in words if part).strip()


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


def iter_author_rows(page_html: str, *, page_number: int) -> Iterable[dict[str, str]]:
    for match in AUTHOR_LINK_RE.finditer(page_html):
        author_id = match.group("author_id")
        slug = match.group("slug")
        label = clean_text(match.group("label"))
        first_last = slug_to_first_last(slug)
        last_first = label if "..." not in label else first_last_to_last_first(first_last)
        yield {
            "source": "lubimyczytac",
            "author_raw": label or first_last,
            "author_first_last": first_last,
            "author_last_first": last_first,
            "title_example": "",
            "language": "pl",
            "source_author_id": author_id,
            "source_work_id": "",
            "source_url": f"https://lubimyczytac.pl/autor/{author_id}/{slug}",
            "confidence": "medium",
            "notes": f"authors-list-html page={page_number}",
        }


def fetch_list_page(
    session: requests.Session,
    page_number: int,
    timeout: float,
    retries: int,
    sleep_seconds: float,
) -> tuple[int, str]:
    delay = max(sleep_seconds, 0.5)
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = session.get(LIST_URL.format(page=page_number), timeout=timeout)
            if response.status_code == 404:
                return response.status_code, response.text
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", "").strip()
                if retry_after.isdigit():
                    time.sleep(max(float(retry_after), delay))
                else:
                    time.sleep(delay)
                    delay = min(delay * 2.0, 30.0)
                last_error = requests.HTTPError(f"429 for page {page_number}", response=response)
                continue
            response.raise_for_status()
            return response.status_code, response.text
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(delay)
            delay = min(delay * 2.0, 30.0)
    if last_error is None:
        raise RuntimeError(f"Nie udalo sie pobrac page={page_number}")
    raise last_error


def read_existing_non_lubimyczytac_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [row for row in reader if (row.get("source") or "").strip().lower() != "lubimyczytac"]


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def collect_author_rows(
    *,
    fetch_page,
    start_page: int,
    end_page: int | None,
    max_pages_without_new_authors: int,
    sleep_seconds: float = 0.0,
) -> tuple[list[dict[str, str]], list[tuple[int, int, int, int]]]:
    collected_rows: list[dict[str, str]] = []
    seen_author_ids: set[str] = set()
    page_stats: list[tuple[int, int, int, int]] = []
    stale_pages = 0
    page_number = start_page

    while end_page is None or page_number <= end_page:
        status_code, page_html = fetch_page(page_number)
        if status_code == 404:
            page_stats.append((page_number, status_code, 0, 0))
            break
        page_rows = list(iter_author_rows(page_html, page_number=page_number))
        new_rows = 0
        for row in page_rows:
            author_id = row["source_author_id"]
            if author_id in seen_author_ids:
                continue
            seen_author_ids.add(author_id)
            collected_rows.append(row)
            new_rows += 1
        page_stats.append((page_number, status_code, len(page_rows), new_rows))
        if end_page is None:
            stale_pages = stale_pages + 1 if new_rows == 0 else 0
            if stale_pages >= max_pages_without_new_authors:
                break
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        page_number += 1

    return collected_rows, page_stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pobiera liste autorow z Lubimyczytac i zapisuje do CSV.")
    parser.add_argument("--output", default="author_patterns.csv", help="Sciezka do pliku wyjsciowego CSV.")
    parser.add_argument("--start-page", type=int, default=1, help="Pierwsza strona do pobrania.")
    parser.add_argument("--end-page", type=int, default=None, help="Opcjonalna ostatnia strona do pobrania.")
    parser.add_argument(
        "--max-pages-without-new-authors",
        type=int,
        default=2,
        help="Po ilu kolejnych stronach bez nowych autorow zatrzymac crawl bez limitu koncowej strony.",
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="Timeout pojedynczego requestu.")
    parser.add_argument("--sleep", type=float, default=0.2, help="Odstep miedzy requestami do listy.")
    parser.add_argument("--retries", type=int, default=6, help="Liczba ponowien po 429 lub bledzie sieci.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_path = Path(args.output).resolve()
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    preserved_rows = read_existing_non_lubimyczytac_rows(output_path)
    collected_rows, page_stats = collect_author_rows(
        fetch_page=lambda page_number: fetch_list_page(session, page_number, args.timeout, args.retries, args.sleep),
        start_page=args.start_page,
        end_page=args.end_page,
        max_pages_without_new_authors=args.max_pages_without_new_authors,
        sleep_seconds=args.sleep,
    )

    all_rows = collected_rows + preserved_rows
    write_rows(output_path, all_rows)

    non_empty_pages = sum(1 for _, _, count, _ in page_stats if count > 0)
    print(f"output={output_path}")
    print(f"lubimyczytac_rows={len(collected_rows)}")
    print(f"preserved_rows={len(preserved_rows)}")
    print(f"pages_with_results={non_empty_pages}/{len(page_stats)}")
    if page_stats:
        print(f"page_{page_stats[0][0]}_rows={page_stats[0][2]}")
        print(f"page_{page_stats[-1][0]}_status={page_stats[-1][1]}")
        print(f"page_{page_stats[-1][0]}_rows={page_stats[-1][2]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
