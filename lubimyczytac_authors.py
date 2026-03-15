from __future__ import annotations

import argparse
import html
import json
import re
import urllib.parse
from typing import Any

import app_runtime as runtime


BOOK_URL_RE = re.compile(r"^https?://(?:www\.)?lubimyczytac\.pl/ksiazka/\d+(?:/[^/?#]+)?/?$", re.IGNORECASE)
AUTHOR_LINK_RE = re.compile(r'<a[^>]+href=["\']/autor/[^"\']+["\'][^>]*>\s*([^<]+?)\s*</a>', re.IGNORECASE)
TITLE_PATTERNS = (
    re.compile(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', re.IGNORECASE),
    re.compile(r"<h1[^>]*class=[\"'][^\"']*book__title[^\"']*[\"'][^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<h1[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL),
)


def clean_text(text: str | None) -> str:
    return runtime.clean(runtime.strip_html_tags(html.unescape(text or "")))


def is_book_url(value: str) -> bool:
    return bool(BOOK_URL_RE.match(value.strip()))


def parse_book_page(page: str) -> dict[str, Any]:
    title = ""
    for pattern in TITLE_PATTERNS:
        match = pattern.search(page)
        if not match:
            continue
        title = clean_text(match.group(1))
        title = re.sub(r"\s*[-|]\s*Lubimyczytac.*$", "", title, flags=re.IGNORECASE)
        if title:
            break

    authors: list[str] = []
    seen: set[str] = set()
    for match in AUTHOR_LINK_RE.finditer(page):
        author = clean_text(match.group(1))
        if not author:
            continue
        key = runtime.author_key(author)
        if key and key not in seen:
            seen.add(key)
            authors.append(author)

    return {"title": title, "authors": authors}


def fetch_book_authors(url: str, timeout: float) -> dict[str, Any]:
    page = runtime.online_text_query(url, timeout)
    if not page:
        raise RuntimeError(f"Brak odpowiedzi dla URL: {url}")
    payload = parse_book_page(page)
    payload["url"] = url
    return payload


def search_book_authors(phrase: str, timeout: float, *, limit: int) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode({"phrase": phrase})
    url = f"https://lubimyczytac.pl/szukaj/ksiazki?{params}"
    page = runtime.online_text_query(url, timeout)
    if not page:
        raise RuntimeError(f"Brak odpowiedzi dla wyszukiwania: {phrase}")

    parser = runtime.LubimyczytacSearchParser()
    parser.feed(page)
    parser.close()

    results: list[dict[str, Any]] = []
    for item in parser.results[: max(1, limit)]:
        results.append(
            {
                "title": item.title,
                "authors": list(item.authors),
                "url": urllib.parse.urljoin("https://lubimyczytac.pl", item.url),
            }
        )
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Wyciaga autorow ksiazki lub wynikow wyszukiwania z lubimyczytac.pl.")
    parser.add_argument("input", help="URL ksiazki z Lubimyczytac albo zwykla fraza do wyszukania.")
    parser.add_argument("--timeout", type=float, default=8.0, help="Timeout dla zapytania HTTP.")
    parser.add_argument("--limit", type=int, default=5, help="Limit wynikow dla wyszukiwania frazy.")
    parser.add_argument("--json", action="store_true", help="Wypisz wynik jako JSON.")
    return parser


def format_text_output(payload: dict[str, Any] | list[dict[str, Any]]) -> str:
    if isinstance(payload, dict):
        authors = ", ".join(payload.get("authors", [])) or "(brak autorow)"
        title = payload.get("title") or "(brak tytulu)"
        url = payload.get("url") or ""
        return f"{title}\nAutorzy: {authors}\n{url}".strip()

    lines: list[str] = []
    for index, item in enumerate(payload, start=1):
        authors = ", ".join(item.get("authors", [])) or "(brak autorow)"
        title = item.get("title") or "(brak tytulu)"
        url = item.get("url") or ""
        lines.append(f"{index}. {title}")
        lines.append(f"   Autorzy: {authors}")
        if url:
            lines.append(f"   {url}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    runtime.configure_logging()
    args = build_parser().parse_args(argv)
    if is_book_url(args.input):
        payload: dict[str, Any] | list[dict[str, Any]] = fetch_book_authors(args.input, args.timeout)
    else:
        payload = search_book_authors(args.input, args.timeout, limit=args.limit)

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(format_text_output(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
