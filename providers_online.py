from __future__ import annotations

import html
import re
import urllib.parse
from html.parser import HTMLParser
from typing import Callable


class LubimyczytacSearchParserBase(HTMLParser):
    def __init__(
        self,
        *,
        clean: Callable[[str | None], str],
        clean_series: Callable[[str | None], str],
        parse_volume_parts: Callable[[str | None], tuple[int, str] | None],
        series_only_paren_index_re,
        result_type,
    ) -> None:
        super().__init__()
        self._clean = clean
        self._clean_series = clean_series
        self._parse_volume_parts = parse_volume_parts
        self._series_only_paren_index_re = series_only_paren_index_re
        self._result_type = result_type
        self.results: list[object] = []
        self._capture_title = False
        self._capture_author_block = False
        self._capture_author_name = False
        self._capture_cycle_block = False
        self._capture_cycle_name = False
        self._title_parts: list[str] = []
        self._author_parts: list[str] = []
        self._cycle_parts: list[str] = []
        self._authors: list[str] = []
        self._title_href = ""
        self._series = ""
        self._volume: tuple[int, str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        class_name = attr_map.get("class") or ""
        if tag == "a" and "authorAllBooks__singleTextTitle" in class_name:
            self._flush_current()
            self._capture_title = True
            self._title_parts = []
            self._authors = []
            self._author_parts = []
            self._cycle_parts = []
            self._title_href = self._clean(attr_map.get("href"))
            self._series = ""
            self._volume = None
            return
        if tag == "div" and "authorAllBooks__singleTextAuthor" in class_name:
            self._capture_author_block = True
            self._author_parts = []
            return
        if tag == "div" and "listLibrary__info--cycles" in class_name:
            self._capture_cycle_block = True
            self._cycle_parts = []
            return
        if self._capture_author_block and tag == "a":
            self._capture_author_name = True
            self._author_parts = []
        if self._capture_cycle_block and tag == "a":
            self._capture_cycle_name = True
            self._cycle_parts = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._capture_title:
            self._capture_title = False
            return
        if tag == "a" and self._capture_author_name:
            author = self._clean("".join(self._author_parts))
            if author:
                self._authors.append(author)
            self._capture_author_name = False
            self._author_parts = []
            return
        if tag == "a" and self._capture_cycle_name:
            cycle_text = self._clean("".join(self._cycle_parts))
            if cycle_text:
                series_match = self._series_only_paren_index_re.match(cycle_text)
                if series_match:
                    self._series = self._clean_series(series_match.group(1))
                    self._volume = self._parse_volume_parts(series_match.group(2))
                else:
                    self._series = self._clean_series(cycle_text)
            self._capture_cycle_name = False
            self._cycle_parts = []
            return
        if tag == "div" and self._capture_author_block:
            self._capture_author_block = False
            return
        if tag == "div" and self._capture_cycle_block:
            self._capture_cycle_block = False
            return

    def handle_data(self, data: str) -> None:
        if self._capture_title:
            self._title_parts.append(data)
        elif self._capture_author_name:
            self._author_parts.append(data)
        elif self._capture_cycle_name:
            self._cycle_parts.append(data)

    def close(self) -> None:
        self._flush_current()
        super().close()

    def _flush_current(self) -> None:
        title = self._clean("".join(self._title_parts))
        if title:
            authors = [author for author in self._authors if author]
            self.results.append(
                self._result_type(
                    title=title,
                    authors=authors,
                    series=self._series,
                    volume=self._volume,
                    url=self._title_href,
                    cycle_source="search" if self._series or self._volume is not None else "",
                )
            )
        self._title_parts = []
        self._authors = []
        self._author_parts = []
        self._cycle_parts = []
        self._title_href = ""
        self._series = ""
        self._volume = None


def build_lubimyczytac_search_parser_factory(
    *,
    clean: Callable[[str | None], str],
    clean_series: Callable[[str | None], str],
    parse_volume_parts: Callable[[str | None], tuple[int, str] | None],
    series_only_paren_index_re,
    result_type,
):
    def parser_factory() -> LubimyczytacSearchParserBase:
        return LubimyczytacSearchParserBase(
            clean=clean,
            clean_series=clean_series,
            parse_volume_parts=parse_volume_parts,
            series_only_paren_index_re=series_only_paren_index_re,
            result_type=result_type,
        )

    return parser_factory


def parse_lubimyczytac_detail_page(
    page: str,
    *,
    clean: Callable[[str | None], str],
    strip_html_tags: Callable[[str | None], str],
    clean_series: Callable[[str | None], str],
    parse_volume_parts: Callable[[str | None], tuple[int, str] | None],
    series_only_paren_index_re,
) -> tuple[str, tuple[int, str] | None, list[str]]:
    series = ""
    volume: tuple[int, str] | None = None
    genres: list[str] = []

    cycle_match = re.search(r"Cykl:\s*<a[^>]*>\s*([^<]+?)\s*</a>", page, flags=re.IGNORECASE | re.DOTALL)
    if not cycle_match:
        cycle_match = re.search(
            r"Kategoria:.*?<dt[^>]*>\s*Cykl:\s*</dt>\s*<dd[^>]*>\s*<a[^>]*>\s*([^<]+?)\s*</a>",
            page,
            flags=re.IGNORECASE | re.DOTALL,
        )
    if cycle_match:
        cycle_text = clean(html.unescape(strip_html_tags(cycle_match.group(1))))
        if cycle_text:
            series_match = series_only_paren_index_re.match(cycle_text)
            if series_match:
                series = clean_series(series_match.group(1))
                volume = parse_volume_parts(series_match.group(2))
            else:
                series = clean_series(cycle_text)

    for pattern in (
        r'class="book__category[^"]*"[^>]*>\s*([^<]+?)\s*</a>',
        r"Kategoria:\s*</dt>\s*<dd[^>]*>\s*<a[^>]*>\s*([^<]+?)\s*</a>",
        r'<a[^>]+href="/kategoria/[^"]+"[^>]*>\s*([^<]+?)\s*</a>',
    ):
        for match in re.finditer(pattern, page, flags=re.IGNORECASE | re.DOTALL):
            label = clean(html.unescape(strip_html_tags(match.group(1))))
            if label and label not in genres:
                genres.append(label)
        if genres:
            break

    return series, volume, genres


def enrich_lubimyczytac_result(
    result,
    timeout: float,
    *,
    online_text_query: Callable[[str, float], str | None],
    parse_detail_page: Callable[[str], tuple[str, tuple[int, str] | None, list[str]]],
    result_type,
) -> object:
    if not result.url:
        return result
    detail_url = urllib.parse.urljoin("https://lubimyczytac.pl", result.url)
    page = online_text_query(detail_url, timeout)
    if not page:
        return result
    series, volume, genres = parse_detail_page(page)
    cycle_source = "detail" if series or volume is not None else getattr(result, "cycle_source", "")
    return result_type(
        title=result.title,
        authors=list(result.authors),
        series=series or result.series,
        volume=volume or result.volume,
        url=result.url,
        genres=genres or list(result.genres),
        cycle_source=cycle_source,
    )


def google_books_candidates(
    meta,
    timeout: float,
    *,
    clean,
    extract_isbns,
    online_query,
    build_online_candidates,
) -> list:
    isbns = extract_isbns(meta.identifiers)
    query = ""
    if isbns:
        query = f"isbn:{isbns[0]}"
    else:
        title = clean(meta.title or meta.core)
        author = clean(meta.creators[0] if meta.creators else "")
        if not title:
            return []
        parts = [f'intitle:"{title}"']
        if author:
            parts.append(f'inauthor:"{author}"')
        query = " ".join(parts)

    params = urllib.parse.urlencode(
        {
            "q": query,
            "maxResults": 3,
            "printType": "books",
            "projection": "lite",
        }
    )
    payload = online_query(f"https://www.googleapis.com/books/v1/volumes?{params}", timeout)
    if not payload or not payload.get("items"):
        return []

    candidates: list[tuple[str, list[str], list[str], list[str]]] = []
    for entry in payload.get("items", [])[:10]:
        item = entry.get("volumeInfo", {})
        title = clean(item.get("title"))
        subtitle = clean(item.get("subtitle"))
        if title and subtitle:
            title = f"{title}: {subtitle}"
        authors = [clean(author) for author in item.get("authors") or [] if clean(author)]
        identifiers = [
            clean(identifier.get("identifier"))
            for identifier in item.get("industryIdentifiers", [])
            if clean(identifier.get("identifier"))
        ]
        categories = [clean(category) for category in item.get("categories") or [] if clean(category)]
        candidates.append((title, authors, identifiers, categories))

    return build_online_candidates(meta, "google-books", "google-books", candidates)


def open_library_candidates(
    meta,
    timeout: float,
    *,
    clean,
    extract_isbns,
    online_query,
    build_online_candidates,
) -> list:
    isbns = extract_isbns(meta.identifiers)
    found: list = []
    if isbns:
        params = urllib.parse.urlencode({"bibkeys": f"ISBN:{isbns[0]}", "format": "json", "jscmd": "data"})
        payload = online_query(f"https://openlibrary.org/api/books?{params}", timeout)
        if payload:
            data = payload.get(f"ISBN:{isbns[0]}")
            if data:
                authors = [clean(author.get("name")) for author in data.get("authors", []) if clean(author.get("name"))]
                subjects = [clean(subject.get("name")) for subject in data.get("subjects", []) if clean(subject.get("name"))]
                found.extend(
                    build_online_candidates(
                        meta,
                        "open-library:isbn",
                        "open-library",
                        [(clean(data.get("title")), authors, list(isbns), subjects)],
                    )
                )

    title = clean(meta.title or meta.core)
    if not title:
        return found
    params = urllib.parse.urlencode({"title": title, "limit": 10})
    payload = online_query(f"https://openlibrary.org/search.json?{params}", timeout)
    docs = (payload or {}).get("docs") or []
    if not docs:
        return found

    candidates: list[tuple[str, list[str], list[str]]] = []
    for doc in docs[:10]:
        authors = [clean(name) for name in doc.get("author_name", []) if clean(name)]
        identifiers = [clean(identifier) for identifier in doc.get("isbn", [])[:5] if clean(identifier)]
        candidates.append((clean(doc.get("title")), authors, identifiers))

    found.extend(build_online_candidates(meta, "open-library:search", "open-library", candidates))
    return found


def crossref_candidates(
    meta,
    timeout: float,
    *,
    clean,
    extract_isbns,
    online_query,
    build_online_candidates,
) -> list:
    isbns = extract_isbns(meta.identifiers)
    if not isbns:
        return []

    params = {
        "rows": 10,
        "select": "title,subtitle,author,ISBN,type",
        "query.bibliographic": isbns[0],
    }

    payload = online_query(f"https://api.crossref.org/works?{urllib.parse.urlencode(params)}", timeout)
    items = ((payload or {}).get("message") or {}).get("items") or []
    if not items:
        return []

    candidates: list[tuple[str, list[str], list[str]]] = []
    for item in items[:10]:
        if clean(item.get("type")).lower() not in {"book", "book-chapter", "monograph"}:
            continue
        title_text = clean((item.get("title") or [""])[0])
        subtitle = clean((item.get("subtitle") or [""])[0])
        if title_text and subtitle:
            title_text = f"{title_text}: {subtitle}"
        authors = []
        for author in item.get("author") or []:
            given = clean(author.get("given"))
            family = clean(author.get("family"))
            full = clean(f"{given} {family}")
            if full:
                authors.append(full)
        identifiers = [clean(identifier) for identifier in item.get("ISBN") or [] if clean(identifier)]
        candidates.append((title_text, authors, identifiers))

    return build_online_candidates(meta, "crossref", "crossref", candidates)


def hathitrust_candidates(
    meta,
    timeout: float,
    *,
    clean,
    extract_isbns,
    online_query,
    build_online_candidates,
) -> list:
    isbns = extract_isbns(meta.identifiers)
    if not isbns:
        return []

    payload = online_query(f"https://catalog.hathitrust.org/api/volumes/brief/isbn/{isbns[0]}.json", timeout)
    items = (payload or {}).get("items") or []
    records = (payload or {}).get("records") or {}
    if not items and not records:
        return []

    candidates: list[tuple[str, list[str], list[str]]] = []
    for item in items:
        from_record = records.get(item.get("fromRecord"), {})
        title = clean(from_record.get("title"))
        authors = [clean(author) for author in from_record.get("authors") or [] if clean(author)]
        identifiers = [isbns[0]]
        candidates.append((title, authors, identifiers))

    if not candidates:
        for record in records.values():
            title = clean(record.get("title"))
            authors = [clean(author) for author in record.get("authors") or [] if clean(author)]
            candidates.append((title, authors, list(isbns)))

    return build_online_candidates(meta, "hathitrust", "hathitrust", candidates)


def lubimyczytac_candidates(
    meta,
    timeout: float,
    *,
    extract_isbns,
    build_lubimyczytac_query_terms,
    normalize_match_text,
    online_text_query,
    parser_factory,
    enrich_result,
    rank_online_candidate,
    clean,
    clean_series,
    provider_score_adjustments,
    infer_book_genre,
    candidate_type,
) -> list:
    def resolve_lubimyczytac_genre(labels: list[str]) -> str:
        for label in labels:
            cleaned_label = clean(label)
            if not cleaned_label:
                continue
            return cleaned_label
        return ""

    isbns = extract_isbns(meta.identifiers)
    terms = build_lubimyczytac_query_terms(meta)
    if not terms and isbns:
        terms.append(isbns[0])

    found: list = []
    seen_terms: set[str] = set()
    for term in terms:
        normalized_term = normalize_match_text(term)
        if not normalized_term or normalized_term in seen_terms:
            continue
        seen_terms.add(normalized_term)

        params = urllib.parse.urlencode({"phrase": term})
        url = f"https://lubimyczytac.pl/szukaj/ksiazki?{params}"
        page = online_text_query(url, timeout)
        if not page:
            continue

        candidates: list = []
        parser = parser_factory()
        parser.feed(page)
        parser.close()
        enriched_results: list = []
        for result in parser.results[:12]:
            enriched_results.append(enrich_result(result, timeout))

        for result in enriched_results:
            if not result.authors:
                continue
            score, reason = rank_online_candidate(meta, result.title, result.authors, list(isbns))
            candidates.append(
                candidate_type(
                    provider="lubimyczytac",
                    source="lubimyczytac",
                    title=result.title,
                    authors=[clean(author) for author in result.authors if clean(author)],
                    identifiers=list(isbns),
                    score=score + provider_score_adjustments.get("lubimyczytac", 0),
                    reason=reason,
                    series=clean_series(result.series),
                    volume=result.volume,
                    genre=resolve_lubimyczytac_genre(list(result.genres)),
                    cycle_source=getattr(result, "cycle_source", ""),
                )
            )

        found.extend(candidates)

    return found


def fetch_online_candidates(
    meta,
    providers: list[str],
    timeout: float,
    *,
    online_mode: str = "PL",
    provider_functions: dict[str, Callable],
    emit_provider_progress: Callable[[str, str], None] | None = None,
) -> list:
    def normalize_online_mode(value: str | None) -> str:
        mode = (value or "PL").strip().upper()
        if mode == "PL+":
            return "PL+"
        if mode.startswith("PL"):
            return "PL"
        return "EN"

    def ordered_providers(selected: list[str]) -> list[str]:
        normalized_mode = normalize_online_mode(online_mode)
        if normalized_mode == "PL":
            return ["lubimyczytac"] if "lubimyczytac" in selected else []
        elif normalized_mode == "PL+":
            preferred_order = ["lubimyczytac", "google", "openlibrary", "crossref", "hathitrust"]
        else:
            preferred_order = ["google", "openlibrary", "crossref", "hathitrust", "lubimyczytac"]
        seen: set[str] = set()
        ordered: list[str] = []
        for provider in preferred_order + selected:
            if provider in seen or provider not in selected:
                continue
            seen.add(provider)
            ordered.append(provider)
        return ordered

    def is_strong_candidate(candidate) -> bool:
        reason = getattr(candidate, "reason", "")
        score = int(getattr(candidate, "score", 0) or 0)
        if reason == "isbn-exact" and score >= 420:
            return True
        if reason == "title-author-exact" and score >= 260:
            return True
        if reason == "title-exact" and score >= 280:
            return True
        return False

    all_candidates: list = []
    normalized_mode = normalize_online_mode(online_mode)
    for provider in ordered_providers(providers):
        func = provider_functions.get(provider)
        if func is None:
            continue
        if emit_provider_progress is not None:
            emit_provider_progress(provider, "sprawdza")
        candidates = func(meta, timeout)
        all_candidates.extend(candidates)
        if emit_provider_progress is not None:
            if candidates:
                preview_titles: list[str] = []
                for candidate in candidates[:3]:
                    title = getattr(candidate, "title", "")
                    if title:
                        preview_titles.append(title)
                if preview_titles:
                    outcome = f"proponuje {len(candidates)} wynikow: " + " | ".join(preview_titles)
                else:
                    outcome = f"proponuje {len(candidates)} wynikow"
            else:
                outcome = "brak trafien"
            if any(candidate.reason == "isbn-exact" and candidate.score >= 420 for candidate in candidates):
                outcome = f"{outcome}, trafienie ISBN"
            emit_provider_progress(provider, outcome)
        if any(candidate.reason == "isbn-exact" and candidate.score >= 420 for candidate in candidates):
            break
        if normalized_mode == "PL+" and provider == "lubimyczytac" and any(is_strong_candidate(candidate) for candidate in candidates):
            break
        if normalized_mode == "EN" and provider != "lubimyczytac" and any(is_strong_candidate(candidate) for candidate in candidates):
            break
    return all_candidates
