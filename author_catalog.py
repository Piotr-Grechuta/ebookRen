from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path

import infer_core


SOURCE_PRIORITY = {
    "lubimyczytac": 0,
    "lubimyczytac | openlibrary": 1,
    "openlibrary | lubimyczytac": 1,
    "openlibrary": 2,
}
COMMON_GIVEN_NAMES = {
    "adrian",
    "agatha",
    "alan",
    "alina",
    "ann",
    "anna",
    "arthur",
    "cecelia",
    "colleen",
    "dakota",
    "eric",
    "harlan",
    "james",
    "jerry",
    "john",
    "jodi",
    "karen",
    "katarzyna",
    "marie",
    "nicholas",
    "nora",
    "remigiusz",
    "robert",
    "sarah",
    "stephen",
    "tess",
    "victoria",
}


@dataclass(frozen=True)
class AuthorCatalog:
    aliases_to_canonical: dict[str, str]

    def resolve(self, text: str | None) -> str:
        key = infer_core.author_key(text or "")
        return self.aliases_to_canonical.get(key, "")

    def is_known(self, text: str | None) -> bool:
        return bool(self.resolve(text))

    def split_prefix(self, text: str | None, *, min_tokens: int = 2, max_tokens: int = 5) -> tuple[str, str] | None:
        tokens = [token for token in infer_core.clean(text).split() if token]
        if len(tokens) <= min_tokens:
            return None
        upper_bound = min(max_tokens, len(tokens) - 1)
        for size in range(upper_bound, min_tokens - 1, -1):
            author_hint = " ".join(tokens[:size])
            title_hint = infer_core.clean(" ".join(tokens[size:]))
            canonical = self.resolve(author_hint)
            if canonical and title_hint:
                return canonical, title_hint
        return None

    def split_suffix(self, text: str | None, *, min_tokens: int = 2, max_tokens: int = 5) -> tuple[str, str] | None:
        tokens = [token for token in infer_core.clean(text).split() if token]
        if len(tokens) <= min_tokens:
            return None
        upper_bound = min(max_tokens, len(tokens) - 1)
        for size in range(upper_bound, min_tokens - 1, -1):
            title_hint = infer_core.clean(" ".join(tokens[:-size]))
            author_hint = " ".join(tokens[-size:])
            canonical = self.resolve(author_hint)
            if canonical and title_hint:
                return canonical, title_hint
        return None

    def resolve_authors(self, text: str | None, *, max_authors: int = 4) -> list[str]:
        cleaned = infer_core.clean(text)
        if not cleaned:
            return []

        direct = self.resolve(cleaned)
        if direct and _is_substantial_author_name(direct):
            return [direct]

        normalized = cleaned.replace(";", " & ")
        normalized = re.sub(r"\s+(?:and|i)\s+", " & ", normalized, flags=re.IGNORECASE)
        if "&" in normalized:
            parts = [infer_core.clean(part) for part in re.split(r"\s*&\s*", normalized) if infer_core.clean(part)]
            resolved_parts = [self.resolve(part) for part in parts]
            if parts and len(parts) <= max_authors and all(value and _is_substantial_author_name(value) for value in resolved_parts):
                return _dedupe_authors(resolved_parts)
            partial_resolved = [value for value in resolved_parts if value and _is_substantial_author_name(value)]
            if len(partial_resolved) >= 2:
                return _dedupe_authors(partial_resolved)

        comma_parts = [infer_core.clean(part) for part in re.split(r"\s*,\s*", cleaned) if infer_core.clean(part)]
        if len(comma_parts) >= 2:
            resolved_parts = [self.resolve(part) for part in comma_parts]
            if len(comma_parts) <= max_authors and all(value and _is_substantial_author_name(value) for value in resolved_parts):
                return _dedupe_authors(resolved_parts)
            partial_resolved = [value for value in resolved_parts if value and _is_substantial_author_name(value)]
            if len(partial_resolved) >= 2:
                return _dedupe_authors(partial_resolved)

        tokens = [token for token in cleaned.split() if token]
        if len(tokens) < 2:
            return []

        spans_by_start: dict[int, list[tuple[int, str]]] = {}
        for start in range(len(tokens)):
            upper_bound = min(len(tokens), start + 5)
            for end in range(start + 2, upper_bound + 1):
                candidate = " ".join(tokens[start:end])
                canonical = self.resolve(candidate)
                if canonical and _is_substantial_author_name(canonical):
                    spans_by_start.setdefault(start, []).append((end, canonical))

        best = _best_author_path(spans_by_start, len(tokens), max_authors=max_authors)
        return _dedupe_authors(best)


def _dedupe_authors(authors: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for author in authors:
        key = infer_core.author_key(author)
        if key and key not in seen:
            seen.add(key)
            result.append(author)
    return result


def _is_substantial_author_name(name: str) -> bool:
    tokens = [token for token in infer_core.clean(name).split() if token]
    if not 2 <= len(tokens) <= 5:
        return False
    meaningful = [re.sub(r"[\W\d_]+", "", token, flags=re.UNICODE) for token in tokens]
    meaningful = [token for token in meaningful if token]
    if len(meaningful) < 2:
        return False
    return sum(len(token) for token in meaningful) >= 6 and any(len(token) >= 3 for token in meaningful)


def _best_author_path(spans_by_start: dict[int, list[tuple[int, str]]], token_count: int, *, max_authors: int) -> list[str]:
    from functools import lru_cache

    @lru_cache(maxsize=None)
    def solve(index: int, used: int) -> tuple[int, int, tuple[str, ...]]:
        if index >= token_count or used >= max_authors:
            return 0, 0, ()

        best_covered, best_count, best_path = solve(index + 1, used)
        for end, canonical in spans_by_start.get(index, []):
            covered, count, path = solve(end, used + 1)
            candidate = (end - index) + covered, 1 + count, (canonical,) + path
            if candidate[:2] > (best_covered, best_count):
                best_covered, best_count, best_path = candidate
        return best_covered, best_count, best_path

    covered, count, path = solve(0, 0)
    if covered < 2 or count == 0:
        return []
    return list(path)


def _source_rank(value: str) -> int:
    return SOURCE_PRIORITY.get((value or "").strip().lower(), 99)


def _pick_canonical(current: str, candidate: str) -> str:
    if not current:
        return candidate
    if len(candidate) > len(current):
        return candidate
    return current


def _normalize_catalog_name(name: str) -> str:
    return re.sub(r"(?<=\b[^\W\d_])\.(?=\s|$)", "", infer_core.clean(name), flags=re.UNICODE)


def _candidate_input_score(name: str) -> tuple[int, int, int]:
    tokens = [token for token in _normalize_catalog_name(name).split() if token]
    if not tokens:
        return (-10, 0, 0)
    lowered = [token.lower() for token in tokens]
    first_is_given = lowered[0] in COMMON_GIVEN_NAMES
    last_is_given = lowered[-1] in COMMON_GIVEN_NAMES
    initials = sum(1 for token in tokens if len(token) == 1)
    return (
        2 if first_is_given else 0,
        -1 if last_is_given else 0,
        -initials,
    )


def _preferred_input_form(author_first_last: str, author_last_first: str, author_raw: str) -> str:
    candidates = [
        _normalize_catalog_name(author_first_last),
        _normalize_catalog_name(author_raw),
        _normalize_catalog_name(infer_core.to_last_first(author_last_first)),
        _normalize_catalog_name(infer_core.to_last_first(author_raw)),
    ]
    candidates = [candidate for candidate in candidates if candidate]
    if not candidates:
        return ""
    return max(candidates, key=lambda candidate: (_candidate_input_score(candidate), len(candidate)))


def load_author_catalog(path: Path) -> AuthorCatalog:
    aliases_to_canonical: dict[str, str] = {}
    if not path.exists():
        return AuthorCatalog(aliases_to_canonical=aliases_to_canonical)

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    rows.sort(key=lambda row: (_source_rank(row.get("source", "")), -len((row.get("author_first_last") or "").strip())))

    canonical_by_group: dict[str, str] = {}
    for row in rows:
        source = (row.get("source") or "").strip()
        author_first_last = infer_core.clean(row.get("author_first_last") or "")
        author_last_first = infer_core.clean(row.get("author_last_first") or "")
        author_raw = infer_core.clean(row.get("author_raw") or "")
        group_key = ""
        for value in (author_first_last, author_last_first, author_raw):
            group_key = infer_core.author_key(value)
            if group_key:
                break
        if not group_key:
            continue
        preferred = _preferred_input_form(author_first_last, author_last_first, author_raw)
        if not preferred:
            continue
        canonical_by_group[group_key] = _pick_canonical(canonical_by_group.get(group_key, ""), preferred)

    for row in rows:
        author_first_last = infer_core.clean(row.get("author_first_last") or "")
        author_last_first = infer_core.clean(row.get("author_last_first") or "")
        author_raw = infer_core.clean(row.get("author_raw") or "")
        group_key = ""
        for value in (author_first_last, author_last_first, author_raw):
            group_key = infer_core.author_key(value)
            if group_key:
                break
        if not group_key:
            continue
        canonical = canonical_by_group.get(group_key) or _preferred_input_form(author_first_last, author_last_first, author_raw)
        aliases = {
            _normalize_catalog_name(author_first_last),
            _normalize_catalog_name(author_last_first),
            _normalize_catalog_name(author_raw),
            _normalize_catalog_name(infer_core.to_last_first(author_first_last)),
            _normalize_catalog_name(infer_core.to_last_first(author_raw)),
        }
        for alias in aliases:
            alias_key = infer_core.author_key(alias)
            if alias_key and alias_key not in aliases_to_canonical:
                aliases_to_canonical[alias_key] = canonical

    return AuthorCatalog(aliases_to_canonical=aliases_to_canonical)
