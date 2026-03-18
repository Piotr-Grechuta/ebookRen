from __future__ import annotations

import csv
import pickle
import re
from dataclasses import dataclass
from pathlib import Path

import infer_core


SOURCE_PRIORITY = {
    "lc": 0,
    "lubimyczytac": 0,
    "lc | ol": 1,
    "ol | lc": 1,
    "lubimyczytac | openlibrary": 1,
    "openlibrary | lubimyczytac": 1,
    "ol": 2,
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
NAME_PARTICLES = {
    "al",
    "bin",
    "da",
    "de",
    "del",
    "della",
    "der",
    "di",
    "du",
    "ibn",
    "la",
    "le",
    "san",
    "st",
    "st.",
    "van",
    "von",
}
CACHE_VERSION = 2


@dataclass(frozen=True)
class AuthorCatalog:
    aliases_to_canonical: dict[str, str]
    initials_to_canonical: dict[str, tuple[str, ...]]
    first_token_keys: frozenset[str]
    last_token_keys: frozenset[str]

    def resolve(self, text: str | None) -> str:
        normalized_text = _normalize_catalog_name(text or "")
        if not normalized_text:
            return ""

        candidates: list[str] = [normalized_text]
        reordered_text = _normalize_catalog_name(infer_core.to_last_first(normalized_text))
        if reordered_text and reordered_text not in candidates:
            candidates.append(reordered_text)

        resolved: list[str] = []
        for candidate in candidates:
            canonical = self.aliases_to_canonical.get(infer_core.author_key(candidate), "")
            if not canonical:
                continue
            preferred = _preferred_query_spelling(candidate, canonical)
            if preferred and preferred not in resolved:
                resolved.append(preferred)
        if not resolved:
            return ""
        return max(resolved, key=lambda candidate: (_candidate_input_score(candidate), len(candidate)))

    def is_known(self, text: str | None) -> bool:
        return bool(self.resolve(text))

    def split_prefix(self, text: str | None, *, min_tokens: int = 2, max_tokens: int = 5) -> tuple[str, str] | None:
        tokens = [token for token in infer_core.clean(text).split() if token]
        if len(tokens) <= min_tokens:
            return None
        upper_bound = min(max_tokens, len(tokens) - 1)
        for size in range(upper_bound, min_tokens - 1, -1):
            author_tokens = tuple(tokens[:size])
            if not self._tokens_may_be_author(author_tokens):
                continue
            author_hint = " ".join(author_tokens)
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
            author_tokens = tuple(tokens[-size:])
            if not self._tokens_may_be_author(author_tokens):
                continue
            title_hint = infer_core.clean(" ".join(tokens[:-size]))
            author_hint = " ".join(author_tokens)
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
            resolved_parts = [self.resolve(part) if self._text_may_be_author(part) else "" for part in parts]
            if parts and len(parts) <= max_authors and all(value and _is_substantial_author_name(value) for value in resolved_parts):
                return _dedupe_authors(resolved_parts)
            partial_resolved = [value for value in resolved_parts if value and _is_substantial_author_name(value)]
            if len(partial_resolved) >= 2:
                return _dedupe_authors(partial_resolved)

        comma_parts = [infer_core.clean(part) for part in re.split(r"\s*,\s*", cleaned) if infer_core.clean(part)]
        if len(comma_parts) >= 2:
            resolved_parts = [self.resolve(part) if self._text_may_be_author(part) else "" for part in comma_parts]
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
                author_tokens = tuple(tokens[start:end])
                if not self._tokens_may_be_author(author_tokens):
                    continue
                candidate = " ".join(author_tokens)
                canonical = self.resolve(candidate)
                if canonical and _is_substantial_author_name(canonical):
                    spans_by_start.setdefault(start, []).append((end, canonical))

        best = _best_author_path(spans_by_start, len(tokens), max_authors=max_authors)
        return _dedupe_authors(best)

    def _text_may_be_author(self, text: str | None) -> bool:
        tokens = tuple(token for token in infer_core.clean(text).split() if token)
        return self._tokens_may_be_author(tokens)

    def _tokens_may_be_author(self, tokens: tuple[str, ...]) -> bool:
        if not 2 <= len(tokens) <= 5:
            return False
        first_token_key = infer_core.author_key(tokens[0])
        last_token_key = infer_core.author_key(tokens[-1])
        if first_token_key and first_token_key in self.first_token_keys:
            pass
        elif last_token_key and last_token_key in self.last_token_keys:
            pass
        else:
            return False
        initials_key = _initials_key_from_tokens(tokens)
        if not initials_key:
            return False
        return initials_key in self.initials_to_canonical


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


def _name_tokens(name: str) -> tuple[str, ...]:
    return tuple(token for token in _normalize_catalog_name(name).split() if token)


def _can_prefer_query_spelling(query: str, canonical: str) -> bool:
    query_tokens = _name_tokens(query)
    canonical_tokens = _name_tokens(canonical)
    if len(query_tokens) < 2 or len(query_tokens) >= len(canonical_tokens):
        return False
    if infer_core.author_key(query) != infer_core.author_key(canonical):
        return False

    query_index = 0
    canonical_index = 0
    merged_after_first = False
    while query_index < len(query_tokens) and canonical_index < len(canonical_tokens):
        query_key = infer_core.author_key(query_tokens[query_index])
        if not query_key:
            return False
        if query_key == infer_core.author_key(canonical_tokens[canonical_index]):
            query_index += 1
            canonical_index += 1
            continue
        if query_index == 0:
            return False
        merged_key = infer_core.author_key(canonical_tokens[canonical_index])
        end = canonical_index
        while end + 1 < len(canonical_tokens):
            if canonical_tokens[end].lower() in NAME_PARTICLES or canonical_tokens[end + 1].lower() in NAME_PARTICLES:
                return False
            end += 1
            merged_key += infer_core.author_key(canonical_tokens[end])
            if merged_key == query_key:
                merged_after_first = True
                query_index += 1
                canonical_index = end + 1
                break
        else:
            return False
    return merged_after_first and query_index == len(query_tokens) and canonical_index == len(canonical_tokens)


def _preferred_query_spelling(query: str, canonical: str) -> str:
    candidates: list[str] = []
    normalized_query = _normalize_catalog_name(query)
    if normalized_query:
        candidates.append(normalized_query)
        reordered_query = _normalize_catalog_name(infer_core.to_last_first(normalized_query))
        if reordered_query and reordered_query not in candidates:
            candidates.append(reordered_query)

    preferred = [candidate for candidate in candidates if _can_prefer_query_spelling(candidate, canonical)]
    if not preferred:
        return canonical
    return max(preferred, key=lambda candidate: (_candidate_input_score(candidate), len(candidate)))


def _initials_key_from_tokens(tokens: tuple[str, ...]) -> str:
    initials: list[str] = []
    for token in tokens:
        folded = infer_core.fold_text(token).lower()
        letters = [character for character in folded if character.isalpha()]
        if not letters:
            return ""
        initials.append(letters[0])
    return "".join(initials)


def _initials_key(name: str) -> str:
    return _initials_key_from_tokens(_name_tokens(name))


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


def _row_group_key(author_first_last: str, author_last_first: str, author_raw: str) -> str:
    for value in (author_first_last, author_last_first, author_raw):
        group_key = infer_core.author_key(value)
        if group_key:
            return group_key
    return ""


def _canonical_priority(source: str, author_first_last: str, preferred: str) -> tuple[int, int, int]:
    return (
        _source_rank(source),
        -len(author_first_last),
        -len(preferred),
    )


def _alias_priority(source: str, author_first_last: str) -> tuple[int, int]:
    return (
        _source_rank(source),
        -len(author_first_last),
    )


def _catalog_cache_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.catalog-cache.pkl")


def catalog_cache_path(path: Path) -> Path:
    return _catalog_cache_path(path)


def _cache_metadata(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def _empty_catalog() -> AuthorCatalog:
    return AuthorCatalog(
        aliases_to_canonical={},
        initials_to_canonical={},
        first_token_keys=frozenset(),
        last_token_keys=frozenset(),
    )


def _load_catalog_cache(path: Path) -> AuthorCatalog | None:
    cache_path = _catalog_cache_path(path)
    if not cache_path.exists() or not path.exists():
        return None
    try:
        with cache_path.open("rb") as handle:
            payload = pickle.load(handle)
    except (OSError, pickle.PickleError, EOFError, AttributeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("version") != CACHE_VERSION:
        return None
    if payload.get("source_meta") != _cache_metadata(path):
        return None
    catalog = payload.get("catalog")
    if not isinstance(catalog, AuthorCatalog):
        return None
    return catalog


def _save_catalog_cache(path: Path, catalog: AuthorCatalog) -> None:
    cache_path = _catalog_cache_path(path)
    temp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
    payload = {
        "version": CACHE_VERSION,
        "source_meta": _cache_metadata(path),
        "catalog": catalog,
    }
    try:
        with temp_path.open("wb") as handle:
            pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
        temp_path.replace(cache_path)
    except OSError:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass


def load_author_catalog(path: Path) -> AuthorCatalog:
    if not path.exists():
        return _empty_catalog()
    cached = _load_catalog_cache(path)
    if cached is not None:
        return cached

    aliases_to_canonical: dict[str, str] = {}
    initials_to_canonical_sets: dict[str, set[str]] = {}
    first_token_keys: set[str] = set()
    last_token_keys: set[str] = set()

    canonical_by_group: dict[str, str] = {}
    canonical_priority_by_group: dict[str, tuple[int, int, int]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            source = (row.get("source") or "").strip()
            author_first_last = infer_core.clean(row.get("author_first_last") or "")
            author_last_first = infer_core.clean(row.get("author_last_first") or "")
            author_raw = infer_core.clean(row.get("author_raw") or "")
            group_key = _row_group_key(author_first_last, author_last_first, author_raw)
            if not group_key:
                continue
            preferred = _preferred_input_form(author_first_last, author_last_first, author_raw)
            if not preferred:
                continue
            candidate_priority = _canonical_priority(source, author_first_last, preferred)
            current_priority = canonical_priority_by_group.get(group_key)
            current_canonical = canonical_by_group.get(group_key, "")
            if current_priority is None or candidate_priority < current_priority:
                canonical_priority_by_group[group_key] = candidate_priority
                canonical_by_group[group_key] = preferred
            elif candidate_priority == current_priority:
                canonical_by_group[group_key] = _pick_canonical(current_canonical, preferred)

    alias_priority_by_key: dict[str, tuple[int, int]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            source = (row.get("source") or "").strip()
            author_first_last = infer_core.clean(row.get("author_first_last") or "")
            author_last_first = infer_core.clean(row.get("author_last_first") or "")
            author_raw = infer_core.clean(row.get("author_raw") or "")
            group_key = _row_group_key(author_first_last, author_last_first, author_raw)
            if not group_key:
                continue
            canonical = canonical_by_group.get(group_key) or _preferred_input_form(author_first_last, author_last_first, author_raw)
            if not canonical:
                continue
            aliases = {
                _normalize_catalog_name(author_first_last),
                _normalize_catalog_name(author_last_first),
                _normalize_catalog_name(author_raw),
                _normalize_catalog_name(infer_core.to_last_first(author_first_last)),
                _normalize_catalog_name(infer_core.to_last_first(author_raw)),
            }
            row_priority = _alias_priority(source, author_first_last)
            for alias in aliases:
                alias_tokens = _name_tokens(alias)
                if 2 <= len(alias_tokens) <= 5:
                    initials_key = _initials_key_from_tokens(alias_tokens)
                    if initials_key:
                        initials_to_canonical_sets.setdefault(initials_key, set()).add(canonical)
                    first_token_key = infer_core.author_key(alias_tokens[0])
                    last_token_key = infer_core.author_key(alias_tokens[-1])
                    if first_token_key:
                        first_token_keys.add(first_token_key)
                    if last_token_key:
                        last_token_keys.add(last_token_key)
                alias_key = infer_core.author_key(alias)
                if not alias_key:
                    continue
                current_priority = alias_priority_by_key.get(alias_key)
                if current_priority is None or row_priority < current_priority:
                    alias_priority_by_key[alias_key] = row_priority
                    aliases_to_canonical[alias_key] = canonical

    catalog = AuthorCatalog(
        aliases_to_canonical=aliases_to_canonical,
        initials_to_canonical={
            key: tuple(sorted(values, key=lambda value: (len(value), value)))
            for key, values in initials_to_canonical_sets.items()
        },
        first_token_keys=frozenset(first_token_keys),
        last_token_keys=frozenset(last_token_keys),
    )
    _save_catalog_cache(path, catalog)
    return catalog
