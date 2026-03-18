from __future__ import annotations

import re
from typing import Callable

from models_core import OnlineSeriesEvidence


AUTHOR_PARTICLES = {
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


def looks_like_structural_title_hint(text: str | None, *, clean) -> bool:
    cleaned = clean(text)
    if not cleaned:
        return True
    allowed_tokens = {
        "book",
        "cykl",
        "czesc",
        "ksiega",
        "part",
        "standalone",
        "tom",
        "vol",
        "volume",
    }
    tokens = [
        re.sub(r"[\W_]+", "", token, flags=re.UNICODE).lower()
        for token in cleaned.split()
    ]
    tokens = [token for token in tokens if token]
    if not tokens:
        return True
    return all(token in allowed_tokens or token.isdigit() for token in tokens)


def is_strong_online_candidate(candidate: object, *, is_online_candidate, clean_series: Callable[[str | None], str]) -> bool:
    series_evidence = online_candidate_series_evidence(candidate, clean_series=clean_series)
    if not is_online_candidate(candidate):
        return False
    if candidate.score < 150 or "ambiguous" in candidate.reason:
        return False
    if candidate.provider != "lubimyczytac":
        return "approx" not in candidate.reason
    return bool(
        candidate.score >= 210
        and (
            series_evidence.series
            or series_evidence.volume is not None
            or candidate.reason in {"isbn-exact", "title-author-exact", "title-exact", "title-author-approx"}
        )
    )


def online_candidate_series_evidence(
    candidate: object,
    *,
    clean_series: Callable[[str | None], str],
) -> OnlineSeriesEvidence:
    series = clean_series(getattr(candidate, "series", ""))
    volume = getattr(candidate, "volume", None)
    cycle_source = str(getattr(candidate, "cycle_source", "")).strip().lower()
    has_series_data = bool(series) or volume is not None
    return OnlineSeriesEvidence(
        series=series,
        volume=volume,
        cycle_source=cycle_source,
        authoritative=has_series_data and cycle_source != "search",
    )


def online_candidate_cycle_is_authoritative(candidate: object, *, clean_series: Callable[[str | None], str]) -> bool:
    return online_candidate_series_evidence(candidate, clean_series=clean_series).authoritative


def author_token_signature(
    value: str | None,
    *,
    clean_author_segment: Callable[[str | None], str],
    normalize_match_text: Callable[[str | None], str],
) -> tuple[str, ...]:
    cleaned_value = clean_author_segment(value)
    tokens = [normalize_match_text(token) for token in cleaned_value.split() if normalize_match_text(token)]
    return tuple(sorted(tokens))


def existing_author_looks_untrusted(
    value: str | None,
    *,
    clean_author_segment: Callable[[str | None], str],
    resolve_author_segment: Callable[[str | None], list[str]],
) -> bool:
    cleaned_value = clean_author_segment(value)
    if not cleaned_value:
        return True
    if resolve_author_segment(cleaned_value):
        return False
    parts = [part for part in cleaned_value.split() if part]
    if not 2 <= len(parts) <= 5:
        return True
    if any(re.search(r"\d", part) for part in parts):
        return True
    meaningful_tokens = [
        re.sub(r"[\W\d_]+", "", part, flags=re.UNICODE)
        for part in parts
        if part.lower().rstrip(".") not in AUTHOR_PARTICLES
    ]
    meaningful_tokens = [token for token in meaningful_tokens if token]
    if len(meaningful_tokens) < 2:
        return True
    return sum(len(token) for token in meaningful_tokens) < 6 or not any(len(token) >= 3 for token in meaningful_tokens)


def should_recover_existing_author_from_title(
    author: str,
    series: str,
    volume: tuple[int, str] | None,
    title_candidate: str,
    resolved_title_authors: list[str],
    *,
    author_token_signature_fn: Callable[[str | None], tuple[str, ...]],
) -> bool:
    return bool(
        author == "Nieznany Autor"
        and (series or "Standalone") == "Standalone"
        and volume == (0, "00")
        and len(resolved_title_authors) == 1
        and author_token_signature_fn(title_candidate) == author_token_signature_fn(resolved_title_authors[0])
    )


def should_reinterpret_existing_trailing_author(
    author: str,
    series: str,
    title_candidate: str,
    resolved_title_authors: list[str],
    hybrid_source: str | None,
    hybrid_author_hint: str,
    *,
    author_token_signature_fn: Callable[[str | None], tuple[str, ...]],
    existing_author_looks_untrusted_fn: Callable[[str | None], bool],
) -> bool:
    return bool(
        str(hybrid_source or "").startswith("hybrid:delimited-index-title-author")
        and series
        and len(resolved_title_authors) == 1
        and author_token_signature_fn(title_candidate) == author_token_signature_fn(resolved_title_authors[0])
        and author_token_signature_fn(hybrid_author_hint) == author_token_signature_fn(resolved_title_authors[0])
        and existing_author_looks_untrusted_fn(author)
    )
