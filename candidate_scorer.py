from __future__ import annotations

from typing import Callable

from models_core import Candidate, EpubMetadata, RankedOnlineMatch


Cleaner = Callable[[str | None], str]
SimilarityScore = Callable[[str | None, str | None], float]


def is_probably_polish_metadata(
    meta: EpubMetadata,
    *,
    clean: Cleaner,
    fold_text: Cleaner,
) -> bool:
    text_parts = [meta.title, meta.core]
    text_parts.extend(meta.creators)
    text_parts.extend(meta.subjects)
    raw_text = " | ".join(clean(part) for part in text_parts if clean(part))
    if not raw_text:
        return False
    if any(char in raw_text for char in "ąćęłńóśźżĄĆĘŁŃÓŚŹŻ"):
        return True
    folded = f" {fold_text(raw_text).lower()} "
    markers = (
        " tom ",
        " czesc ",
        " ksiega ",
        " kategoria ",
        " cykl ",
        " powiesc ",
        " opowiadania ",
    )
    return any(marker in folded for marker in markers)


def local_series_candidate_score(
    meta: EpubMetadata,
    candidate: Candidate,
    *,
    clean: Cleaner,
    normalize_match_text: Cleaner,
    similarity_score: SimilarityScore,
    strip_leading_title_index: Cleaner,
    looks_like_author_segment: Callable[[str | None], bool],
    series_source_priorities: dict[str, int],
) -> tuple[int, int, int, int]:
    series_key = normalize_match_text(candidate.series)
    title_probes = [
        clean(meta.title),
        clean(meta.core),
        strip_leading_title_index(meta.title),
        strip_leading_title_index(meta.core),
    ]
    signal = 0
    if series_key and any(series_key and series_key in normalize_match_text(probe) for probe in title_probes if probe):
        signal += 12
    if candidate.title_override:
        candidate_title = clean(candidate.title_override)
        if candidate_title:
            if looks_like_author_segment(candidate_title):
                signal -= 22
            similarity = max(similarity_score(candidate_title, probe) for probe in title_probes if probe) if any(title_probes) else 0.0
            signal += int(similarity * 10)
            if normalize_match_text(candidate_title) == series_key:
                signal -= 10
    if candidate.source.startswith("title:"):
        signal += 6
    if candidate.source.startswith("segment:"):
        signal -= 4
    word_count = len(candidate.series.split())
    if word_count > 6:
        signal -= min(18, (word_count - 6) * 3)
    priority = series_source_priorities.get(candidate.source, candidate.score)
    return (
        priority + signal,
        candidate.score + signal,
        1 if candidate.title_override else 0,
        -word_count,
    )


def local_title_candidate_score(
    meta: EpubMetadata,
    candidate: Candidate,
    selected_series: str,
    *,
    clean: Cleaner,
    normalize_match_text: Cleaner,
    similarity_score: SimilarityScore,
    strip_leading_title_index: Cleaner,
    looks_like_author_segment: Callable[[str | None], bool],
) -> tuple[int, int, int]:
    candidate_title = clean(candidate.title_override)
    if not candidate_title:
        return (-10**9, -10**9, -10**9)
    if looks_like_author_segment(candidate_title):
        return (-10**9, -10**9, -10**9)
    probes = [
        clean(meta.title),
        clean(meta.core),
        strip_leading_title_index(meta.title),
        strip_leading_title_index(meta.core),
    ]
    best_similarity = max((similarity_score(candidate_title, probe) for probe in probes if probe), default=0.0)
    exact = any(normalize_match_text(candidate_title) == normalize_match_text(probe) for probe in probes if probe)
    series_match = normalize_match_text(candidate.series) == normalize_match_text(selected_series)
    return (
        1 if series_match else 0,
        1 if exact else 0,
        int(best_similarity * 100) + candidate.score,
    )


def choose_best_local_series_candidate(
    meta: EpubMetadata,
    candidates: list[Candidate],
    *,
    clean: Cleaner,
    normalize_match_text: Cleaner,
    similarity_score: SimilarityScore,
    strip_leading_title_index: Cleaner,
    looks_like_author_segment: Callable[[str | None], bool],
    series_source_priorities: dict[str, int],
) -> Candidate | None:
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda candidate: local_series_candidate_score(
            meta,
            candidate,
            clean=clean,
            normalize_match_text=normalize_match_text,
            similarity_score=similarity_score,
            strip_leading_title_index=strip_leading_title_index,
            looks_like_author_segment=looks_like_author_segment,
            series_source_priorities=series_source_priorities,
        ),
    )


def choose_best_local_title_candidate(
    meta: EpubMetadata,
    candidates: list[Candidate],
    selected_series: str,
    *,
    clean: Cleaner,
    normalize_match_text: Cleaner,
    similarity_score: SimilarityScore,
    strip_leading_title_index: Cleaner,
    looks_like_author_segment: Callable[[str | None], bool],
) -> Candidate | None:
    title_candidates = [candidate for candidate in candidates if candidate.title_override]
    if not title_candidates:
        return None
    return max(
        title_candidates,
        key=lambda candidate: local_title_candidate_score(
            meta,
            candidate,
            selected_series,
            clean=clean,
            normalize_match_text=normalize_match_text,
            similarity_score=similarity_score,
            strip_leading_title_index=strip_leading_title_index,
            looks_like_author_segment=looks_like_author_segment,
        ),
    )


def online_candidate_provider_bias(
    meta: EpubMetadata,
    *,
    provider_label: str,
    title: str,
    reason: str,
    clean: Cleaner,
    normalize_match_text: Cleaner,
    fold_text: Cleaner,
    strip_leading_title_index: Cleaner,
) -> int:
    polish_bias = is_probably_polish_metadata(meta, clean=clean, fold_text=fold_text)
    bias = 0
    if polish_bias:
        if provider_label == "lubimyczytac":
            bias += 28
            if reason in {"title-author-exact", "title-exact", "title-author-approx"}:
                bias += 10
        elif provider_label in {"google-books", "open-library"}:
            bias -= 6

    meta_title = strip_leading_title_index(meta.title or meta.core)
    if provider_label == "lubimyczytac" and meta_title:
        if normalize_match_text(title) == normalize_match_text(meta_title):
            bias += 8
    return bias


def ranked_online_match_score(
    meta: EpubMetadata,
    match: RankedOnlineMatch,
    *,
    clean: Cleaner,
    normalize_match_text: Cleaner,
    fold_text: Cleaner,
) -> int:
    score = match.score
    if is_probably_polish_metadata(meta, clean=clean, fold_text=fold_text) and "lubimyczytac" in match.providers:
        score += 18
        if len(match.providers) == 1:
            score += 8
        meta_title = normalize_match_text(meta.title or meta.core)
        if meta_title and normalize_match_text(match.title) == meta_title:
            score += 6
    return score


def should_penalize_single_lubimyczytac(
    meta: EpubMetadata,
    best: RankedOnlineMatch,
    *,
    clean: Cleaner,
    fold_text: Cleaner,
) -> bool:
    if len(best.providers) != 1 or best.providers[0] != "lubimyczytac":
        return False
    return not is_probably_polish_metadata(meta, clean=clean, fold_text=fold_text)
