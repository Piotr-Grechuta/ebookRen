from __future__ import annotations

import dataclasses
import difflib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import infer_core as infer_core_mod
from models_core import EpubMetadata, OnlineCandidate, RankedOnlineMatch


DEVICE_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}

GENRE_SUFFIX_RE = re.compile(r"^(.*?)\s*\[([^\[\]]+)\]\s*$")
BLOCKING_REVIEW_REASONS = {
    "online-niejednoznaczne",
    "online-best-effort",
    "nieznany-autor",
    "brak-tytulu",
    "fallback",
    "szum-w-tytule",
    "artefakt-zrodla",
}
ONLINE_AMBIGUITY_MARGIN = 25


@dataclass
class BookRecord:
    path: Path
    author: str
    series: str
    volume: tuple[int, str] | None
    title: str
    source: str
    identifiers: list[str]
    notes: list[str]
    genre: str = ""
    confidence: int = 0
    review_reasons: list[str] = field(default_factory=list)
    decision_reasons: list[str] = field(default_factory=list)
    filename_suffix: str = ""
    output_folder: Path | None = None
    online_checked: bool = False
    online_applied: bool = False

    @property
    def needs_review(self) -> bool:
        return self.confidence < 65 or any(reason in BLOCKING_REVIEW_REASONS for reason in self.review_reasons)

    @property
    def filename(self) -> str:
        return self._filename_for_folder(self.output_folder or self.path.parent)

    def _filename_for_folder(self, folder: Path) -> str:
        author = infer_core_mod.sanitize_component(self.author, device_names=DEVICE_NAMES)
        series = infer_core_mod.sanitize_component(self.series or "Standalone", device_names=DEVICE_NAMES)
        volume = infer_core_mod.format_volume(self.volume)
        title_for_path = self.title
        if self.filename_suffix:
            title_for_path = infer_core_mod.clean(f"{title_for_path} {self.filename_suffix}")
        title_for_path = infer_core_mod.format_title_with_genre(
            title_for_path,
            self.genre,
            genre_suffix_re=GENRE_SUFFIX_RE,
        )
        title = infer_core_mod.sanitize_component(title_for_path, device_names=DEVICE_NAMES)
        title = infer_core_mod.trim_title_for_path(folder, author, series, volume, title)
        return (
            f"{author} - {series} - {volume} - "
            f"{infer_core_mod.sanitize_component(title, device_names=DEVICE_NAMES)}{self.path.suffix.lower()}"
        )


def author_match_keys(values: Iterable[str]) -> set[str]:
    keys: set[str] = set()
    for value in values:
        cleaned = infer_core_mod.clean(value)
        if not cleaned:
            continue
        for variant in (cleaned, infer_core_mod.to_last_first(cleaned)):
            normalized = infer_core_mod.normalize_match_text(variant)
            if normalized:
                keys.add(normalized)
    return keys


def rank_online_candidate(
    meta: EpubMetadata,
    title: str,
    authors: list[str],
    identifiers: Iterable[str],
    *,
    split_authors,
    provider_score_adjustments: dict[str, int] | None = None,
) -> tuple[int, str]:
    del provider_score_adjustments
    meta_isbns = set(infer_core_mod.extract_isbns(meta.identifiers, isbn_re=re.compile(r"(97[89][0-9]{10}|[0-9]{9}[0-9Xx])")))
    candidate_isbns = set(infer_core_mod.extract_isbns(identifiers, isbn_re=re.compile(r"(97[89][0-9]{10}|[0-9]{9}[0-9Xx])")))
    if meta_isbns and candidate_isbns and meta_isbns.intersection(candidate_isbns):
        return 420, "isbn-exact"

    meta_title = infer_core_mod.clean(meta.title or meta.core)
    title_exact = bool(meta_title) and infer_core_mod.normalize_match_text(meta_title) == infer_core_mod.normalize_match_text(title)
    title_similarity = infer_core_mod.similarity_score(meta_title, title)

    meta_authors: list[str] = []
    for creator in meta.creators:
        meta_authors.extend(split_authors(creator))
    meta_author_keys = author_match_keys(meta_authors)
    candidate_author_keys = author_match_keys(authors)
    author_exact = bool(meta_author_keys and candidate_author_keys and meta_author_keys.intersection(candidate_author_keys))

    author_similarity = 0.0
    for meta_author in meta_author_keys:
        for candidate_author in candidate_author_keys:
            author_similarity = max(author_similarity, difflib.SequenceMatcher(None, meta_author, candidate_author).ratio())

    if title_exact and author_exact:
        return 320, "title-author-exact"
    if title_exact:
        return 250 + int(author_similarity * 40), "title-exact"

    blended = int(title_similarity * 100) + int(author_similarity * 35)
    if title_similarity >= 0.75 and author_similarity < 0.25:
        blended -= 20
    if title_similarity >= 0.82 and (author_exact or author_similarity >= 0.68):
        return 180 + blended, "title-author-approx"
    if title_similarity >= 0.9:
        return 150 + blended, "title-approx"
    return 100 + blended, "approx"


def online_confidence(score: int) -> int:
    if score >= 420:
        return 78
    if score >= 320:
        return 72
    if score >= 250:
        return 66
    if score >= 180:
        return 60
    return 54


def build_online_candidates(
    meta: EpubMetadata,
    source: str,
    provider_label: str,
    candidates: Iterable[tuple[str, list[str], list[str]] | tuple[str, list[str], list[str], list[str]]],
    *,
    provider_score_adjustments: dict[str, int],
    split_authors,
) -> list[OnlineCandidate]:
    ranked: list[OnlineCandidate] = []
    for raw_candidate in candidates:
        if len(raw_candidate) == 4:
            title, authors, identifiers, genres = raw_candidate
            genre = infer_core_mod.infer_book_genre(genres)
        else:
            title, authors, identifiers = raw_candidate
            genre = ""
        cleaned_title = infer_core_mod.clean(title)
        cleaned_authors = [infer_core_mod.clean(author) for author in authors if infer_core_mod.clean(author)]
        cleaned_identifiers = [infer_core_mod.clean(identifier) for identifier in identifiers if infer_core_mod.clean(identifier)]
        if not cleaned_title and not cleaned_authors:
            continue
        score, reason = rank_online_candidate(meta, cleaned_title, cleaned_authors, cleaned_identifiers, split_authors=split_authors)
        score += provider_score_adjustments.get(provider_label, 0)
        ranked.append(
            OnlineCandidate(
                provider=provider_label,
                source=source,
                title=cleaned_title,
                authors=cleaned_authors,
                identifiers=cleaned_identifiers,
                score=score,
                reason=reason,
                series="",
                volume=None,
                genre=genre,
            )
        )
    return ranked


def online_candidate_group_key(candidate: OnlineCandidate) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    identifiers = tuple(sorted(infer_core_mod.extract_isbns(candidate.identifiers, isbn_re=re.compile(r"(97[89][0-9]{10}|[0-9]{9}[0-9Xx])"))))
    title_key = infer_core_mod.normalize_match_text(candidate.title)
    author_keys = tuple(sorted(author_match_keys(candidate.authors)))
    return title_key, author_keys, identifiers


def is_online_candidate(candidate: object) -> bool:
    return all(
        hasattr(candidate, attribute)
        for attribute in ("provider", "source", "title", "authors", "identifiers", "score", "reason")
    )


def aggregate_online_candidates(candidates: Iterable[OnlineCandidate]) -> list[RankedOnlineMatch]:
    grouped: dict[tuple[str, tuple[str, ...], tuple[str, ...]], list[OnlineCandidate]] = {}
    for candidate in candidates:
        if not is_online_candidate(candidate):
            continue
        key = online_candidate_group_key(candidate)
        grouped.setdefault(key, []).append(candidate)

    aggregated: list[RankedOnlineMatch] = []
    for group in grouped.values():
        providers: list[str] = []
        sources: list[str] = []
        identifiers: list[str] = []
        best = max(group, key=lambda item: item.score)
        series = ""
        volume: tuple[int, str] | None = None
        genre = ""
        for item in group:
            if item.provider not in providers:
                providers.append(item.provider)
            if item.source not in sources:
                sources.append(item.source)
            for identifier in item.identifiers:
                normalized = infer_core_mod.clean(identifier)
                if normalized and normalized not in identifiers:
                    identifiers.append(normalized)
            if not series and infer_core_mod.clean_series(item.series):
                series = infer_core_mod.clean_series(item.series)
            if volume is None and item.volume is not None:
                volume = item.volume
            if not genre and infer_core_mod.clean(item.genre):
                genre = infer_core_mod.clean(item.genre)

        aggregate_score = best.score + max(0, (len(providers) - 1) * 25)
        aggregate_reason = best.reason if len(providers) == 1 else f"{best.reason}+consensus"
        aggregated.append(
            RankedOnlineMatch(
                providers=providers,
                sources=sources,
                title=best.title,
                authors=best.authors,
                identifiers=identifiers,
                score=aggregate_score,
                reason=aggregate_reason,
                series=series,
                volume=volume,
                genre=genre,
            )
        )

    aggregated.sort(key=lambda item: item.score, reverse=True)
    return aggregated


def pick_best_online_match(meta: EpubMetadata, candidates: Iterable[OnlineCandidate]) -> RankedOnlineMatch | None:
    del meta
    aggregated = aggregate_online_candidates(candidates)
    if not aggregated:
        return None
    best = aggregated[0]
    if best.score < 140 and not best.title:
        return None
    if len(aggregated) > 1 and best.score < 420:
        second = aggregated[1]
        if best.score - second.score < ONLINE_AMBIGUITY_MARGIN:
            best = RankedOnlineMatch(
                providers=best.providers,
                sources=best.sources,
                title=best.title,
                authors=best.authors,
                identifiers=best.identifiers,
                score=max(0, best.score - 10),
                reason=f"{best.reason}+ambiguous",
                series=best.series,
                volume=best.volume,
                genre=best.genre,
            )
    if len(best.providers) == 1 and best.providers[0] == "lubimyczytac" and best.score < 420:
        best = RankedOnlineMatch(
            providers=best.providers,
            sources=best.sources,
            title=best.title,
            authors=best.authors,
            identifiers=best.identifiers,
            score=max(0, best.score - 15),
            reason=f"{best.reason}+best-effort",
            series=best.series,
            volume=best.volume,
            genre=best.genre,
        )
    return best


def build_online_record(meta: EpubMetadata, best: RankedOnlineMatch, *, extract_authors) -> BookRecord:
    provider_text = ",".join(best.providers)
    source_text = ",".join(best.sources)
    review_reasons: list[str] = []
    if "ambiguous" in best.reason:
        review_reasons.append("online-niejednoznaczne")
    if "best-effort" in best.reason:
        review_reasons.append("online-best-effort")
    return BookRecord(
        path=meta.path,
        author=extract_authors(best.authors, ""),
        series=infer_core_mod.clean_series(best.series),
        volume=best.volume,
        title=best.title,
        source=source_text or "online-aggregate",
        identifiers=best.identifiers,
        genre=infer_core_mod.clean(best.genre),
        notes=[
            "online:title-author-only",
            f"online-rank:{best.reason}",
            f"online-score:{best.score}",
            f"online-providers:{provider_text}",
        ],
        confidence=online_confidence(best.score),
        review_reasons=review_reasons,
        decision_reasons=[f"online-candidate:{provider_text}", f"match:{best.reason}"],
        online_checked=True,
    )


def parse_existing_filename(stem: str) -> tuple[str, str, tuple[int, str] | None, str, str] | None:
    def looks_like_existing_author(text: str) -> bool:
        value = infer_core_mod.clean(text)
        if not value or not re.search(r"[A-Za-z]", value):
            return False
        if re.match(r"^[#(\[]?\s*\d", value):
            return False
        if re.fullmatch(r"[IVXLCDM]+", value, flags=re.IGNORECASE):
            return False
        return True

    prefix, sep, title = stem.rpartition(" - ")
    if not sep or not title:
        return None
    clean_title, genre = infer_core_mod.split_title_genre_suffix(title, genre_suffix_re=GENRE_SUFFIX_RE)
    left, sep, last_segment = prefix.rpartition(" - ")
    if not sep:
        return None

    if last_segment.startswith("Tom "):
        volume = infer_core_mod.parse_volume_parts(last_segment)
        if volume is None:
            return None
        author_part, sep, series_part = left.rpartition(" - ")
        if sep and author_part and series_part:
            author = infer_core_mod.clean(author_part)
            series = infer_core_mod.clean_series(series_part)
        else:
            author = infer_core_mod.clean(left)
            series = ""
        if not author or not looks_like_existing_author(author):
            return None
        return author, series, volume, infer_core_mod.clean(clean_title), genre

    author = infer_core_mod.clean(left)
    series = infer_core_mod.clean_series(last_segment)
    if not author or not series or not looks_like_existing_author(author):
        return None
    return author, series, None, infer_core_mod.clean(clean_title), genre


def finalize_record_quality(
    record: BookRecord,
    meta: EpubMetadata,
    base_confidence: int,
    title_from_core: bool,
    *,
    hex_noise_re,
    anna_archive_re,
) -> BookRecord:
    confidence = base_confidence
    review_reasons = list(record.review_reasons)

    if record.author == "Nieznany Autor":
        confidence -= 25
        review_reasons.append("nieznany-autor")
    if not record.title or record.title == "Bez tytulu":
        confidence -= 30
        review_reasons.append("brak-tytulu")
    if title_from_core:
        confidence -= 5
        review_reasons.append("tytul-z-nazwy-pliku")
    if record.source == "fallback":
        confidence -= 15
        review_reasons.append("fallback")
    if meta.errors:
        confidence -= 10
        review_reasons.append("blad-odczytu-metadanych")
    if hex_noise_re.search(record.title):
        confidence -= 20
        review_reasons.append("szum-w-tytule")
    if anna_archive_re.search(record.title):
        confidence -= 20
        review_reasons.append("artefakt-zrodla")

    deduped_reasons: list[str] = []
    seen: set[str] = set()
    for reason in review_reasons:
        if reason not in seen:
            seen.add(reason)
            deduped_reasons.append(reason)

    record.confidence = max(0, min(100, confidence))
    record.review_reasons = deduped_reasons
    return record


def make_record_clone(
    record: BookRecord,
    *,
    title: str | None = None,
    notes: list[str] | None = None,
    confidence: int | None = None,
    review_reasons: list[str] | None = None,
    decision_reasons: list[str] | None = None,
    filename_suffix: str | None = None,
) -> BookRecord:
    return dataclasses.replace(
        record,
        title=title if title is not None else record.title,
        notes=list(notes) if notes is not None else list(record.notes),
        confidence=record.confidence if confidence is None else confidence,
        review_reasons=list(review_reasons) if review_reasons is not None else list(record.review_reasons),
        decision_reasons=list(decision_reasons) if decision_reasons is not None else list(record.decision_reasons),
        filename_suffix=record.filename_suffix if filename_suffix is None else filename_suffix,
        identifiers=list(record.identifiers),
    )
