from __future__ import annotations

import dataclasses
import difflib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import candidate_scorer as candidate_scorer_mod
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
    archive_source_path: Path | None = None
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


def extract_authors_preserving_order(values: Iterable[str]) -> str:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = infer_core_mod.to_last_first(value)
        key = infer_core_mod.author_key(normalized)
        if normalized and key and key not in seen:
            seen.add(key)
            ordered.append(normalized)
    return " & ".join(ordered) if ordered else "Nieznany Autor"


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
        score += candidate_scorer_mod.online_candidate_provider_bias(
            meta,
            provider_label=provider_label,
            title=cleaned_title,
            reason=reason,
            clean=infer_core_mod.clean,
            normalize_match_text=infer_core_mod.normalize_match_text,
            fold_text=infer_core_mod.fold_text,
            strip_leading_title_index=infer_core_mod.clean,
        )
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
        best = max(group, key=lambda item: item.score)
        for item in group:
            if item.provider not in providers:
                providers.append(item.provider)
            if item.source not in sources:
                sources.append(item.source)

        aggregate_score = best.score + max(0, (len(providers) - 1) * 25)
        aggregate_reason = best.reason if len(providers) == 1 else f"{best.reason}+consensus"
        aggregated.append(
            RankedOnlineMatch(
                providers=providers,
                sources=sources,
                title=best.title,
                authors=best.authors,
                identifiers=list(best.identifiers),
                score=aggregate_score,
                reason=aggregate_reason,
                series=infer_core_mod.clean_series(best.series),
                volume=best.volume,
                genre=infer_core_mod.clean(best.genre),
                cycle_source=getattr(best, "cycle_source", ""),
            )
        )

    aggregated.sort(key=lambda item: item.score, reverse=True)
    return aggregated


def pick_best_online_match(meta: EpubMetadata, candidates: Iterable[OnlineCandidate]) -> RankedOnlineMatch | None:
    aggregated = aggregate_online_candidates(candidates)
    if not aggregated:
        return None
    aggregated.sort(
        key=lambda item: candidate_scorer_mod.ranked_online_match_score(
            meta,
            item,
            clean=infer_core_mod.clean,
            normalize_match_text=infer_core_mod.normalize_match_text,
            fold_text=infer_core_mod.fold_text,
        ),
        reverse=True,
    )
    best = aggregated[0]
    if best.score < 140 and not best.title:
        return None
    if len(aggregated) > 1 and best.score < 420:
        second = aggregated[1]
        best_scored = candidate_scorer_mod.ranked_online_match_score(
            meta,
            best,
            clean=infer_core_mod.clean,
            normalize_match_text=infer_core_mod.normalize_match_text,
            fold_text=infer_core_mod.fold_text,
        )
        second_scored = candidate_scorer_mod.ranked_online_match_score(
            meta,
            second,
            clean=infer_core_mod.clean,
            normalize_match_text=infer_core_mod.normalize_match_text,
            fold_text=infer_core_mod.fold_text,
        )
        if best_scored - second_scored < ONLINE_AMBIGUITY_MARGIN:
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
                cycle_source=best.cycle_source,
            )
    if (
        len(best.providers) == 1
        and best.providers[0] == "lubimyczytac"
        and best.score < 420
        and candidate_scorer_mod.should_penalize_single_lubimyczytac(
            meta,
            best,
            clean=infer_core_mod.clean,
            fold_text=infer_core_mod.fold_text,
        )
    ):
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
            cycle_source=best.cycle_source,
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
    author_text = extract_authors([], " & ".join(best.authors))
    if "lubimyczytac" in best.providers:
        author_text = extract_authors_preserving_order(best.authors)
    cycle_source = str(getattr(best, "cycle_source", "")).strip().lower()
    authoritative_series = infer_core_mod.clean_series(best.series)
    authoritative_volume = best.volume
    if cycle_source == "search":
        authoritative_series = ""
        authoritative_volume = None
    return BookRecord(
        path=meta.path,
        author=author_text,
        series=authoritative_series,
        volume=authoritative_volume,
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
    def normalize_existing_author(text: str, *, allow_leading_index: bool = False) -> str:
        value = infer_core_mod.clean(text)
        if not value:
            return ""
        if not allow_leading_index:
            return value
        stripped = infer_core_mod.clean(re.sub(r"^\d{1,4}(?:[.)_-]+|\s+)+", "", value))
        return stripped or value

    def looks_like_existing_author(text: str, *, allow_leading_index: bool = False) -> bool:
        value = normalize_existing_author(text, allow_leading_index=allow_leading_index)
        if not value or not re.search(r"[A-Za-z]", value):
            return False
        words = value.split()
        if len(words) == 1 and len(value) <= 2:
            return False
        if len(words) == 1 and value.lower() == value:
            return False
        if re.match(r"^[#(\[]?\s*\d", value):
            return False
        if re.fullmatch(r"[IVXLCDM]+", value, flags=re.IGNORECASE):
            return False
        return True

    def looks_like_existing_series(text: str, title_text: str) -> bool:
        value = infer_core_mod.clean_series(text)
        if not value:
            return False
        if looks_like_existing_author(value) and infer_core_mod.clean(title_text).startswith("["):
            return False
        return True

    def is_structural_placeholder(text: str) -> bool:
        value = infer_core_mod.clean(text)
        if not value:
            return True
        if infer_core_mod.normalize_match_text(value) == infer_core_mod.normalize_match_text("Standalone"):
            return True
        if re.fullmatch(r"(?:\d+(?:\.\d+)?|[IVXLCDM]+)", value, flags=re.IGNORECASE):
            return True
        tokens = [infer_core_mod.normalize_match_text(token) for token in value.split() if infer_core_mod.normalize_match_text(token)]
        if not tokens:
            return True
        allowed_tokens = {"book", "cykl", "czesc", "ksiega", "part", "standalone", "tom", "vol", "volume"}
        return all(token in allowed_tokens or token.isdigit() for token in tokens)

    def split_embedded_series_volume_title(text: str) -> tuple[str, tuple[int, str], str] | None:
        value = infer_core_mod.clean(text)
        if not value:
            return None
        match = re.match(
            r"^(?P<series>.+?)\s*\(\s*(?:(?:book|tom|volume|vol\.?|czesc|część|ksiega|księga)\s*)?(?P<volume>\d+(?:\.\d+)?|[IVXLCDM]+)\s*\)\s*(?:[-:._]\s*)?(?P<title>.+)$",
            value,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        series = infer_core_mod.clean_series(match.group("series"))
        volume = infer_core_mod.parse_volume_parts(match.group("volume"))
        title = infer_core_mod.clean(match.group("title"))
        if (
            not series
            or volume is None
            or not title
            or len(series.split()) < 2
            or is_structural_placeholder(series)
            or is_structural_placeholder(title)
        ):
            return None
        return series, volume, title

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
        normalized_series = infer_core_mod.clean_series(series_part) if series_part else ""
        allow_prefixed_author_index = bool(normalized_series and normalized_series != "Standalone")
        selected_title = infer_core_mod.clean(clean_title)
        if sep and author_part and series_part:
            author = normalize_existing_author(author_part, allow_leading_index=allow_prefixed_author_index)
            series = normalized_series
            legacy_title_part, legacy_sep, legacy_author_part = author_part.rpartition(" - ")
            normalized_legacy_author = normalize_existing_author(legacy_author_part, allow_leading_index=allow_prefixed_author_index)
            legacy_title = infer_core_mod.clean(legacy_title_part)
            if (
                legacy_sep
                and legacy_title
                and normalized_legacy_author
                and looks_like_existing_author(normalized_legacy_author, allow_leading_index=allow_prefixed_author_index)
            ):
                selected_title_key = infer_core_mod.normalize_match_text(selected_title)
                series_key = infer_core_mod.normalize_match_text(series)
                if (
                    len(selected_title.split()) == 1
                    and len(normalized_legacy_author.split()) == 1
                    and re.search(r"[A-Za-z]", selected_title)
                ):
                    repaired_legacy_title = legacy_title
                    if " & " in repaired_legacy_title:
                        left_title, _, right_title = repaired_legacy_title.rpartition(" & ")
                        if infer_core_mod.clean(right_title) and len(infer_core_mod.clean(right_title).split()) == 1:
                            repaired_legacy_title = infer_core_mod.clean(left_title) or repaired_legacy_title
                    author = infer_core_mod.clean(f"{selected_title} {normalized_legacy_author}")
                    selected_title = repaired_legacy_title
                elif (
                    not selected_title
                    or is_structural_placeholder(selected_title)
                    or selected_title_key == series_key
                    or len(selected_title.split()) < 2 <= len(legacy_title.split())
                ):
                    author = normalized_legacy_author
                    selected_title = legacy_title
        else:
            author = normalize_existing_author(left)
            series = ""
        if (not series or series == "Standalone") and volume == (0, "00"):
            embedded_series = split_embedded_series_volume_title(selected_title)
            if embedded_series is not None:
                series, volume, selected_title = embedded_series
        if not author or not looks_like_existing_author(author, allow_leading_index=allow_prefixed_author_index):
            return None
        if is_structural_placeholder(selected_title) or (series and series != "Standalone" and is_structural_placeholder(series)):
            return None
        return author, series, volume, selected_title, genre

    author = normalize_existing_author(left)
    series = infer_core_mod.clean_series(last_segment)
    normalized_title = infer_core_mod.normalize_match_text(clean_title)
    if (
        author
        and looks_like_existing_author(author)
        and normalized_title == infer_core_mod.normalize_match_text("Standalone")
        and infer_core_mod.clean(last_segment)
        and infer_core_mod.normalize_match_text(last_segment) != infer_core_mod.normalize_match_text("Standalone")
    ):
        return author, "Standalone", None, infer_core_mod.clean(last_segment), genre
    if (
        not author
        or not series
        or not looks_like_existing_author(author)
        or not looks_like_existing_series(series, clean_title)
        or is_structural_placeholder(series)
        or is_structural_placeholder(clean_title)
    ):
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
    author: str | None = None,
    series: str | None = None,
    volume: tuple[int, str] | None | object = dataclasses.MISSING,
    title: str | None = None,
    source: str | None = None,
    genre: str | None = None,
    notes: list[str] | None = None,
    confidence: int | None = None,
    review_reasons: list[str] | None = None,
    decision_reasons: list[str] | None = None,
    filename_suffix: str | None = None,
    output_folder: Path | None = None,
    archive_source_path: Path | None = None,
) -> BookRecord:
    return dataclasses.replace(
        record,
        author=author if author is not None else record.author,
        series=series if series is not None else record.series,
        volume=record.volume if volume is dataclasses.MISSING else volume,
        title=title if title is not None else record.title,
        source=source if source is not None else record.source,
        genre=genre if genre is not None else record.genre,
        notes=list(notes) if notes is not None else list(record.notes),
        confidence=record.confidence if confidence is None else confidence,
        review_reasons=list(review_reasons) if review_reasons is not None else list(record.review_reasons),
        decision_reasons=list(decision_reasons) if decision_reasons is not None else list(record.decision_reasons),
        filename_suffix=record.filename_suffix if filename_suffix is None else filename_suffix,
        output_folder=record.output_folder if output_folder is None else output_folder,
        archive_source_path=record.archive_source_path if archive_source_path is None else archive_source_path,
        identifiers=list(record.identifiers),
    )
