from __future__ import annotations

import re
from typing import Any, Callable, Iterable, TypeAlias

from domain_naming import BookRecord
from models_core import Candidate, EpubMetadata

StrCleaner: TypeAlias = Callable[[str | None], str]
VolumeParser: TypeAlias = Callable[[str | None], tuple[int, str] | None]
LooksLikeText: TypeAlias = Callable[[str | None], bool]
GENERIC_VOLUME_MARKER_RE = re.compile(
    r"(?:^|[\s,._-])(?:book|tom|volume|vol\.?|part|cykl|czesc|część|ksiega|księga)\s*$",
    re.IGNORECASE,
)


def add_candidate(
    candidates: list[Candidate],
    series: str,
    volume: tuple[int, str] | None,
    score: int,
    source: str,
    title_override: str | None = None,
    *,
    clean_series: StrCleaner,
    is_publisher_like: LooksLikeText,
    clean: StrCleaner,
) -> None:
    cleaned = clean_series(series)
    if not cleaned:
        return
    if source == "opf" and is_publisher_like(cleaned):
        return
    candidates.append(Candidate(score, cleaned, volume, clean(title_override), source))


def series_candidate_priority(candidate: Candidate, *, series_source_priorities: dict[str, int]) -> tuple[int, int, int]:
    return (
        series_source_priorities.get(candidate.source, candidate.score),
        candidate.score,
        1 if candidate.title_override else 0,
    )


def choose_series_candidate(candidates: list[Candidate], *, series_source_priorities: dict[str, int]) -> Candidate | None:
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: series_candidate_priority(candidate, series_source_priorities=series_source_priorities))


def choose_title_candidate(candidates: list[Candidate]) -> Candidate | None:
    title_candidates = [candidate for candidate in candidates if candidate.title_override]
    if not title_candidates:
        return None
    return max(title_candidates, key=lambda candidate: (candidate.score, len(candidate.title_override or "")))


def source_needs_online_verification(source: str) -> bool:
    return source.startswith("core:") or source.startswith("segment:")


def existing_format_needs_online_verification(record: BookRecord) -> bool:
    del record
    return True


def extract_trailing_author_from_core(
    text: str,
    *,
    strip_source_artifacts: StrCleaner,
    clean_author_segment: StrCleaner,
    looks_like_author_segment: LooksLikeText,
) -> str:
    value = strip_source_artifacts(text)
    if " - " in value:
        _, _, trailing = value.rpartition(" - ")
        trailing = clean_author_segment(trailing)
        if looks_like_author_segment(trailing):
            return trailing
    parts = value.split()
    if len(parts) < 2:
        return ""
    surname_particles = {
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
    blocked_tokens = {"book", "part", "series", "tom", "volume"}
    for size in (2, 3):
        if len(parts) < size:
            continue
        candidate = clean_author_segment(" ".join(parts[-size:]))
        if not looks_like_author_segment(candidate):
            continue
        name_parts = candidate.split()
        if size == 3 and name_parts[1].lower() not in surname_particles and not re.fullmatch(r"[A-Za-z]\.?", name_parts[1]):
            continue
        if any(token.lower() in blocked_tokens for token in name_parts):
            continue
        return candidate
    return ""


def strip_leading_title_index(title: str, *, clean: StrCleaner, leading_index_title_re) -> str:
    cleaned = clean(title)
    match = leading_index_title_re.match(cleaned)
    if not match:
        return cleaned
    stripped = clean(match.group(1))
    return stripped or cleaned


def sanitize_title_for_online_query(
    title: str,
    author: str,
    series: str,
    volume: tuple[int, str] | None,
    *,
    strip_source_artifacts: StrCleaner,
    query_noise_paren_re,
    looks_like_author_segment: LooksLikeText,
    sanitize_title,
    normalize_match_text,
    strip_author_from_title,
    strip_leading_title_index,
    clean: StrCleaner,
) -> str:
    value = strip_source_artifacts(title)
    if not value:
        return ""
    value = query_noise_paren_re.sub("", value)
    if " - " in value:
        left, _, right = value.partition(" - ")
        if looks_like_author_segment(left):
            value = right
    value = sanitize_title(value, series, volume) or value
    if series and normalize_match_text(series) in normalize_match_text(value):
        if volume is not None:
            value = sanitize_title(value, series, volume) or value
    value = strip_author_from_title(value, author)
    value = strip_leading_title_index(value)
    value = clean(value)
    return value


def lubimyczytac_author_query_terms(
    creators: Iterable[str],
    *,
    clean_author_segment: StrCleaner,
    to_last_first,
    normalize_match_text,
    clean: StrCleaner,
) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for creator in creators:
        cleaned_creator = clean_author_segment(creator)
        if not cleaned_creator:
            continue
        creator_variants = [cleaned_creator]
        normalized_creator = clean_author_segment(to_last_first(cleaned_creator))
        if normalized_creator and normalized_creator not in creator_variants:
            creator_variants.append(normalized_creator)
        for variant in creator_variants:
            parts = [part for part in variant.split() if len(part) > 1]
            if len(parts) < 2:
                continue
            for candidate in (parts[-1], parts[0]):
                key = normalize_match_text(candidate)
                cleaned_candidate = clean(candidate)
                if not key or not cleaned_candidate or key in seen:
                    continue
                seen.add(key)
                terms.append(cleaned_candidate)
    return terms


def normalize_lubimyczytac_query_title(title: str, *, sanitize_title_for_online_query, clean: StrCleaner) -> str:
    value = sanitize_title_for_online_query(title, "", "", None) or clean(title)
    if not value:
        return ""
    value = value.replace("?", "").replace("_", " ")
    if '"' in value or ",," in value:
        value = value.split('"')[0].split(",,")[0]
    value = value.replace("&", " ")
    value = value.replace("#", "")
    value = value.replace("(", " ").replace(")", " ")
    if "'" in value:
        value = value.split("'")[0]
    return clean(value)


def build_lubimyczytac_query_terms(
    meta: EpubMetadata,
    *,
    clean: StrCleaner,
    normalize_lubimyczytac_query_title,
    lubimyczytac_author_query_terms,
    normalize_match_text,
) -> list[str]:
    title = clean(meta.title or meta.core)
    if not title:
        return []

    primary_title = normalize_lubimyczytac_query_title(title)
    if not primary_title:
        return []

    title_variants = [primary_title]
    if "." in primary_title:
        before_dot = normalize_lubimyczytac_query_title(primary_title.split(".", 1)[0])
        if before_dot and before_dot not in title_variants:
            title_variants.append(before_dot)

    author_terms = lubimyczytac_author_query_terms(meta.creators)
    terms: list[str] = []
    seen: set[str] = set()
    for variant in title_variants:
        candidates = [variant]
        for author_term in reversed(author_terms):
            candidates.insert(0, clean(f"{author_term} {variant}"))
        for candidate in candidates:
            key = normalize_match_text(candidate)
            if not key or key in seen:
                continue
            seen.add(key)
            terms.append(candidate)
    return terms


def split_trailing_series_book(
    title: str,
    *,
    trailing_book_index_re,
    parse_volume_parts: VolumeParser,
    clean: StrCleaner,
    clean_series: StrCleaner,
    is_publisher_like: LooksLikeText,
) -> tuple[str, str, tuple[int, str] | None] | None:
    match = trailing_book_index_re.match(title)
    if not match:
        return None
    body = clean(match.group(1))
    volume = parse_volume_parts(match.group(2))
    if not body or volume is None:
        return None

    parts = body.split()
    if len(parts) < 3:
        return None

    joiners = {"of", "the", "and", "a", "an", "&"}
    best: tuple[int, str, str] | None = None
    for cut in range(1, len(parts) - 1):
        title_part = clean(" ".join(parts[:cut]))
        series_part = clean_series(" ".join(parts[cut:]))
        if not title_part or not series_part or is_publisher_like(series_part):
            continue
        series_words = series_part.split()
        if len(series_words) < 2:
            continue

        score = 0
        if re.match(r"^(?:The|A|An)\b", series_part, flags=re.IGNORECASE):
            score += 30
        if 2 <= len(series_words) <= 5:
            score += 20
        score += sum(
            1
            for word in series_words
            if word[:1].isupper() or word.lower() in joiners or word.startswith(("(", "["))
        )
        candidate = (score, title_part, series_part)
        if best is None or candidate[0] > best[0]:
            best = candidate

    if best is None:
        return None
    return best[1], best[2], volume


def looks_like_generic_volume_prefix(text: str, *, clean: StrCleaner) -> bool:
    value = clean(text)
    if not value:
        return False
    return bool(GENERIC_VOLUME_MARKER_RE.search(value))


def split_square_bracket_series_book(
    title: str,
    *,
    clean: StrCleaner,
    clean_series: StrCleaner,
    parse_volume_parts: VolumeParser,
) -> tuple[str, str, tuple[int, str] | None] | None:
    normalized = clean(title)
    if not normalized:
        return None

    def parse_bracket_payload(payload: str) -> tuple[str, tuple[int, str] | None] | None:
        inner = clean(payload)
        if not inner:
            return None
        inner = re.sub(r"^(?:cykl|seria)\s*[-:\s]+", "", inner, flags=re.IGNORECASE)
        paren_match = re.match(r"^(.*?)\s*\(([^()]+)\)\s*$", inner)
        if paren_match:
            series_name = clean_series(paren_match.group(1))
            volume = parse_volume_parts(paren_match.group(2))
            if series_name and volume is not None:
                return series_name, volume
        tail_match = re.match(r"^(.*?)\s+([0-9]+(?:\.[0-9]+)?|[IVXLCDM]+)\s*$", inner, flags=re.IGNORECASE)
        if tail_match:
            series_name = clean_series(tail_match.group(1))
            volume = parse_volume_parts(tail_match.group(2))
            if series_name and volume is not None:
                return series_name, volume
        return None

    prefix_match = re.match(r"^\[([^\]]+)\]\s*(.+)$", normalized)
    if prefix_match:
        parsed = parse_bracket_payload(prefix_match.group(1))
        title_part = clean(prefix_match.group(2))
        if parsed is not None and title_part:
            return title_part, parsed[0], parsed[1]

    suffix_match = re.match(r"^(.*?)\s*\[([^\]]+)\]\s*$", normalized)
    if suffix_match:
        parsed = parse_bracket_payload(suffix_match.group(2))
        title_part = clean(suffix_match.group(1))
        if parsed is not None and title_part:
            return title_part, parsed[0], parsed[1]

    return None


def collect_title_candidates(
    title: str,
    candidates: list[Candidate],
    *,
    clean,
    parse_volume_parts,
    add_candidate,
    split_trailing_series_book,
    split_square_bracket_series_book,
    title_dotted_series_book_re,
    title_double_colon_book_re,
    title_with_series_re,
    paren_series_re,
    series_only_paren_index_re,
    title_colon_series_index_re,
    indexed_title_re,
    index_only_re,
    leading_index_dotted_title_re,
    box_set_re,
) -> None:
    title = clean(title)
    if not title:
        return

    match = title_dotted_series_book_re.match(title)
    if match:
        volume = parse_volume_parts(match.group(2))
        if volume is not None:
            add_candidate(candidates, match.group(1), volume, 97, "title:dotted-series-book", match.group(3))

    match = title_double_colon_book_re.match(title)
    if match:
        volume = parse_volume_parts(match.group(3))
        if volume is not None:
            add_candidate(candidates, match.group(2), volume, 98, "title:double-colon-book", match.group(1))

    square_bracket_series_book = split_square_bracket_series_book(title)
    if square_bracket_series_book is not None:
        title_part, series_part, volume = square_bracket_series_book
        add_candidate(candidates, series_part, volume, 96, "title:square-bracket-series-book", title_part)

    trailing_series_book = split_trailing_series_book(title)
    if trailing_series_book is not None:
        title_part, series_part, volume = trailing_series_book
        add_candidate(candidates, series_part, volume, 95, "title:trailing-series-book", title_part)

    match = title_with_series_re.match(title)
    if match:
        volume = parse_volume_parts(match.group(3))
        if volume is not None:
            add_candidate(candidates, match.group(2), volume, 93, "title:series-book", match.group(1))

    match = paren_series_re.match(title)
    if match:
        volume = parse_volume_parts(match.group(3))
        if volume is not None:
            add_candidate(candidates, match.group(2), volume, 92, "title:paren-series", match.group(1))

    match = series_only_paren_index_re.match(title)
    if match:
        volume = parse_volume_parts(match.group(2))
        if volume is not None:
            add_candidate(candidates, match.group(1), volume, 91, "title:series-index-only")

    match = title_colon_series_index_re.match(title)
    if match:
        volume = parse_volume_parts(match.group(3))
        if volume is not None:
            add_candidate(candidates, match.group(2), volume, 94, "title:colon-series-index", match.group(1))

    match = leading_index_dotted_title_re.match(title)
    if match:
        volume = parse_volume_parts(match.group(1))
        dotted_title = clean(match.group(2))
        if volume is not None and dotted_title:
            parts = [clean(part) for part in dotted_title.split(".") if clean(part)]
            if len(parts) >= 2 and 1 <= len(parts[0].split()) <= 4:
                add_candidate(candidates, parts[0], volume, 89, "title:leading-index-dotted", ". ".join(parts[1:]))

    match = indexed_title_re.match(title)
    if match and not re.match(r"^\d", clean(match.group(3))):
        volume = parse_volume_parts(match.group(2))
        if volume is not None:
            add_candidate(candidates, match.group(1), volume, 86, "title:indexed", match.group(3))

    match = index_only_re.match(title)
    if match:
        volume = parse_volume_parts(match.group(2))
        if volume is not None:
            add_candidate(candidates, match.group(1), volume, 78, "title:index-only")

    match = box_set_re.match(title)
    if match:
        add_candidate(candidates, match.group(1), None, 82, "title:box-set", match.group(2))


def collect_core_candidates(
    core: str,
    candidates: list[Candidate],
    *,
    clean,
    add_candidate,
    parse_volume_parts,
    looks_like_author_segment,
    box_set_re,
    paren_series_re,
    core_title_author_re,
    core_comma_re,
    core_joined_re,
    core_spaced_re,
    core_index_only_re,
) -> None:
    core = clean(core)
    if not core:
        return

    match = box_set_re.match(core)
    if match:
        add_candidate(candidates, match.group(1), None, 81, "core:box-set", match.group(2))

    match = paren_series_re.match(core)
    if match:
        volume = parse_volume_parts(match.group(3))
        if volume is not None:
            add_candidate(candidates, match.group(2), volume, 90, "core:paren-series", match.group(1))

    match = core_title_author_re.match(core)
    if (
        match
        and looks_like_author_segment(match.group(4))
        and not re.match(r"^\d+\b", clean(match.group(3)))
        and not looks_like_generic_volume_prefix(match.group(1), clean=clean)
    ):
        volume = parse_volume_parts(match.group(2))
        if volume is not None:
            add_candidate(candidates, match.group(1), volume, 91, "core:title-author", match.group(3))

    match = core_comma_re.match(core)
    if match and not looks_like_generic_volume_prefix(match.group(1), clean=clean):
        volume = parse_volume_parts(match.group(2))
        if volume is not None:
            add_candidate(candidates, match.group(1), volume, 87, "core:comma", match.group(3))

    match = core_joined_re.match(core)
    if (
        match
        and not re.match(r"^\d+\b", clean(match.group(3)))
        and not looks_like_generic_volume_prefix(match.group(1), clean=clean)
    ):
        volume = parse_volume_parts(match.group(2))
        if volume is not None:
            add_candidate(candidates, match.group(1), volume, 88, "core:joined", match.group(3))

    match = core_spaced_re.match(core)
    if (
        match
        and not re.match(r"^\d+\b", clean(match.group(3)))
        and not looks_like_generic_volume_prefix(match.group(1), clean=clean)
    ):
        volume = parse_volume_parts(match.group(2))
        if volume is not None:
            add_candidate(candidates, match.group(1), volume, 80, "core:spaced", match.group(3))

    match = core_index_only_re.match(core)
    if match:
        volume = parse_volume_parts(match.group(2))
        if volume is not None:
            add_candidate(candidates, match.group(1), volume, 76, "core:index-only")


def collect_segment_candidates(
    segments: list[str],
    candidates: list[Candidate],
    *,
    strip_source_artifacts,
    is_source_artifact,
    is_publisher_like,
    segment_hash_re,
    segment_comma_re,
    segment_year_re,
    add_candidate,
    parse_volume_parts,
) -> None:
    for segment in segments[2:6]:
        segment = strip_source_artifacts(segment)
        if not segment or is_source_artifact(segment) or is_publisher_like(segment):
            continue

        match = segment_hash_re.search(segment)
        if match:
            volume = parse_volume_parts(match.group(2))
            if volume is not None:
                add_candidate(candidates, match.group(1), volume, 74, "segment:hash")

        match = segment_comma_re.match(segment)
        if match:
            volume = parse_volume_parts(match.group(2))
            if volume is not None:
                add_candidate(candidates, match.group(1), volume, 72, "segment:comma")

        match = segment_year_re.match(segment)
        if match:
            volume = parse_volume_parts(match.group(2))
            if volume is not None:
                add_candidate(candidates, match.group(1), volume, 75, "segment:year")


def sanitize_title(
    title: str,
    series: str,
    volume: tuple[int, str] | None,
    *,
    strip_source_artifacts,
    genre_tail_re,
    trailing_series_suffix_re,
    volume_match_pattern,
    is_series_volume_only_title,
    clean,
) -> str:
    title = strip_source_artifacts(title)
    if not title:
        return ""
    title = genre_tail_re.sub("", title)
    title = trailing_series_suffix_re.sub("", title)
    if series and volume is not None:
        prefix = rf"^{re.escape(series)}\s+{volume_match_pattern(volume)}\s*[:\-]\s*"
        title = re.sub(prefix, "", title, flags=re.IGNORECASE)
        suffix = rf"\s+{re.escape(series)}\s*,?\s*(?:Book|Tom|Volume|Vol\.?)\s*{volume_match_pattern(volume)}$"
        title = re.sub(suffix, "", title, flags=re.IGNORECASE)
    if is_series_volume_only_title(title, series, volume):
        return ""
    return clean(title)


def is_series_volume_only_title(
    title: str,
    series: str,
    volume: tuple[int, str] | None,
    *,
    clean,
    clean_series,
    volume_match_pattern,
    series_words: str,
) -> bool:
    title = clean(title)
    series = clean_series(series)
    if not title or not series or volume is None:
        return False

    volume_pattern = rf"(?:{volume_match_pattern(volume)})"
    series_pattern = re.escape(series)
    patterns = [
        rf"^{series_pattern}\s*(?:[-_:,]\s*)?(?:{series_words}\s*)?{volume_pattern}$",
        rf"^{series_pattern}\s*(?:[-_:,]\s*)?{volume_pattern}$",
        rf"^(?:{series_words}\s*)?{volume_pattern}\s*(?:[-_:,]\s*)?{series_pattern}$",
    ]
    return any(re.match(pattern, title, flags=re.IGNORECASE) for pattern in patterns)


def strip_author_from_title(title: str, author: str, *, clean, looks_like_author_segment) -> str:
    title = clean(title)
    if not title:
        return title
    if " - " in title:
        left, _, right = title.partition(" - ")
        if looks_like_author_segment(left):
            title = right
    if not author:
        return clean(title)
    for token in [part.strip() for part in author.split("&") if part.strip()]:
        names = [name for name in clean(token).split() if len(name) > 1]
        if len(names) < 2:
            continue
        title = re.sub(rf"\b{re.escape(names[0])}\b", "", title, flags=re.IGNORECASE)
        title = re.sub(rf"\b{re.escape(names[-1])}\b", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s{2,}", " ", title)
    title = re.sub(r"\s*[-,:]\s*$", "", title)
    return clean(title)


def extract_authors(creators: list[str], segment_author: str, *, resolve_author_segment, split_authors, canonicalize_authors, to_last_first) -> str:
    raw: list[str] = []
    ordered: list[str] = []

    def token_signature(text: str) -> tuple[str, ...]:
        return tuple(re.sub(r"[\W\d_]+", "", token, flags=re.UNICODE).lower() for token in text.split() if token)

    def extend_ordered(values: list[str]) -> None:
        seen = {re.sub(r"[^a-z0-9]", "", to_last_first(item).lower()) for item in ordered}
        for value in values:
            normalized = to_last_first(value)
            key = re.sub(r"[^a-z0-9]", "", normalized.lower())
            if normalized and key and key not in seen:
                seen.add(key)
                ordered.append(normalized)

    def pick_authors(text: str) -> tuple[list[str], bool]:
        plain = split_authors(text)
        resolved = resolve_author_segment(text)
        if len(resolved) > 1 and (len(resolved) > len(plain) or len(plain) <= 1):
            return resolved, True
        if len(resolved) > 1 and any(len(part.split()) < 2 for part in plain):
            return resolved, True
        if len(resolved) == 1 and len(plain) > 1:
            plain_resolved = [resolve_author_segment(part) for part in plain]
            matching_parts = sum(1 for item in plain_resolved if item == resolved)
            recognized_parts = sum(1 for item in plain_resolved if item)
            if matching_parts == 1 and recognized_parts == 1:
                return resolved, True
        if len(resolved) == 1 and len(plain) == 1 and token_signature(resolved[0]) != token_signature(plain[0]):
            return resolved, False
        return plain, False

    for creator in creators:
        values, preserve_order = pick_authors(creator)
        if preserve_order:
            extend_ordered(values)
        else:
            raw.extend(values)
    if segment_author and re.search(r"[A-Za-z]", segment_author) and len(creators) <= 1:
        values, preserve_order = pick_authors(segment_author)
        if preserve_order:
            extend_ordered(values)
        else:
            raw.extend(values)
    result = list(ordered)
    seen = {re.sub(r"[^a-z0-9]", "", item.lower()) for item in result}
    for item in canonicalize_authors(raw):
        key = re.sub(r"[^a-z0-9]", "", item.lower())
        if key and key not in seen:
            seen.add(key)
            result.append(item)
    return " & ".join(result) if result else "Nieznany Autor"


def build_online_query_variants(
    meta: EpubMetadata,
    prototype: Any,
    *,
    clean,
    clean_author_segment,
    to_last_first,
    normalize_match_text,
    author_match_keys,
    looks_like_author_segment,
    sanitize_title_for_online_query,
) -> list[EpubMetadata]:
    variants: list[EpubMetadata] = []
    seen: set[tuple[str, tuple[str, ...]]] = set()

    def add_variant(title: str, creators: list[str]) -> None:
        normalized_title = clean(title)
        normalized_creators: list[str] = []
        for item in creators:
            cleaned_creator = clean_author_segment(item)
            if not cleaned_creator:
                continue
            preferred_creator = clean_author_segment(to_last_first(cleaned_creator)) or cleaned_creator
            for candidate_creator in (preferred_creator, cleaned_creator):
                if candidate_creator and candidate_creator not in normalized_creators:
                    normalized_creators.append(candidate_creator)
        key = (normalize_match_text(normalized_title), tuple(sorted(author_match_keys(normalized_creators))))
        if not normalized_title or key in seen:
            return
        seen.add(key)
        variants.append(
            EpubMetadata(
                path=meta.path,
                stem=meta.stem,
                segments=list(meta.segments),
                core=normalized_title,
                title=normalized_title,
                creators=list(normalized_creators),
                identifiers=list(meta.identifiers),
                subjects=list(meta.subjects),
                meta_series=meta.meta_series,
                meta_volume=meta.meta_volume,
                errors=[],
            )
        )

    prototype_authors = [part.strip() for part in prototype.author.split("&") if part.strip()] if prototype.author != "Nieznany Autor" else []
    if not prototype_authors and " - " in prototype.title:
        left, _, _ = prototype.title.partition(" - ")
        if looks_like_author_segment(left):
            prototype_authors = [left]
    cleaned_title = sanitize_title_for_online_query(prototype.title, prototype.author, prototype.series, prototype.volume)
    if cleaned_title:
        add_variant(cleaned_title, prototype_authors or list(meta.creators))
        if prototype.series and prototype.series != "Standalone":
            add_variant(f"{prototype.series} {cleaned_title}", prototype_authors or list(meta.creators))
    if meta.title and clean(meta.title) != clean(prototype.title):
        add_variant(meta.title, list(meta.creators))
    return variants
