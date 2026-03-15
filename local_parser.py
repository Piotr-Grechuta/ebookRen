from __future__ import annotations

import re
from typing import Callable

from models_core import EpubMetadata, HybridLocalParse


Cleaner = Callable[[str | None], str]
LooksLikeText = Callable[[str | None], bool]
VolumeParser = Callable[[str | None], tuple[int, str] | None]
KnownAuthorResolver = Callable[[str | None], str]
KnownAuthorPredicate = Callable[[str | None], bool]
KnownAuthorSplitter = Callable[[str | None], tuple[str, str] | None]
KnownAuthorSegmentResolver = Callable[[str | None], list[str]]
TITLE_WITH_VOLUME_MARKER_RE = re.compile(
    r"(?:^|[\s,._-])(?:book|tom|volume|vol\.?|part|cykl|czesc|czÄ™Ĺ›Ä‡|ksiega|ksiÄ™ga)\s+\d+(?:\.\d+)?\s*$",
    re.IGNORECASE,
)
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
AUTHOR_BLOCKED_TOKENS = {
    "a",
    "an",
    "and",
    "book",
    "chronicles",
    "cykl",
    "ksiega",
    "ksiÄ™ga",
    "of",
    "part",
    "series",
    "the",
    "tom",
    "volume",
}
COMMON_GIVEN_NAMES = {
    "agatha",
    "ann",
    "adrian",
    "adrianna",
    "alan",
    "aleksandra",
    "alexandra",
    "alina",
    "anders",
    "andrzej",
    "anna",
    "anthony",
    "arthur",
    "barbara",
    "charles",
    "cheslaw",
    "czeslaw",
    "cyril",
    "dakota",
    "emil",
    "ewa",
    "francis",
    "gerald",
    "gojko",
    "grzegorz",
    "harlan",
    "henryk",
    "jacek",
    "james",
    "jan",
    "jerry",
    "joanna",
    "john",
    "joseph",
    "julia",
    "krzysztof",
    "lidia",
    "maja",
    "marcin",
    "marek",
    "maria",
    "marie",
    "michael",
    "piotr",
    "rafal",
    "rafaĹ‚",
    "richard",
    "robert",
    "saladin",
    "scott",
    "stanislaw",
    "stanisĹ‚aw",
    "stephen",
    "victoria",
    "wasilij",
    "william",
    "ake",
    "ĂĄke",
    "ahmed",
}


def parse_hybrid_local(
    meta: EpubMetadata,
    *,
    clean: Cleaner,
    clean_author_segment: Cleaner,
    looks_like_author_segment: LooksLikeText,
    strip_leading_title_index: Cleaner,
    parse_volume_parts: VolumeParser,
    resolve_known_author: KnownAuthorResolver | None = None,
    is_known_author: KnownAuthorPredicate | None = None,
    split_known_author_prefix: KnownAuthorSplitter | None = None,
    split_known_author_suffix: KnownAuthorSplitter | None = None,
    resolve_author_segment: KnownAuthorSegmentResolver | None = None,
) -> HybridLocalParse:
    def normalize_title_hint(value: str) -> str:
        normalized = strip_leading_title_index(value)
        return clean(normalized)

    def is_substantial_author_label(value: str) -> bool:
        parts = [part for part in clean_author_segment(value).split() if part]
        if not 2 <= len(parts) <= 5:
            return False
        meaningful_tokens = []
        for part in parts:
            if part.lower().rstrip(".") in AUTHOR_PARTICLES:
                continue
            token = re.sub(r"[^\w]", "", part, flags=re.UNICODE)
            token = re.sub(r"[\d_]+", "", token, flags=re.UNICODE)
            if token:
                meaningful_tokens.append(token)
        if len(meaningful_tokens) < 2:
            return False
        return sum(len(token) for token in meaningful_tokens) >= 6 and any(len(token) >= 3 for token in meaningful_tokens)

    def resolve_substantial_author(value: str | None) -> str:
        if resolve_known_author is None:
            return ""
        resolved = clean_author_segment(resolve_known_author(value))
        return resolved if is_substantial_author_label(resolved) else ""

    def canonical_author_hint(value: str) -> str:
        return clean_author_segment(value)

    def is_mixed_author_segment(segment: str, resolved_authors: list[str]) -> bool:
        cleaned_segment = clean_author_segment(segment)
        if not cleaned_segment or not resolved_authors:
            return False
        if "&" in segment or "," in segment or ";" in segment:
            return True
        resolved_tokens = max(len(clean_author_segment(author).split()) for author in resolved_authors)
        segment_tokens = len(cleaned_segment.split())
        return segment_tokens > resolved_tokens

    def is_bracketed_series_payload(value: str) -> bool:
        payload = clean(value)
        if not payload or re.fullmatch(r"\d{1,4}", payload):
            return False
        paren_match = re.match(r"^(.*?)\s*\(([^()]+)\)\s*$", payload)
        if paren_match:
            return bool(clean(paren_match.group(1)) and parse_volume_parts(paren_match.group(2)) is not None)
        tail_match = re.match(r"^(.*?)\s+([0-9]+(?:\.\d+)?|[IVXLCDM]+)\s*$", payload, flags=re.IGNORECASE)
        if tail_match:
            return bool(clean(tail_match.group(1)) and parse_volume_parts(tail_match.group(2)) is not None)
        return False

    def is_structural_title_label(value: str) -> bool:
        cleaned = clean(value)
        if not cleaned:
            return False
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
            return False
        return all(token in allowed_tokens or token.isdigit() for token in tokens)

    def looks_strongly_like_author_name(value: str) -> bool:
        cleaned = clean_author_segment(value)
        if resolve_substantial_author(cleaned):
            return True
        if is_known_author is not None and is_known_author(cleaned) and is_substantial_author_label(cleaned):
            return True
        if not cleaned or not looks_like_author_segment(cleaned):
            return False
        parts = cleaned.split()
        if not 2 <= len(parts) <= 4:
            return False
        if any(part.lower() in AUTHOR_BLOCKED_TOKENS for part in parts):
            return False
        for part in parts:
            lowered = part.lower()
            if lowered in AUTHOR_PARTICLES:
                continue
            if re.fullmatch(r"[A-Z]\.?", part):
                continue
            if not part[:1].isupper():
                return False
            if not re.fullmatch(r"[^\W\d_][^\s,;:!?/\\]*", part, flags=re.UNICODE):
                return False
        lowered_parts = [part.lower() for part in parts]
        return any(part in COMMON_GIVEN_NAMES for part in lowered_parts) or any(
            re.fullmatch(r"[A-Z]\.?", part) for part in parts
        )

    def normalize_compact_author_hint(value: str) -> str:
        cleaned = clean_author_segment(value)
        resolved = resolve_substantial_author(cleaned)
        if resolved:
            return resolved
        parts = cleaned.split()
        if len(parts) == 2 and parts[1].lower() in COMMON_GIVEN_NAMES and parts[0].lower() not in COMMON_GIVEN_NAMES:
            return clean(f"{parts[1]} {parts[0]}")
        return cleaned

    core = clean(meta.core)
    if not core:
        return HybridLocalParse()

    if " - " not in core and " -- " not in core and "," not in core:
        if split_known_author_prefix is not None:
            prefix_match = split_known_author_prefix(core)
            if prefix_match is not None and is_substantial_author_label(prefix_match[0]):
                author_hint, title_hint = prefix_match
                return HybridLocalParse(
                    title_hint=normalize_title_hint(title_hint),
                    author_hint=clean_author_segment(author_hint),
                    volume_hint=None,
                    source="hybrid:compact-author-title",
                    confidence=92,
                )
        if split_known_author_suffix is not None:
            suffix_match = split_known_author_suffix(core)
            if suffix_match is not None and is_substantial_author_label(suffix_match[0]):
                author_hint, title_hint = suffix_match
                return HybridLocalParse(
                    title_hint=normalize_title_hint(title_hint),
                    author_hint=clean_author_segment(author_hint),
                    volume_hint=None,
                    source="hybrid:compact-title-author",
                    confidence=92,
                )

    if " - " not in core and " & " in core:
        left, _, right = core.rpartition(" & ")
        trailing_author = canonical_author_hint(right)
        title_hint = clean(left)
        if trailing_author and len(trailing_author.split()) >= 2 and title_hint and len(title_hint.split()) >= 2:
            return HybridLocalParse(
                title_hint=normalize_title_hint(title_hint),
                author_hint=trailing_author,
                volume_hint=None,
                source="hybrid:ampersand-title-author",
                confidence=86,
            )

    if " - " not in core:
        compact_parts = [clean(part) for part in core.split() if clean(part)]
        if 3 <= len(compact_parts) <= 6:
            prefix_author = normalize_compact_author_hint(" ".join(compact_parts[:2]))
            suffix_title = normalize_title_hint(" ".join(compact_parts[2:]))
            if (
                looks_strongly_like_author_name(prefix_author)
                and suffix_title
                and not looks_strongly_like_author_name(suffix_title)
            ):
                return HybridLocalParse(
                    title_hint=suffix_title,
                    author_hint=prefix_author,
                    volume_hint=None,
                    source="hybrid:compact-author-title",
                    confidence=83,
                )

    parts = [clean(part) for part in re.split(r"\s+-\s+", core) if clean(part)]
    if len(parts) < 2:
        return HybridLocalParse()

    if len(parts) == 2 and resolve_author_segment is not None:
        leading_segment_authors = resolve_author_segment(parts[0])
        trailing_segment_authors = resolve_author_segment(parts[1])
        trailing_title_hint = normalize_title_hint(parts[1])
        leading_title_hint = normalize_title_hint(parts[0])
        if (
            len(leading_segment_authors) == 1
            and not trailing_segment_authors
            and trailing_title_hint
            and is_mixed_author_segment(parts[0], leading_segment_authors)
        ):
            return HybridLocalParse(
                title_hint=trailing_title_hint,
                author_hint=clean_author_segment(leading_segment_authors[0]),
                volume_hint=None,
                source="hybrid:catalog-delimited-author-title",
                confidence=93,
            )
        if (
            len(trailing_segment_authors) == 1
            and not leading_segment_authors
            and leading_title_hint
            and is_mixed_author_segment(parts[1], trailing_segment_authors)
        ):
            return HybridLocalParse(
                title_hint=leading_title_hint,
                author_hint=clean_author_segment(trailing_segment_authors[0]),
                volume_hint=None,
                source="hybrid:catalog-delimited-title-author",
                confidence=93,
            )

    if len(parts) >= 3:
        leading_noise = clean(parts[0])
        second_author = canonical_author_hint(parts[1])
        trailing_title_candidate = clean(re.sub(r"^\s*\[[^\]]+\]\s*", "", " - ".join(parts[2:])))
        if (
            leading_noise
            and leading_noise.lower() == leading_noise
            and len(leading_noise.split()) == 1
            and len(leading_noise) <= 3
            and second_author
            and looks_like_author_segment(second_author)
            and len(second_author.split()) >= 2
            and trailing_title_candidate
            and len(trailing_title_candidate.split()) >= 2
        ):
            return HybridLocalParse(
                title_hint=normalize_title_hint(trailing_title_candidate),
                author_hint=second_author,
                volume_hint=None,
                source="hybrid:prefixed-noise-title-author",
                confidence=87,
            )

    trailing_author = canonical_author_hint(re.sub(r"\s*\[[^\]]+\]\s*$", "", parts[-1]))
    bracketed_title_candidate = clean(parts[0].strip("[](){} "))
    if (
        len(parts) == 2
        and trailing_author
        and looks_like_author_segment(trailing_author)
        and bracketed_title_candidate
        and re.search(r"[A-Za-z]", bracketed_title_candidate)
        and re.fullmatch(r"[\[(].*[\])]", parts[0])
    ):
        return HybridLocalParse(
            title_hint=normalize_title_hint(bracketed_title_candidate),
            author_hint=trailing_author,
            volume_hint=None,
            source="hybrid:bracketed-title-author",
            confidence=90,
        )

    prefixed_bracketed_title = re.match(r"^\[([^\]]+)\]\s*(.+)$", parts[0]) if len(parts) == 2 else None
    if prefixed_bracketed_title:
        trailing_author = canonical_author_hint(parts[1])
        bracketed_payload = clean(prefixed_bracketed_title.group(1))
        bracketed_title = clean(parts[0])
        plain_title = normalize_title_hint(prefixed_bracketed_title.group(2))
        if (
            trailing_author
            and looks_like_author_segment(trailing_author)
            and is_bracketed_series_payload(bracketed_payload)
            and plain_title
        ):
            return HybridLocalParse(
                title_hint=bracketed_title,
                author_hint=trailing_author,
                volume_hint=None,
                source="hybrid:delimited-title-author",
                confidence=91,
            )

    trailing_author_with_bracket = re.match(r"^(.*?)\s*\[([^\]]+)\]\s*$", parts[-1]) if len(parts) == 2 else None
    if trailing_author_with_bracket:
        bracketed_author = canonical_author_hint(trailing_author_with_bracket.group(1))
        bracketed_payload = clean(trailing_author_with_bracket.group(2))
        leading_title = normalize_title_hint(parts[0])
        if (
            bracketed_author
            and looks_like_author_segment(bracketed_author)
            and is_bracketed_series_payload(bracketed_payload)
            and leading_title
        ):
            return HybridLocalParse(
                title_hint=leading_title,
                author_hint=bracketed_author,
                volume_hint=None,
                source="hybrid:delimited-title-author",
                confidence=88,
            )

    leading_author_with_bracketed_title = re.match(r"^\[([^\]]+)\]\s*(.+)$", parts[1]) if len(parts) == 2 else None
    if leading_author_with_bracketed_title:
        bracketed_author = canonical_author_hint(parts[0])
        bracketed_payload = clean(leading_author_with_bracketed_title.group(1))
        bracketed_title = clean(parts[1])
        plain_title = normalize_title_hint(leading_author_with_bracketed_title.group(2))
        if (
            bracketed_author
            and looks_like_author_segment(bracketed_author)
            and is_bracketed_series_payload(bracketed_payload)
            and plain_title
        ):
            return HybridLocalParse(
                title_hint=bracketed_title,
                author_hint=bracketed_author,
                volume_hint=None,
                source="hybrid:delimited-author-title",
                confidence=91,
            )

    leading_author = canonical_author_hint(parts[0])
    trailing_title = normalize_title_hint(" - ".join(parts[1:]))
    if leading_author and trailing_title:
        if (
            len(parts) == 2
            and looks_strongly_like_author_name(parts[0])
            and trailing_title
            and not looks_strongly_like_author_name(trailing_title)
        ):
            return HybridLocalParse(
                title_hint=trailing_title,
                author_hint=leading_author,
                volume_hint=None,
                source="hybrid:delimited-author-title",
                confidence=89,
            )
        family_plural_hint = any(token.lower().endswith("owie") for token in leading_author.split())
        numeric_bracket_prefix = bool(re.match(r"^\s*[\[(]\d{1,4}[\])]", core)) and bool(re.search(r"[A-Za-z]", leading_author))
        joined_by_i = [clean(part) for part in re.split(r"\s+i\s+", leading_author, flags=re.IGNORECASE) if clean(part)]
        joined_by_amp = [clean(part) for part in re.split(r"\s*&\s*", leading_author) if clean(part)]
        coauthor_i_hint = len(joined_by_i) >= 2 and all(looks_strongly_like_author_name(part) for part in joined_by_i)
        coauthor_amp_hint = len(joined_by_amp) >= 2 and all(looks_strongly_like_author_name(part) for part in joined_by_amp)
        leading_author_hint = numeric_bracket_prefix or coauthor_i_hint or coauthor_amp_hint or family_plural_hint
        title_hint = re.search(r"\(\d+\)\s*$", trailing_title) or len(trailing_title.split()) >= 2
        if leading_author_hint and title_hint:
            return HybridLocalParse(
                title_hint=trailing_title,
                author_hint=leading_author,
                volume_hint=None,
                source="hybrid:delimited-author-title",
                confidence=90,
            )

    if not looks_like_author_segment(trailing_author):
        return HybridLocalParse()

    middle_title = clean(" - ".join(parts[1:-1])) if len(parts) > 2 else ""
    leading_segment = clean(parts[0])
    leading_volume = parse_volume_parts(leading_segment.strip("()[]{} ")) if leading_segment else None
    leading_title = normalize_title_hint(leading_segment)

    if middle_title and clean(middle_title) != clean(trailing_author):
        normalized_middle_title = normalize_title_hint(middle_title)
        if (
            is_structural_title_label(normalized_middle_title)
            and leading_title
        ):
            return HybridLocalParse(
                title_hint=leading_title,
                author_hint=trailing_author,
                volume_hint=None,
                source="hybrid:delimited-index-title-author+leading-title",
                confidence=92 if leading_volume is not None else 88,
            )
        return HybridLocalParse(
            title_hint=normalized_middle_title,
            author_hint=trailing_author,
            volume_hint=leading_volume,
            source="hybrid:delimited-index-title-author",
            confidence=92 if leading_volume is not None else 88,
        )

    title_candidate = leading_title
    conjunction_title_hint = bool(re.search(r"\s+(?:i|&)\s+", title_candidate, flags=re.IGNORECASE))
    clear_trailing_author = len(trailing_author.split()) >= 2
    explicit_volume_title_hint = bool(TITLE_WITH_VOLUME_MARKER_RE.search(title_candidate))
    short_compact_title_hint = len(title_candidate.split()) == 1 and 1 <= len(title_candidate) <= 4
    if title_candidate and (
        not looks_like_author_segment(title_candidate)
        or (conjunction_title_hint and clear_trailing_author)
        or (explicit_volume_title_hint and clear_trailing_author)
        or (short_compact_title_hint and clear_trailing_author and not looks_strongly_like_author_name(title_candidate))
    ):
        return HybridLocalParse(
            title_hint=title_candidate,
            author_hint=trailing_author,
            volume_hint=None if short_compact_title_hint and clear_trailing_author else leading_volume,
            source="hybrid:delimited-title-author",
            confidence=84,
        )

    return HybridLocalParse()
