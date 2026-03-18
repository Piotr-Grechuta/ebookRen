from __future__ import annotations

import re
from pathlib import Path
from typing import Callable


Cleaner = Callable[[str | None], str]
DOMAIN_ARTIFACT_RE = re.compile(
    r"(?i)\b(?:https?://)?(?:www\.)?(?:[a-z0-9-]+\.){1,}[a-z]{2,}(?:/[^\s]*)?\b"
)
TRAILING_RENAME_ARTIFACT_RE = re.compile(
    r"(?:\s+-\s+(?:standalone|tom(?:\s+\d+(?:\.\d+)?)?|vol(?:ume)?(?:\s+\d+(?:\.\d+)?)?)){2,}\s*$",
    re.IGNORECASE,
)


def _looks_like_noise_fragment(value: str, *, clean: Cleaner) -> bool:
    cleaned = clean(value)
    if not cleaned:
        return True
    tokens = [token for token in cleaned.split() if token]
    if not tokens:
        return True
    letters = re.findall(r"[A-Za-z]+", cleaned)
    digits = re.findall(r"\d", cleaned)
    normalized_tokens = [re.sub(r"[\W_]+", "", token, flags=re.UNICODE) for token in tokens]
    normalized_tokens = [token for token in normalized_tokens if token]
    noise_words = {"known", "u", "n", "unk", "unknown"}
    if normalized_tokens and all(token.lower() in noise_words or len(token) == 1 for token in normalized_tokens):
        return True
    if len(normalized_tokens) == 1:
        token = normalized_tokens[0]
        if token and token.lower() == token and len(token) >= 2:
            return True
    if digits and len(digits) >= max(4, sum(len(token) for token in letters)):
        return True
    return False


def _looks_like_author_head(value: str, *, clean: Cleaner) -> bool:
    cleaned = clean(value)
    tokens = [token for token in cleaned.split() if token]
    if not 1 <= len(tokens) <= 4:
        return False
    meaningful = 0
    for token in tokens:
        stripped = re.sub(r"[\W_]+", "", token, flags=re.UNICODE)
        if not stripped:
            return False
        if len(stripped) == 1:
            meaningful += 1
            continue
        if not token[:1].isupper():
            return False
        meaningful += 1
    return meaningful >= 1


def is_publisher_like(text: str | None, *, clean: Cleaner, publisher_like_re) -> bool:
    value = clean(text)
    return bool(value and publisher_like_re.search(value))


def strip_source_artifacts(
    text: str | None,
    *,
    clean: Cleaner,
    source_artifact_re,
    hex_noise_re,
) -> str:
    value = clean(text)
    if not value:
        return ""
    value = source_artifact_re.sub("", value)
    if DOMAIN_ARTIFACT_RE.fullmatch(value):
        value = ""
    value = hex_noise_re.sub("", value)
    value = re.sub(r"\s*\(\d+\)\s*$", "", value)
    value = TRAILING_RENAME_ARTIFACT_RE.sub("", value)
    if " & " in value:
        left, _, right = value.partition(" & ")
        right_head = clean(right.split(" - ", 1)[0])
        if _looks_like_noise_fragment(left, clean=clean) and _looks_like_author_head(right_head, clean=clean):
            value = right
    return clean(value)


def is_source_artifact(text: str | None, *, clean: Cleaner, source_artifact_re, nullish_re) -> bool:
    value = clean(text)
    return bool(
        value
        and (
            source_artifact_re.search(value)
            or nullish_re.match(value)
            or DOMAIN_ARTIFACT_RE.fullmatch(value)
        )
    )


def looks_like_author_segment(
    text: str | None,
    *,
    strip_source_artifacts: Cleaner,
    is_publisher_like: Callable[[str | None], bool],
    is_source_artifact: Callable[[str | None], bool],
) -> bool:
    value = strip_source_artifacts(text)
    if not value or is_publisher_like(value) or is_source_artifact(value):
        return False
    if not re.search(r"[A-Za-z]", value):
        return False
    if re.fullmatch(r"\d{4}", value):
        return False
    return True


def clean_author_segment(text: str | None, *, strip_source_artifacts: Cleaner, clean: Cleaner) -> str:
    value = strip_source_artifacts(text)
    if not value:
        return ""
    value = re.sub(r"^\s*[\[(]\d{1,4}[\])]\s*", "", value)
    value = re.sub(r"\s*[-,;]\s*\d+(?:\s*,\s*\d{4})?\s*$", "", value)
    value = re.sub(r"\s*,\s*\d{4}\s*$", "", value)
    if " & " in value:
        left, _, right = value.partition(" & ")
        if _looks_like_noise_fragment(left, clean=clean) and not _looks_like_noise_fragment(right, clean=clean):
            value = right
        elif _looks_like_noise_fragment(right, clean=clean) and not _looks_like_noise_fragment(left, clean=clean):
            value = left
    return clean(value)


def is_supported_book_file(path: Path, *, supported_extensions: set[str]) -> bool:
    return path.is_file() and path.suffix.lower() in supported_extensions
