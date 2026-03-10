from __future__ import annotations

import re
from pathlib import Path
from typing import Callable


Cleaner = Callable[[str | None], str]


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
    value = hex_noise_re.sub("", value)
    value = re.sub(r"\s*\(\d+\)\s*$", "", value)
    return clean(value)


def is_source_artifact(text: str | None, *, clean: Cleaner, source_artifact_re, nullish_re) -> bool:
    value = clean(text)
    return bool(value and (source_artifact_re.search(value) or nullish_re.match(value)))


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
    value = re.sub(r"\s*[-,;]\s*\d+(?:\s*,\s*\d{4})?\s*$", "", value)
    value = re.sub(r"\s*,\s*\d{4}\s*$", "", value)
    return clean(value)


def is_supported_book_file(path: Path, *, supported_extensions: set[str]) -> bool:
    return path.is_file() and path.suffix.lower() in supported_extensions
