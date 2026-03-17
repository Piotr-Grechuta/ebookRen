from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Callable, Iterable


CALIBRE_WRITE_FORMATS = {
    ".azw",
    ".azw1",
    ".azw3",
    ".azw4",
    ".docx",
    ".epub",
    ".fb2",
    ".fbz",
    ".htmlz",
    ".kepub",
    ".lrf",
    ".mobi",
    ".odt",
    ".pdb",
    ".pdf",
    ".prc",
    ".rtf",
    ".tpz",
    ".txtz",
}


def find_ebook_meta_binary() -> Path | None:
    candidates: list[str] = []
    discovered = shutil.which("ebook-meta")
    if discovered:
        candidates.append(discovered)
    candidates.extend(
        [
            r"C:\Program Files\Calibre2\ebook-meta.exe",
            r"C:\Program Files\Calibre2\ebook-meta.bat",
        ]
    )
    seen: set[str] = set()
    for candidate in candidates:
        normalized = str(candidate).strip()
        if not normalized or normalized.lower() in seen:
            continue
        seen.add(normalized.lower())
        path = Path(normalized)
        if path.exists():
            return path
    return None


def format_series_index(volume: tuple[int, str] | None) -> str:
    if volume is None:
        return ""
    major, minor = volume
    minor_text = str(minor).zfill(2)
    if major == 0 and minor_text == "00":
        return ""
    return f"{major}.{minor_text}"


def build_subjects(
    genre: str,
    extra_tags: Iterable[str],
    *,
    clean: Callable[[str | None], str],
    normalize_match_text: Callable[[str | None], str],
) -> list[str]:
    subjects: list[str] = []
    seen: set[str] = set()
    for value in [genre, *list(extra_tags)]:
        cleaned = clean(value)
        key = normalize_match_text(cleaned)
        if not cleaned or not key or key in seen:
            continue
        seen.add(key)
        subjects.append(cleaned)
    return subjects


def write_metadata_with_calibre(
    path: Path,
    *,
    title: str,
    creators: list[str],
    author_sort: str,
    series: str,
    volume: tuple[int, str] | None,
    subjects: list[str],
    identifiers: list[str],
    clean: Callable[[str | None], str],
    clean_series: Callable[[str | None], str],
    normalize_match_text: Callable[[str | None], str],
) -> None:
    suffix = path.suffix.lower()
    if suffix not in CALIBRE_WRITE_FORMATS:
        raise ValueError(f"metadata-write-unsupported:{suffix or '(brak rozszerzenia)'}")

    ebook_meta = find_ebook_meta_binary()
    if ebook_meta is None:
        raise FileNotFoundError("Nie znaleziono calibre ebook-meta.exe")

    normalized_title = clean(title) or "Bez tytulu"
    normalized_creators = [clean(item) for item in creators if clean(item)] or ["Nieznany Autor"]
    normalized_author_sort = clean(author_sort)
    normalized_series = clean_series(series)
    normalized_subjects = [clean(item) for item in subjects if clean(item)]
    series_index = format_series_index(volume)

    command = [
        str(ebook_meta),
        str(path),
        "--title",
        normalized_title,
        "--authors",
        " & ".join(normalized_creators),
    ]
    if normalized_author_sort:
        command.extend(["--author-sort", normalized_author_sort])
    if normalized_series and normalize_match_text(normalized_series) != normalize_match_text("Standalone"):
        command.extend(["--series", normalized_series])
    if series_index:
        command.extend(["--index", series_index])
    if normalized_subjects:
        command.extend(["--tags", ", ".join(normalized_subjects)])

    seen_identifiers: set[str] = set()
    for identifier in identifiers:
        cleaned_identifier = clean(identifier)
        if not cleaned_identifier:
            continue
        identifier_key = cleaned_identifier.lower()
        if identifier_key in seen_identifiers:
            continue
        seen_identifiers.add(identifier_key)
        if re.fullmatch(r"97[89][0-9]{10}|[0-9]{9}[0-9Xx]", cleaned_identifier):
            command.extend(["--isbn", cleaned_identifier.upper()])
            continue
        if ":" in cleaned_identifier:
            command.extend(["--identifier", cleaned_identifier])

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(stderr or f"ebook-meta exited with code {completed.returncode}")

