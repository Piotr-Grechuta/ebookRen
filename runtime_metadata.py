from __future__ import annotations

import re
from pathlib import Path
from typing import Callable


def read_book_metadata(
    path: Path,
    *,
    metadata_type,
    strip_source_artifacts: Callable[[str | None], str],
    clean: Callable[[str | None], str],
    clean_series: Callable[[str | None], str],
    parse_volume_parts: Callable[[str | None], tuple[int, str] | None],
    epub_module,
):
    stem = path.stem
    segments = [strip_source_artifacts(part) for part in re.split(r"\s*--\s*", stem) if strip_source_artifacts(part)]
    core = segments[0] if segments else stem
    meta = metadata_type(path=path, stem=stem, segments=segments, core=core)

    if path.suffix.lower() != ".epub":
        return meta

    try:
        book = epub_module.read_epub(str(path), options={"ignore_ncx": True})
        meta.title = clean(book.get_metadata("DC", "title")[0][0]) if book.get_metadata("DC", "title") else ""
        meta.creators = [clean(item[0]) for item in book.get_metadata("DC", "creator") if clean(item[0])]
        meta.identifiers = [clean(item[0]) for item in book.get_metadata("DC", "identifier") if clean(item[0])]
        meta.subjects = [clean(item[0]) for item in book.get_metadata("DC", "subject") if clean(item[0])]

        for namespace, key in (
            ("OPF", "calibre:series"),
            ("OPF", "series"),
            ("OPF", "belongs-to-collection"),
        ):
            values = book.get_metadata(namespace, key)
            if values and not meta.meta_series:
                meta.meta_series = clean_series(values[0][0])

        for namespace, key in (
            ("OPF", "calibre:series_index"),
            ("OPF", "series_index"),
            ("OPF", "group-position"),
        ):
            values = book.get_metadata(namespace, key)
            if values and meta.meta_volume is None:
                meta.meta_volume = parse_volume_parts(values[0][0])
    except Exception as exc:
        meta.errors.append(f"epub-read: {exc}")

    return meta
