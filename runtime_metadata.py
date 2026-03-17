from __future__ import annotations

import os
import re
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Callable

CONTAINER_NS = "urn:oasis:names:tc:opendocument:xmlns:container"
OPF_NS = "http://www.idpf.org/2007/opf"
DC_NS = "http://purl.org/dc/elements/1.1/"

ET.register_namespace("", OPF_NS)
ET.register_namespace("dc", DC_NS)
ET.register_namespace("opf", OPF_NS)


def _xml_tag(namespace: str, local_name: str) -> str:
    return f"{{{namespace}}}{local_name}"


def _local_name(tag: str) -> str:
    return tag.split("}", 1)[1] if tag.startswith("{") else tag


def _find_metadata_element(root: ET.Element) -> ET.Element:
    for child in root:
        if _local_name(child.tag) == "metadata":
            return child
    metadata = ET.Element(_xml_tag(OPF_NS, "metadata"))
    root.insert(0, metadata)
    return metadata


def _resolve_epub_package_path(path: Path) -> str:
    with zipfile.ZipFile(path, "r") as archive:
        container_xml = archive.read("META-INF/container.xml")
    container_root = ET.fromstring(container_xml)
    rootfile = container_root.find(f".//{{{CONTAINER_NS}}}rootfile")
    if rootfile is None:
        raise ValueError("Brak wpisu rootfile w META-INF/container.xml")
    package_path = (rootfile.get("full-path") or "").strip()
    if not package_path:
        raise ValueError("Brak atrybutu full-path w rootfile EPUB")
    return package_path


def _rewrite_epub_entry(path: Path, *, entry_name: str, payload: bytes) -> None:
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=path.suffix, dir=path.parent) as handle:
            temp_path = Path(handle.name)
        with zipfile.ZipFile(path, "r") as source_archive, zipfile.ZipFile(temp_path, "w") as target_archive:
            target_archive.comment = source_archive.comment
            for info in source_archive.infolist():
                data = payload if info.filename == entry_name else source_archive.read(info.filename)
                cloned = zipfile.ZipInfo(info.filename, date_time=info.date_time)
                cloned.compress_type = info.compress_type
                cloned.comment = info.comment
                cloned.extra = info.extra
                cloned.create_system = info.create_system
                cloned.create_version = info.create_version
                cloned.extract_version = info.extract_version
                cloned.flag_bits = info.flag_bits
                cloned.volume = info.volume
                cloned.internal_attr = info.internal_attr
                cloned.external_attr = info.external_attr
                target_archive.writestr(cloned, data, compress_type=info.compress_type)
        os.replace(temp_path, path)
    except Exception:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()
        raise


def _remove_metadata_nodes(metadata: ET.Element, predicate: Callable[[ET.Element], bool]) -> None:
    for child in list(metadata):
        if predicate(child):
            metadata.remove(child)


def _append_dc_text(metadata: ET.Element, local_name: str, text: str) -> ET.Element:
    element = ET.SubElement(metadata, _xml_tag(DC_NS, local_name))
    element.text = text
    return element


def _format_series_index(volume: tuple[int, str] | None) -> str:
    if volume is None:
        return ""
    major, minor = volume
    minor_text = str(minor).zfill(2)
    if major == 0 and minor_text == "00":
        return ""
    return f"{major}.{minor_text}"


def write_epub_metadata(
    path: Path,
    *,
    title: str,
    creators: list[str],
    creator_sort_keys: list[str] | None = None,
    series: str,
    volume: tuple[int, str] | None,
    genre: str,
    extra_subjects: list[str] | None = None,
    clean: Callable[[str | None], str],
    clean_series: Callable[[str | None], str],
    normalize_match_text: Callable[[str | None], str],
) -> None:
    if path.suffix.lower() != ".epub":
        return

    package_path = _resolve_epub_package_path(path)
    with zipfile.ZipFile(path, "r") as archive:
        package_xml = archive.read(package_path)

    package_root = ET.fromstring(package_xml)
    metadata = _find_metadata_element(package_root)
    normalized_title = clean(title) or "Bez tytulu"
    normalized_creators = [clean(item) for item in creators if clean(item)] or ["Nieznany Autor"]
    normalized_creator_sort_keys = [clean(item) for item in (creator_sort_keys or [])]
    normalized_series = clean_series(series)
    normalized_genre = clean(genre)
    normalized_extra_subjects = [clean(item) for item in (extra_subjects or []) if clean(item)]
    series_index = _format_series_index(volume)

    _remove_metadata_nodes(metadata, lambda child: child.tag == _xml_tag(DC_NS, "title"))
    _remove_metadata_nodes(metadata, lambda child: child.tag == _xml_tag(DC_NS, "creator"))
    _remove_metadata_nodes(
        metadata,
        lambda child: _local_name(child.tag) == "meta"
        and (
            (child.get("name") or "").strip().lower() in {"calibre:series", "calibre:series_index"}
            or (child.get("property") or "").strip().lower() in {"belongs-to-collection", "collection-type", "group-position"}
        ),
    )

    _append_dc_text(metadata, "title", normalized_title)
    for index, creator in enumerate(normalized_creators):
        creator_element = _append_dc_text(metadata, "creator", creator)
        sort_key = normalized_creator_sort_keys[index] if index < len(normalized_creator_sort_keys) and normalized_creator_sort_keys[index] else creator
        creator_element.set(_xml_tag(OPF_NS, "role"), "aut")
        creator_element.set(_xml_tag(OPF_NS, "file-as"), sort_key)

    subject_keys = {
        normalize_match_text(child.text or "")
        for child in metadata
        if child.tag == _xml_tag(DC_NS, "subject") and normalize_match_text(child.text or "")
    }
    for subject in [normalized_genre, *normalized_extra_subjects]:
        subject_key = normalize_match_text(subject)
        if not subject or not subject_key or subject_key in subject_keys:
            continue
        subject_keys.add(subject_key)
        _append_dc_text(metadata, "subject", subject)

    if normalized_series and normalize_match_text(normalized_series) != normalize_match_text("Standalone"):
        calibre_series = ET.SubElement(metadata, _xml_tag(OPF_NS, "meta"))
        calibre_series.set("name", "calibre:series")
        calibre_series.set("content", normalized_series)
        if series_index:
            calibre_series_index = ET.SubElement(metadata, _xml_tag(OPF_NS, "meta"))
            calibre_series_index.set("name", "calibre:series_index")
            calibre_series_index.set("content", series_index)

        collection_meta = ET.SubElement(metadata, _xml_tag(OPF_NS, "meta"))
        collection_meta.set("property", "belongs-to-collection")
        collection_meta.set("id", "series-collection")
        collection_meta.text = normalized_series

        collection_type = ET.SubElement(metadata, _xml_tag(OPF_NS, "meta"))
        collection_type.set("refines", "#series-collection")
        collection_type.set("property", "collection-type")
        collection_type.text = "series"

        if series_index:
            group_position = ET.SubElement(metadata, _xml_tag(OPF_NS, "meta"))
            group_position.set("refines", "#series-collection")
            group_position.set("property", "group-position")
            group_position.text = series_index

    updated_package_xml = ET.tostring(package_root, encoding="utf-8", xml_declaration=True)
    _rewrite_epub_entry(path, entry_name=package_path, payload=updated_package_xml)


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

    if epub_module is None:
        try:
            from ebooklib import epub as epub_module  # type: ignore[import-not-found]
        except Exception as exc:
            meta.errors.append(f"epub-read: {exc}")
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
