import unittest
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile

import runtime_metadata
from models_core import EpubMetadata


class _FakeBook:
    def __init__(self, metadata: dict[tuple[str, str], list[tuple[str, object]]]) -> None:
        self._metadata = metadata

    def get_metadata(self, namespace: str, key: str):
        return self._metadata.get((namespace, key), [])


class _FakeEpubModule:
    def __init__(self, book: _FakeBook | None = None, error: Exception | None = None) -> None:
        self._book = book
        self._error = error

    def read_epub(self, path: str, options=None):
        if self._error is not None:
            raise self._error
        return self._book


def _write_test_epub(path: Path, *, package_xml: str) -> None:
    container_xml = """<?xml version="1.0" encoding="utf-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
        archive.writestr("META-INF/container.xml", container_xml)
        archive.writestr("OEBPS/content.opf", package_xml)
        archive.writestr("OEBPS/chapter.xhtml", "<html xmlns='http://www.w3.org/1999/xhtml'><body>Test</body></html>")


def _read_package_root(path: Path) -> ET.Element:
    with zipfile.ZipFile(path, "r") as archive:
        return ET.fromstring(archive.read("OEBPS/content.opf"))


class RuntimeMetadataTests(unittest.TestCase):
    def test_read_book_metadata_for_non_epub_returns_empty_metadata(self) -> None:
        meta = runtime_metadata.read_book_metadata(
            Path("Author -- Title.txt"),
            metadata_type=EpubMetadata,
            strip_source_artifacts=lambda text: (text or "").strip(),
            clean=lambda text: (text or "").strip(),
            clean_series=lambda text: (text or "").strip(),
            parse_volume_parts=lambda text: None,
            epub_module=_FakeEpubModule(),
        )

        self.assertEqual(meta.stem, "Author -- Title")
        self.assertEqual(meta.segments, ["Author", "Title"])
        self.assertEqual(meta.errors, [])

    def test_read_book_metadata_populates_epub_fields(self) -> None:
        fake_book = _FakeBook(
            {
                ("DC", "title"): [(" Czerwona Królowa ", {})],
                ("DC", "creator"): [(" Victoria Aveyard ", {})],
                ("DC", "identifier"): [("9781234567890", {})],
                ("DC", "subject"): [("fantasy", {})],
                ("OPF", "calibre:series"): [(" Czerwona Królowa ", {})],
                ("OPF", "calibre:series_index"): [("1", {})],
            }
        )
        meta = runtime_metadata.read_book_metadata(
            Path("book.epub"),
            metadata_type=EpubMetadata,
            strip_source_artifacts=lambda text: (text or "").strip(),
            clean=lambda text: (text or "").strip(),
            clean_series=lambda text: (text or "").strip(),
            parse_volume_parts=lambda text: (int(text), "00") if text else None,
            epub_module=_FakeEpubModule(book=fake_book),
        )

        self.assertEqual(meta.title, "Czerwona Królowa")
        self.assertEqual(meta.creators, ["Victoria Aveyard"])
        self.assertEqual(meta.identifiers, ["9781234567890"])
        self.assertEqual(meta.subjects, ["fantasy"])
        self.assertEqual(meta.meta_series, "Czerwona Królowa")
        self.assertEqual(meta.meta_volume, (1, "00"))

    def test_read_book_metadata_records_epub_errors(self) -> None:
        meta = runtime_metadata.read_book_metadata(
            Path("broken.epub"),
            metadata_type=EpubMetadata,
            strip_source_artifacts=lambda text: (text or "").strip(),
            clean=lambda text: (text or "").strip(),
            clean_series=lambda text: (text or "").strip(),
            parse_volume_parts=lambda text: None,
            epub_module=_FakeEpubModule(error=RuntimeError("bad epub")),
        )

        self.assertEqual(len(meta.errors), 1)
        self.assertIn("epub-read: bad epub", meta.errors[0])

    def test_write_epub_metadata_updates_title_creators_subject_and_series(self) -> None:
        with self.subTest("write"):
            package_xml = """<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" xmlns:dc="http://purl.org/dc/elements/1.1/" version="2.0">
  <metadata>
    <dc:title>Old Title</dc:title>
    <dc:creator>Old Author</dc:creator>
    <dc:subject>old-tag</dc:subject>
    <meta name="calibre:series" content="Old Series" />
    <meta name="calibre:series_index" content="9.00" />
  </metadata>
  <manifest />
  <spine />
</package>
"""
            path = Path(self.id().replace(".", "_") + ".epub")
        try:
            _write_test_epub(path, package_xml=package_xml)
            runtime_metadata.write_epub_metadata(
                path,
                title="New Title",
                creators=["Author One", "Author Two"],
                creator_sort_keys=["One Author", "Two Author"],
                series="Series Name",
                volume=(3, "00"),
                genre="fantasy",
                extra_subjects=["Killim"],
                clean=lambda text: (text or "").strip(),
                clean_series=lambda text: (text or "").strip(),
                normalize_match_text=lambda text: (text or "").strip().lower(),
            )

            package_root = _read_package_root(path)
            metadata = next(child for child in package_root if child.tag.endswith("metadata"))
            titles = [child.text for child in metadata if child.tag.endswith("title")]
            creator_elements = [child for child in metadata if child.tag.endswith("creator")]
            creators = [child.text for child in creator_elements]
            subjects = [child.text for child in metadata if child.tag.endswith("subject")]
            calibre_meta = {
                child.get("name"): child.get("content")
                for child in metadata
                if child.tag.endswith("meta") and child.get("name")
            }
            collection_properties = {
                child.get("property"): (child.text or "").strip()
                for child in metadata
                if child.tag.endswith("meta") and child.get("property")
            }

            self.assertEqual(titles, ["New Title"])
            self.assertEqual(creators, ["Author One", "Author Two"])
            self.assertIn("old-tag", subjects)
            self.assertIn("fantasy", subjects)
            self.assertIn("Killim", subjects)
            self.assertEqual([child.attrib.get("{http://www.idpf.org/2007/opf}file-as") for child in creator_elements], ["One Author", "Two Author"])
            self.assertEqual(calibre_meta["calibre:series"], "Series Name")
            self.assertEqual(calibre_meta["calibre:series_index"], "3.00")
            self.assertEqual(collection_properties["belongs-to-collection"], "Series Name")
            self.assertEqual(collection_properties["collection-type"], "series")
            self.assertEqual(collection_properties["group-position"], "3.00")
        finally:
            if path.exists():
                path.unlink()

    def test_write_epub_metadata_removes_existing_series_for_standalone(self) -> None:
        package_xml = """<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" xmlns:dc="http://purl.org/dc/elements/1.1/" version="2.0">
  <metadata>
    <dc:title>Old Title</dc:title>
    <dc:creator>Old Author</dc:creator>
    <meta name="calibre:series" content="Old Series" />
    <meta name="calibre:series_index" content="9.00" />
    <meta property="belongs-to-collection" id="series-collection">Old Series</meta>
    <meta refines="#series-collection" property="collection-type">series</meta>
    <meta refines="#series-collection" property="group-position">9.00</meta>
  </metadata>
  <manifest />
  <spine />
</package>
"""
        path = Path(self.id().replace(".", "_") + ".epub")
        try:
            _write_test_epub(path, package_xml=package_xml)
            runtime_metadata.write_epub_metadata(
                path,
                title="Standalone Title",
                creators=["Solo Author"],
                creator_sort_keys=["Author Solo"],
                series="Standalone",
                volume=(0, "00"),
                genre="",
                clean=lambda text: (text or "").strip(),
                clean_series=lambda text: (text or "").strip(),
                normalize_match_text=lambda text: (text or "").strip().lower(),
            )

            package_root = _read_package_root(path)
            metadata = next(child for child in package_root if child.tag.endswith("metadata"))
            remaining_series_meta = [
                child
                for child in metadata
                if child.tag.endswith("meta")
                and (
                    child.get("name") in {"calibre:series", "calibre:series_index"}
                    or child.get("property") in {"belongs-to-collection", "collection-type", "group-position"}
                )
            ]
            self.assertEqual(remaining_series_meta, [])
        finally:
            if path.exists():
                path.unlink()


if __name__ == "__main__":
    unittest.main()
