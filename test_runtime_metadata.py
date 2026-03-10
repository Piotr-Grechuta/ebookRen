import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
