from pathlib import Path
import unittest
from unittest import mock

import app_runtime


class AppRuntimeTests(unittest.TestCase):
    def test_collect_title_candidates_supports_unicode_polish_markers(self) -> None:
        candidates: list[app_runtime.Candidate] = []

        app_runtime.collect_title_candidates("Saga. Część 2. Finał", candidates)

        best_series = app_runtime.choose_series_candidate(candidates)
        best_title = app_runtime.choose_title_candidate(candidates)
        self.assertIsNotNone(best_series)
        self.assertIsNotNone(best_title)
        assert best_series is not None
        assert best_title is not None
        self.assertEqual(best_series.series, "Saga")
        self.assertEqual(best_series.volume, (2, "00"))
        self.assertEqual(best_title.title_override, "Finał")

    def test_read_book_metadata_delegates_to_runtime_metadata(self) -> None:
        sentinel = object()
        with mock.patch.object(app_runtime.runtime_metadata_mod, "read_book_metadata", return_value=sentinel) as reader:
            result = app_runtime.read_book_metadata(Path("book.epub"))

        self.assertIs(result, sentinel)
        reader.assert_called_once()
        self.assertEqual(reader.call_args.args[0], Path("book.epub"))
        self.assertIs(reader.call_args.kwargs["metadata_type"], app_runtime.EpubMetadata)

    def test_write_book_metadata_delegates_to_runtime_metadata(self) -> None:
        record = app_runtime.BookRecord(
            path=Path("book.epub"),
            author="Example Author",
            series="Series Name",
            volume=(2, "00"),
            title="Book Title",
            source="test",
            identifiers=[],
            notes=[],
            genre="fantasy",
        )

        with mock.patch.object(app_runtime.runtime_metadata_mod, "write_epub_metadata") as writer:
            app_runtime.write_book_metadata(Path("book.epub"), record)

        writer.assert_called_once()
        self.assertEqual(writer.call_args.args[0], Path("book.epub"))
        self.assertEqual(writer.call_args.kwargs["title"], "Book Title")
        self.assertEqual(writer.call_args.kwargs["creators"], ["Author Example"])
        self.assertEqual(writer.call_args.kwargs["creator_sort_keys"], ["Example Author"])
        self.assertEqual(writer.call_args.kwargs["series"], "Series Name")
        self.assertEqual(writer.call_args.kwargs["volume"], (2, "00"))
        self.assertEqual(writer.call_args.kwargs["genre"], "fantasy")

    def test_write_book_metadata_uses_calibre_writer_for_non_epub(self) -> None:
        record = app_runtime.BookRecord(
            path=Path("book.mobi"),
            author="Hitchcock Alfred",
            series="Series Name",
            volume=(2, "00"),
            title="Book Title",
            source="test",
            identifiers=["uri:https://example.test"],
            notes=[],
            genre="fantasy",
        )

        with mock.patch.object(app_runtime, "resolve_known_author", return_value="Alfred Hitchcock"), mock.patch.object(
            app_runtime.embedded_metadata_mod, "write_metadata_with_calibre"
        ) as writer:
            app_runtime.write_book_metadata(Path("book.mobi"), record, extra_tags=["Killim"])

        writer.assert_called_once()
        self.assertEqual(writer.call_args.args[0], Path("book.mobi"))
        self.assertEqual(writer.call_args.kwargs["creators"], ["Alfred Hitchcock"])
        self.assertEqual(writer.call_args.kwargs["author_sort"], "Hitchcock Alfred")
        self.assertIn("Killim", writer.call_args.kwargs["subjects"])

    def test_metadata_author_display_name_flips_last_first_name(self) -> None:
        with mock.patch.object(app_runtime, "resolve_known_author", return_value="Alfred Hitchcock"):
            self.assertEqual(app_runtime.metadata_author_display_name("Hitchcock Alfred"), "Alfred Hitchcock")

    def test_online_fetch_delegates_to_runtime_online(self) -> None:
        sentinel = {"ok": True}
        with mock.patch.object(app_runtime.runtime_online_mod, "online_fetch", return_value=sentinel) as online_fetch:
            result = app_runtime.online_fetch("https://example.test", 2.0, kind="json")

        self.assertEqual(result, sentinel)
        online_fetch.assert_called_once()
        self.assertEqual(online_fetch.call_args.args[:2], ("https://example.test", 2.0))
        self.assertEqual(online_fetch.call_args.kwargs["kind"], "json")


if __name__ == "__main__":
    unittest.main()
