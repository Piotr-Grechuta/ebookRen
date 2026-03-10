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
