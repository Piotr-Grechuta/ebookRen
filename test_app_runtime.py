from pathlib import Path
import tempfile
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
            app_runtime.write_book_metadata(
                Path("book.mobi"),
                record,
                extra_tags=["Killim"],
                calibre_folder=Path(r"C:\Program Files\Calibre2"),
            )

        writer.assert_called_once()
        self.assertEqual(writer.call_args.args[0], Path("book.mobi"))
        self.assertEqual(writer.call_args.kwargs["creators"], ["Alfred Hitchcock"])
        self.assertEqual(writer.call_args.kwargs["author_sort"], "Hitchcock Alfred")
        self.assertIn("Killim", writer.call_args.kwargs["subjects"])
        self.assertEqual(writer.call_args.kwargs["calibre_folder"], Path(r"C:\Program Files\Calibre2"))

    def test_metadata_author_display_name_flips_last_first_name(self) -> None:
        with mock.patch.object(app_runtime, "resolve_known_author", return_value="Alfred Hitchcock"):
            self.assertEqual(app_runtime.metadata_author_display_name("Hitchcock Alfred"), "Alfred Hitchcock")

    def test_parse_extra_tags_deduplicates_and_cleans_values(self) -> None:
        self.assertEqual(
            app_runtime.parse_extra_tags(" Killim, Killim ;  Arka |  "),
            ["Killim", "Arka"],
        )

    def test_choose_conversion_source_prefers_richer_format(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            txt = folder / "Book.txt"
            azw3 = folder / "Book.azw3"
            txt.write_text("x", encoding="utf-8")
            azw3.write_text("x", encoding="utf-8")

            chosen = app_runtime.choose_conversion_source([txt, azw3])

        self.assertEqual(chosen, azw3)

    def test_run_metadata_backfill_writes_metadata_with_selected_tags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            (folder / "one.epub").write_text("x", encoding="utf-8")
            (folder / "two.mobi").write_text("x", encoding="utf-8")
            record = app_runtime.BookRecord(
                path=Path("template.epub"),
                author="Author Example",
                series="Standalone",
                volume=None,
                title="Title",
                source="test",
                identifiers=[],
                notes=[],
            )

            with mock.patch.object(app_runtime, "_record_from_file_path", return_value=record), mock.patch.object(
                app_runtime, "write_book_metadata"
            ) as writer:
                code, lines = app_runtime.run_metadata_backfill(
                    folder,
                    recursive=False,
                    tags_text="Killim, Arka, Killim",
                    apply_changes=True,
                    calibre_folder=Path(r"C:\Program Files\Calibre2"),
                )

        self.assertEqual(code, 0)
        self.assertIn("WRITTEN=2", lines[-1])
        self.assertEqual(writer.call_count, 2)
        self.assertEqual(writer.call_args.kwargs["extra_tags"], ["Killim", "Arka"])
        self.assertEqual(writer.call_args.kwargs["calibre_folder"], Path(r"C:\Program Files\Calibre2"))

    def test_run_epub_export_moves_existing_epub_and_trashes_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dest_tmp:
            source = Path(src_tmp)
            destination = Path(dest_tmp)
            source_epub = source / "Book.epub"
            source_mobi = source / "Book.mobi"
            source_epub.write_text("epub", encoding="utf-8")
            source_mobi.write_text("mobi", encoding="utf-8")
            record = app_runtime.BookRecord(
                path=source_epub,
                author="Author Example",
                series="Standalone",
                volume=None,
                title="Book",
                source="test",
                identifiers=[],
                notes=[],
            )
            trashed: list[Path] = []

            with mock.patch.object(app_runtime, "_record_from_file_path", return_value=record), mock.patch.object(
                app_runtime, "write_book_metadata"
            ) as writer, mock.patch.object(
                app_runtime, "move_path_to_trash", side_effect=lambda path: trashed.append(path)
            ) as trash:
                code, lines = app_runtime.run_epub_export(
                    source,
                    destination,
                    recursive=False,
                    tags_text="Killim",
                    write_metadata_after_export=True,
                )
                self.assertEqual(code, 0)
                self.assertTrue((destination / "Book.epub").exists())
                self.assertFalse(source_epub.exists())
                self.assertEqual(trashed, [source_mobi])
                trash.assert_called_once()
                writer.assert_called_once()
                self.assertEqual(writer.call_args.args[0], destination / "Book.epub")
                self.assertEqual(writer.call_args.kwargs["extra_tags"], ["Killim"])
                self.assertIn("MOVED_EPUB=1", lines[-1])
                self.assertIn("TRASHED=1", lines[-1])

    def test_run_epub_export_converts_best_source_when_epub_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dest_tmp:
            source = Path(src_tmp)
            destination = Path(dest_tmp)
            azw3 = source / "Book.azw3"
            txt = source / "Book.txt"
            azw3.write_text("azw3", encoding="utf-8")
            txt.write_text("txt", encoding="utf-8")
            record = app_runtime.BookRecord(
                path=azw3,
                author="Author Example",
                series="Standalone",
                volume=None,
                title="Book",
                source="test",
                identifiers=[],
                notes=[],
            )

            def fake_convert(source_path: Path, destination_path: Path, *, calibre_folder: Path | None = None) -> None:
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                destination_path.write_text(source_path.name, encoding="utf-8")

            with mock.patch.object(
                app_runtime.embedded_metadata_mod,
                "convert_to_epub_with_calibre",
                side_effect=fake_convert,
            ) as converter, mock.patch.object(
                app_runtime, "_record_from_file_path", return_value=record
            ), mock.patch.object(
                app_runtime, "write_book_metadata"
            ) as writer:
                code, lines = app_runtime.run_epub_export(
                    source,
                    destination,
                    recursive=False,
                    calibre_folder=Path(r"C:\Program Files\Calibre2"),
                    tags_text="Killim",
                    write_metadata_after_export=True,
                )
                self.assertEqual(code, 0)
                converter.assert_called_once()
                self.assertEqual(converter.call_args.args[:2], (azw3, destination / "Book.epub"))
                self.assertEqual(converter.call_args.kwargs["calibre_folder"], Path(r"C:\Program Files\Calibre2"))
                writer.assert_called_once()
                self.assertEqual(writer.call_args.args[0], destination / "Book.epub")
                self.assertEqual(writer.call_args.kwargs["extra_tags"], ["Killim"])
                self.assertTrue((destination / "Book.epub").exists())
                self.assertTrue(azw3.exists())
                self.assertTrue(txt.exists())
                self.assertIn("CONVERTED=1", lines[-1])

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
