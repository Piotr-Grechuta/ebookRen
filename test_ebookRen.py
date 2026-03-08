import csv
import importlib.util
import re
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("ebookRen.py")
SPEC = importlib.util.spec_from_file_location("ebookRen", MODULE_PATH)
kod_v3 = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(kod_v3)


def make_meta(stem: str, *, title: str = "", creators: list[str] | None = None, identifiers: list[str] | None = None):
    segments = [kod_v3.clean(part) for part in re.split(r"\s*--\s*", stem) if kod_v3.clean(part)]
    meta = kod_v3.EpubMetadata(
        path=Path(stem + ".epub"),
        stem=stem,
        segments=segments,
        core=segments[0] if segments else stem,
    )
    meta.title = title
    meta.creators = list(creators or [])
    meta.identifiers = list(identifiers or [])
    return meta


class KodV3Tests(unittest.TestCase):
    def test_local_inference_prefers_explicit_series_marker_over_weak_core_series(self) -> None:
        stem = (
            "Completionist Chronicles 01_ Bibliomancer -- Hunter, James & Krout, Dakota -- "
            "Wolfman Warlock #1, 2019 -- Mountaindale Press -- 68b670c535b8be0930e8d69b0f45fe89 -- Anna’s Archive"
        )
        meta = make_meta(stem)
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.series, "Wolfman Warlock")
        self.assertEqual(record.title, "Bibliomancer")
        self.assertEqual(record.filename, "Hunter James & Krout Dakota - Wolfman Warlock - Tom 01.00 - Bibliomancer.epub")

    def test_collect_title_candidates_parses_colon_series_book_format(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.collect_title_candidates("Bibliomancer: Wolfman Warlock, Book 1", candidates)
        self.assertTrue(candidates)
        best = kod_v3.choose_series_candidate(candidates)
        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best.series, "Wolfman Warlock")
        self.assertEqual(best.volume, (1, "00"))

    def test_collect_title_candidates_parses_roman_volume(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.collect_title_candidates("Chronicle: Cykl, Tom IV", candidates)
        best = kod_v3.choose_series_candidate(candidates)
        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best.volume, (4, "00"))

    def test_sanitize_title_strips_trailing_roman_volume_suffix(self) -> None:
        self.assertEqual(
            kod_v3.sanitize_title("Chronicle (Tom IV)", "Cykl", (4, "00")),
            "Chronicle",
        )

    def test_sanitize_title_strips_trailing_cycle_suffix(self) -> None:
        self.assertEqual(
            kod_v3.sanitize_title("Chronicle (Cykl II)", "Cykl", (2, "00")),
            "Chronicle",
        )

    def test_unicode_normalization_keeps_polish_matching_signal(self) -> None:
        self.assertEqual(kod_v3.normalize_match_text("Żmijewski"), "zmijewski")
        self.assertEqual(kod_v3.author_key("Łukasz Żmijewski"), "lukaszzmijewski")

    def test_google_books_candidates_returns_empty_list_when_no_query_can_be_built(self) -> None:
        meta = make_meta("", title="", creators=[], identifiers=[])
        self.assertEqual(kod_v3.google_books_candidates(meta, 2.0), [])

    def test_pick_best_online_match_marks_small_margin_as_ambiguous(self) -> None:
        meta = make_meta("Right Book", title="Right Book", creators=["Test Author"])
        candidates = [
            kod_v3.OnlineCandidate("google-books", "google-books", "Right Book", ["Test Author"], [], 300, "title-author-exact"),
            kod_v3.OnlineCandidate("open-library", "open-library:search", "Right Book Deluxe", ["Test Author"], [], 284, "title-author-exact"),
        ]
        best = kod_v3.pick_best_online_match(meta, candidates)
        self.assertIsNotNone(best)
        assert best is not None
        self.assertIn("ambiguous", best.reason)

    def test_ambiguous_online_is_checked_but_not_applied(self) -> None:
        meta = make_meta("Mystery Book")
        offline = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        online = kod_v3.BookRecord(
            path=meta.path,
            author="Test Author",
            series="",
            volume=None,
            title="Mystery Book",
            source="google-books",
            identifiers=[],
            notes=[],
            confidence=78,
            review_reasons=["online-niejednoznaczne"],
            decision_reasons=["online-candidate:google-books"],
            online_checked=True,
        )
        with mock.patch.object(kod_v3, "enrich_from_online", return_value=online):
            record = kod_v3.infer_record(meta, use_online=True, providers=["google"], timeout=1.0)
        self.assertTrue(record.online_checked)
        self.assertFalse(record.online_applied)
        self.assertIn("online-niejednoznaczne", record.review_reasons)
        self.assertNotIn("uzupelnione-online", record.review_reasons)
        self.assertEqual(record.confidence, offline.confidence)

    def test_online_applied_does_not_force_review(self) -> None:
        meta = make_meta("Series 1: Title", creators=["Known Author"])
        record = kod_v3.BookRecord(
            path=meta.path,
            author="Known Author",
            series="Series",
            volume=(1, "00"),
            title="Title",
            source="title:series-book",
            identifiers=[],
            notes=[],
            confidence=78,
            review_reasons=[],
            decision_reasons=["online-applied:google-books"],
            online_checked=True,
            online_applied=True,
        )
        final = kod_v3.finalize_record_quality(record, meta, 78, title_from_core=False)
        self.assertTrue(final.online_checked)
        self.assertTrue(final.online_applied)
        self.assertNotIn("uzupelnione-online", final.review_reasons)
        self.assertFalse(final.needs_review)

    def test_advisory_review_reason_does_not_force_review(self) -> None:
        record = kod_v3.BookRecord(
            path=Path("x.epub"),
            author="Author",
            series="Series",
            volume=(1, "00"),
            title="Title",
            source="test",
            identifiers=[],
            notes=[],
            confidence=80,
            review_reasons=["blad-odczytu-metadanych"],
            decision_reasons=[],
        )
        self.assertFalse(record.needs_review)

    def test_crossref_requires_isbn(self) -> None:
        meta = make_meta("Only Title", title="Only Title", creators=["Author"], identifiers=[])
        with mock.patch.object(kod_v3, "online_query") as online_query:
            self.assertEqual(kod_v3.crossref_candidates(meta, 2.0), [])
        online_query.assert_not_called()

    def test_lubimyczytac_single_source_is_best_effort(self) -> None:
        meta = make_meta("Some Book", title="Some Book")
        best = kod_v3.pick_best_online_match(
            meta,
            [kod_v3.OnlineCandidate("lubimyczytac", "lubimyczytac", "Some Book", ["Author"], [], 320, "title-author-exact")],
        )
        self.assertIsNotNone(best)
        assert best is not None
        self.assertIn("best-effort", best.reason)

    def test_lubimyczytac_html_parser_extracts_title_and_author(self) -> None:
        parser = kod_v3.LubimyczytacSearchParser()
        parser.feed(
            '<a class="authorAllBooks__singleTextTitle" href="/x">Bibliomancer</a>'
            '<div class="authorAllBooks__singleTextAuthor"><a href="/a">James Hunter</a></div>'
        )
        parser.close()
        self.assertEqual(parser.results, [("Bibliomancer", ["James Hunter"])])

    def test_online_cache_persists_to_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "cache.json"
            with mock.patch.object(kod_v3, "ONLINE_CACHE_PATH", cache_path):
                with kod_v3.ONLINE_CACHE_LOCK:
                    kod_v3.ONLINE_CACHE.clear()
                    kod_v3.ONLINE_CACHE["https://example.test"] = {"ok": True}
                kod_v3.save_online_cache()
                with kod_v3.ONLINE_CACHE_LOCK:
                    kod_v3.ONLINE_CACHE.clear()
                kod_v3.load_online_cache()
                with kod_v3.ONLINE_CACHE_LOCK:
                    self.assertEqual(kod_v3.ONLINE_CACHE["https://example.test"], {"ok": True})

    def test_flush_online_cache_if_needed_respects_buffer_until_forced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "cache.json"
            with mock.patch.object(kod_v3, "ONLINE_CACHE_PATH", cache_path):
                with kod_v3.ONLINE_CACHE_LOCK:
                    kod_v3.ONLINE_CACHE.clear()
                    kod_v3.ONLINE_CACHE["https://example.test"] = {"ok": True}
                    kod_v3.ONLINE_CACHE_DIRTY = True
                    kod_v3.ONLINE_CACHE_PENDING_WRITES = 1
                    kod_v3.ONLINE_CACHE_LAST_SAVE = time.perf_counter()
                kod_v3.flush_online_cache_if_needed(force=False)
                self.assertFalse(cache_path.exists())
                kod_v3.flush_online_cache_if_needed(force=True)
                self.assertTrue(cache_path.exists())

    def test_online_fetch_caches_failures_temporarily(self) -> None:
        with mock.patch.object(kod_v3.urllib.request, "urlopen", side_effect=OSError("timeout")) as urlopen:
            self.assertIsNone(kod_v3.online_fetch("https://example.test", 1.0, kind="json"))
            self.assertIsNone(kod_v3.online_fetch("https://example.test", 1.0, kind="json"))
        self.assertEqual(urlopen.call_count, 1)

    def test_save_online_cache_keeps_dirty_flags_on_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "cache.json"
            with mock.patch.object(kod_v3, "ONLINE_CACHE_PATH", cache_path):
                with kod_v3.ONLINE_CACHE_LOCK:
                    kod_v3.ONLINE_CACHE.clear()
                    kod_v3.ONLINE_CACHE["json:https://example.test"] = {"ok": True}
                    kod_v3.ONLINE_CACHE_DIRTY = True
                    kod_v3.ONLINE_CACHE_PENDING_WRITES = 2
                with mock.patch.object(Path, "write_text", side_effect=OSError("disk-full")):
                    with self.assertRaises(OSError):
                        kod_v3.save_online_cache()
                with kod_v3.ONLINE_CACHE_LOCK:
                    self.assertTrue(kod_v3.ONLINE_CACHE_DIRTY)
                    self.assertEqual(kod_v3.ONLINE_CACHE_PENDING_WRITES, 2)

    def test_apply_flag_defaults_to_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            (folder / "Book 1 - Author.epub").touch()
            code, lines = kod_v3.run_job(
                folder,
                apply_changes=False,
                use_online=False,
                providers=[],
                timeout=1.0,
                limit=0,
            )
            self.assertEqual(code, 0)
            self.assertIn("MODE=DRY-RUN", lines)
            self.assertTrue(any(line.startswith("PROFILE_TOTAL_MS=") for line in lines))

    def test_run_job_parallelizes_infer_record_across_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            for index in range(6):
                (folder / f"Book {index}.epub").touch()
            thread_ids: set[int] = set()
            lock = threading.Lock()

            def fake_infer(meta, use_online, providers, timeout):
                time.sleep(0.05)
                with lock:
                    thread_ids.add(threading.get_ident())
                return kod_v3.BookRecord(
                    path=meta.path,
                    author="Author",
                    series="Series",
                    volume=(1, "00"),
                    title=meta.stem,
                    source="test",
                    identifiers=[],
                    notes=[],
                    confidence=95,
                    review_reasons=[],
                    decision_reasons=[],
                )

            with mock.patch.object(kod_v3, "infer_record", side_effect=fake_infer):
                code, _ = kod_v3.run_job(
                    folder,
                    apply_changes=False,
                    use_online=False,
                    providers=[],
                    timeout=1.0,
                    limit=0,
                )
            self.assertEqual(code, 0)
            self.assertGreater(len(thread_ids), 1)

    def test_main_uses_gui_by_default(self) -> None:
        with mock.patch.object(sys, "argv", ["ebookRen.py"]):
            with mock.patch.object(kod_v3, "launch_gui", return_value=0) as launch_gui:
                code = kod_v3.main()
        self.assertEqual(code, 0)
        launch_gui.assert_called_once()
        self.assertEqual(launch_gui.call_args.args[0], kod_v3.DEFAULT_SOURCE_FOLDER)
        self.assertEqual(launch_gui.call_args.args[-2], False)
        self.assertEqual(launch_gui.call_args.args[-1], 2)

    def test_main_passes_online_flag_to_gui_default(self) -> None:
        with mock.patch.object(sys, "argv", ["ebookRen.py", "--online"]):
            with mock.patch.object(kod_v3, "launch_gui", return_value=0) as launch_gui:
                code = kod_v3.main()
        self.assertEqual(code, 0)
        self.assertEqual(launch_gui.call_args.args[-2], True)

    def test_parser_accepts_cli_and_apply_flags(self) -> None:
        with mock.patch.object(sys, "argv", ["ebookRen.py", "--cli", "--apply"]):
            args = kod_v3.parse_args()
        self.assertTrue(args.cli)
        self.assertTrue(args.apply)

    def test_parser_rejects_apply_and_dry_run_together(self) -> None:
        with mock.patch.object(sys, "argv", ["ebookRen.py", "--apply", "--dry-run"]):
            with self.assertRaises(SystemExit):
                kod_v3.parse_args()

    def test_parser_accepts_online_workers_flag(self) -> None:
        with mock.patch.object(sys, "argv", ["ebookRen.py", "--cli", "--online-workers", "2"]):
            args = kod_v3.parse_args()
        self.assertEqual(args.online_workers, 2)

    def test_parser_defaults_match_requested_gui_profile(self) -> None:
        with mock.patch.object(sys, "argv", ["ebookRen.py"]):
            args = kod_v3.parse_args()
        self.assertEqual(args.folder, kod_v3.DEFAULT_SOURCE_FOLDER)
        self.assertFalse(args.online)
        self.assertEqual(args.online_workers, 2)

    def test_to_last_first_keeps_multiword_surname_particles(self) -> None:
        self.assertEqual(kod_v3.to_last_first("Ludwig van Beethoven"), "van Beethoven Ludwig")

    def test_build_undo_plan_uses_raw_csv_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            report_path = folder / "log.csv"
            with report_path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.writer(handle, delimiter=";")
                writer.writerow(["source_name", "target_name", "mode", "execution_status"])
                writer.writerow([" Book-.epub ", " Target-.epub ", "apply", "renamed"])
            plan = kod_v3.build_undo_plan(report_path, folder)
            self.assertEqual(plan.moves[0].source.name, " Target-.epub ")
            self.assertEqual(plan.moves[0].destination.name, " Book-.epub ")

    def test_review_required_records_are_not_renamed_in_apply_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            source = folder / "A Touch of Power Omnibus -- Jay Boyce.epub"
            source.touch()
            code, lines = kod_v3.run_job(
                folder,
                apply_changes=True,
                use_online=False,
                providers=[],
                timeout=1.0,
                limit=0,
            )
            self.assertEqual(code, 0)
            self.assertIn("TO_WRITE=0", lines)
            self.assertTrue(source.exists())

    def test_apply_report_contains_actual_execution_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            source = folder / "Advent (Red Mage Book 1) -- Xander Boyce.mobi"
            source.touch()
            code, lines = kod_v3.run_job(
                folder,
                apply_changes=True,
                use_online=False,
                providers=[],
                timeout=1.0,
                limit=0,
            )
            self.assertEqual(code, 0)
            report_line = next(line for line in lines if line.startswith("REPORT="))
            report_path = Path(report_line.split("=", 1)[1])
            with report_path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle, delimiter=";"))
            self.assertTrue(rows)
            self.assertEqual(rows[0]["execution_status"], "renamed")
            self.assertEqual(rows[0]["online_checked"], "no")
            self.assertEqual(rows[0]["online_applied"], "no")

    def test_apply_copy_mode_writes_to_destination_and_keeps_source(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp:
            source_folder = Path(src_tmp)
            destination_folder = Path(dst_tmp)
            source = source_folder / "Advent (Red Mage Book 1) -- Xander Boyce.mobi"
            source.touch()
            code, lines = kod_v3.run_job(
                source_folder,
                destination_folder=destination_folder,
                apply_changes=True,
                use_online=False,
                providers=[],
                timeout=1.0,
                limit=0,
            )
            self.assertEqual(code, 0)
            self.assertTrue(source.exists())
            copied = destination_folder / "Boyce Xander - Red Mage - Tom 01.00 - Advent.mobi"
            self.assertTrue(copied.exists())
            self.assertIn("OPERATION=COPY", lines)
            self.assertIn("INFER_WORKERS=1", lines)
            self.assertIn("ONLINE_HTTP_SLOTS=4", lines)

    def test_copy_report_contains_copy_operation(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp:
            source_folder = Path(src_tmp)
            destination_folder = Path(dst_tmp)
            source = source_folder / "Advent (Red Mage Book 1) -- Xander Boyce.mobi"
            source.touch()
            code, lines = kod_v3.run_job(
                source_folder,
                destination_folder=destination_folder,
                apply_changes=True,
                use_online=False,
                providers=[],
                timeout=1.0,
                limit=0,
            )
            self.assertEqual(code, 0)
            report_line = next(line for line in lines if line.startswith("REPORT="))
            report_path = Path(report_line.split("=", 1)[1])
            with report_path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle, delimiter=";"))
            self.assertEqual(rows[0]["operation"], "copy")
            self.assertEqual(rows[0]["execution_status"], "copied")
            self.assertEqual(rows[0]["target_folder"], str(destination_folder))

    def test_filename_for_folder_respects_destination_length_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source_folder = Path(tmp) / "src"
            destination_folder = Path(tmp) / ("dest_" + ("x" * 120))
            source_folder.mkdir()
            record = kod_v3.BookRecord(
                path=source_folder / "source.epub",
                author="Author",
                series="Series",
                volume=(1, "00"),
                title="Very Long Title " * 20,
                source="test",
                identifiers=[],
                notes=[],
                confidence=95,
                review_reasons=[],
                decision_reasons=[],
            )
            record.output_folder = source_folder
            source_name = record.filename
            record.output_folder = destination_folder
            destination_name = record.filename
            self.assertLessEqual(len(str(destination_folder / destination_name)), 240)
            self.assertLessEqual(len(destination_name), len(source_name))

    def test_dedupe_uses_suffix_one_for_first_collision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            conflict_name = "Author - Series - Tom 01.00 - Title.epub"
            (folder / conflict_name).touch()
            record = kod_v3.BookRecord(
                path=folder / "source.epub",
                author="Author",
                series="Series",
                volume=(1, "00"),
                title="Title",
                source="test",
                identifiers=[],
                notes=[],
                confidence=95,
                review_reasons=[],
                decision_reasons=[],
            )
            deduped = kod_v3.dedupe_destinations([record], folder)
            self.assertEqual(deduped[0].filename, "Author - Series - Tom 01.00 - Title (1).epub")


if __name__ == "__main__":
    unittest.main()
