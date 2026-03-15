import json
import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path

import job_runner


@dataclass
class DummyRecord:
    path: Path
    filename: str
    notes: list[str] = field(default_factory=list)
    confidence: int = 80
    review_reasons: list[str] = field(default_factory=list)
    decision_reasons: list[str] = field(default_factory=list)
    author: str = "Author"
    series: str = "Series"
    volume: tuple[int, str] | None = (1, "00")
    title: str = "Title"
    genre: str = ""
    source: str = "test"
    identifiers: list[str] = field(default_factory=list)
    online_checked: bool = False
    online_applied: bool = False
    output_folder: Path | None = None
    archive_source_path: Path | None = None

    @property
    def needs_review(self) -> bool:
        return False


class JobRunnerTests(unittest.TestCase):
    def test_processed_manifest_tracks_successful_source_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            source = folder / "source.epub"
            source.write_text("x", encoding="utf-8")

            manifest: dict[str, dict[str, object]] = {}
            job_runner.update_processed_manifest_entry(
                manifest,
                source,
                status="copied",
                target_name="Author - Series - Tom 01.00 - Title.epub",
            )
            job_runner.save_processed_manifest(folder, manifest)

            loaded = job_runner.load_processed_manifest(folder)
            self.assertTrue(job_runner.should_skip_processed_file(source, loaded))

            source.write_text("changed", encoding="utf-8")
            self.assertFalse(job_runner.should_skip_processed_file(source, loaded))

    def test_load_processed_manifest_migrates_old_path_key_to_filename_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            source = folder / "source.epub"
            source.write_text("x", encoding="utf-8")
            size = source.stat().st_size
            mtime_ns = source.stat().st_mtime_ns
            manifest_path = folder / job_runner.PROCESSED_MANIFEST_NAME
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "entries": {
                            str(source): {
                                "status": "copied",
                                "target_name": "Target.epub",
                                "size": size,
                                "mtime_ns": mtime_ns,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            loaded = job_runner.load_processed_manifest(folder)

            self.assertIn("source.epub", loaded)
            self.assertEqual(loaded["source.epub"]["source_name"], "source.epub")
            self.assertTrue(job_runner.should_skip_processed_file(source, loaded))

    def test_build_progress_lines_includes_source_and_target(self) -> None:
        record = DummyRecord(
            path=Path("source.epub"),
            filename="Author - Series - Tom 01.00 - Title.epub",
            source="title:leading-index-dotted",
            decision_reasons=["inference:title:leading-index-dotted", "online-role-title:yes"],
            online_checked=True,
            online_applied=True,
        )
        lines = job_runner.build_progress_lines(record, index=1, total=3, target_folder=Path("out"))
        self.assertEqual(lines[0], "[1/3] source.epub")
        self.assertIn("title:leading-index-dotted", lines[1])
        self.assertIn("online-role-title", lines[1])
        self.assertEqual(lines[2], "  wynik: source.epub -> Author - Series - Tom 01.00 - Title.epub")

    def test_dedupe_destinations_moves_existing_conflict_to_dubel_folder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            (folder / "Author - Series - Tom 01.00 - Title.epub").write_text("x", encoding="utf-8")
            record = DummyRecord(path=folder / "source.epub", filename="Author - Series - Tom 01.00 - Title.epub")
            deduped = job_runner.dedupe_destinations(
                [record],
                folder,
                is_supported_book_file=lambda path: path.suffix == ".epub",
                make_record_clone=lambda base, **kwargs: DummyRecord(
                    path=base.path,
                    filename=f"Author - Series - Tom 01.00 - Title {kwargs['filename_suffix']}.epub" if kwargs.get("filename_suffix") else base.filename,
                    notes=kwargs["notes"],
                    confidence=kwargs["confidence"],
                    review_reasons=kwargs["review_reasons"],
                    decision_reasons=kwargs["decision_reasons"],
                    output_folder=kwargs.get("output_folder"),
                ),
            )
        self.assertEqual(deduped[0].filename, "Author - Series - Tom 01.00 - Title.epub")
        self.assertEqual(deduped[0].output_folder, folder / "dubel")

    def test_dedupe_destinations_adds_suffix_inside_dubel_when_name_already_taken(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            dubel = folder / "dubel"
            dubel.mkdir()
            (folder / "Author - Series - Tom 01.00 - Title.epub").write_text("x", encoding="utf-8")
            (dubel / "Author - Series - Tom 01.00 - Title.epub").write_text("x", encoding="utf-8")
            record = DummyRecord(path=folder / "source.epub", filename="Author - Series - Tom 01.00 - Title.epub")
            deduped = job_runner.dedupe_destinations(
                [record],
                folder,
                is_supported_book_file=lambda path: path.suffix == ".epub",
                make_record_clone=lambda base, **kwargs: DummyRecord(
                    path=base.path,
                    filename=f"Author - Series - Tom 01.00 - Title {kwargs['filename_suffix']}.epub" if kwargs.get("filename_suffix") else base.filename,
                    notes=kwargs["notes"],
                    confidence=kwargs["confidence"],
                    review_reasons=kwargs["review_reasons"],
                    decision_reasons=kwargs["decision_reasons"],
                    output_folder=kwargs.get("output_folder", base.output_folder),
                ),
            )
        self.assertEqual(deduped[0].filename, "Author - Series - Tom 01.00 - Title (1).epub")
        self.assertEqual(deduped[0].output_folder, dubel)

    def test_write_report_emits_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.csv"
            row = DummyRecord(path=Path(tmp) / "source.epub", filename="target.epub")
            job_runner.write_report(
                report,
                [row],
                dry_run=True,
                source_folder=Path(tmp),
                target_folder=Path(tmp),
                operation="rename",
                format_volume=lambda volume: "Tom 01.00",
            )
            content = report.read_text(encoding="utf-8-sig")
        self.assertIn("source_name;target_name", content)
        self.assertIn("source.epub;target.epub", content)

    def test_build_manifest_progress_lines_reports_matches_for_current_folder(self) -> None:
        files = [Path("source.epub"), Path("other.epub")]
        manifest = {
            "source.epub": {"source_name": "source.epub", "status": "copied"},
            "missing.epub": {"source_name": "missing.epub", "status": "copied"},
        }

        lines = job_runner.build_manifest_progress_lines(
            manifest,
            files=files,
            skip_previously_processed=True,
        )

        self.assertEqual(lines[0], "Manifest przetworzonych plikow")
        self.assertIn("wpisy: 2", lines[1])
        self.assertIn("zgodne z obecnym folderem: 1", lines[2])
        self.assertIn("pomijanie: wlaczone", lines[3])

    def test_sort_paths_windows_style_matches_windows_logical_order(self) -> None:
        paths = [
            Path("[10] a.epub"),
            Path("A.epub"),
            Path("[2] a.epub"),
            Path("(03) a.epub"),
        ]

        sorted_paths = job_runner.sort_paths_windows_style(paths)

        self.assertEqual(
            [path.name for path in sorted_paths],
            ["(03) a.epub", "[2] a.epub", "[10] a.epub", "A.epub"],
        )

    def test_assign_archive_source_paths_avoids_existing_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            archive_folder = Path(tmp)
            (archive_folder / "source.epub").write_text("old", encoding="utf-8")
            records = [DummyRecord(path=Path("src") / "source.epub", filename="target.epub")]

            job_runner.assign_archive_source_paths(records, archive_folder)

            self.assertEqual(records[0].archive_source_path, archive_folder / "source (1).epub")


if __name__ == "__main__":
    unittest.main()
