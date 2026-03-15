import csv
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import fs_ops
from models_core import RenameMove


class FsOpsTests(unittest.TestCase):
    def test_build_moves_uses_copy_for_different_target_folder(self) -> None:
        record = type("Record", (), {"path": Path("src") / "source.epub", "filename": "target.epub"})()

        moves = fs_ops.build_moves([record], Path("src"), Path("dst"), None, "20260310_120000")

        self.assertEqual(len(moves), 1)
        self.assertEqual(moves[0].operation, "copy")
        self.assertIsNone(moves[0].temp)

    def test_build_moves_adds_archive_move_when_archive_folder_is_set(self) -> None:
        record = type(
            "Record",
            (),
            {
                "path": Path("src") / "source.epub",
                "filename": "target.epub",
                "archive_source_path": Path("archive") / "source.epub",
            },
        )()

        moves = fs_ops.build_moves([record], Path("src"), Path("dst"), Path("archive"), "20260310_120000")

        self.assertEqual([move.operation for move in moves], ["copy", "move"])
        self.assertEqual(moves[1].destination, Path("archive") / "source.epub")

    def test_build_undo_plan_turns_copied_rows_into_delete_moves(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report_path = root / "rename_books_log.csv"
            with report_path.open("w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.DictWriter(
                    handle,
                    delimiter=";",
                    fieldnames=["source_name", "target_name", "source_folder", "target_folder", "mode", "execution_status", "operation"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "source_name": "source.epub",
                        "target_name": "target.epub",
                        "source_folder": str(root / "src"),
                        "target_folder": str(root / "dst"),
                        "mode": "apply",
                        "execution_status": "copied",
                        "operation": "copy",
                    }
                )

            plan = fs_ops.build_undo_plan(report_path, root)
            self.assertEqual(len(plan.moves), 1)
            self.assertEqual(plan.moves[0].operation, "delete")
            self.assertIn("__tmp_undo_delete_", plan.moves[0].temp.name)

    def test_build_undo_plan_turns_copy_archive_rows_into_move_and_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report_path = root / "rename_books_log.csv"
            archive_folder = root / "done"
            with report_path.open("w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.DictWriter(
                    handle,
                    delimiter=";",
                    fieldnames=[
                        "source_name",
                        "target_name",
                        "source_folder",
                        "target_folder",
                        "archive_source_name",
                        "archive_source_folder",
                        "mode",
                        "execution_status",
                        "operation",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "source_name": "source.epub",
                        "target_name": "target.epub",
                        "source_folder": str(root / "src"),
                        "target_folder": str(root / "dst"),
                        "archive_source_name": "source.epub",
                        "archive_source_folder": str(archive_folder),
                        "mode": "apply",
                        "execution_status": "copied+archived",
                        "operation": "copy+archive",
                    }
                )

            plan = fs_ops.build_undo_plan(report_path, root)
            self.assertEqual([move.operation for move in plan.moves], ["move", "delete"])

    def test_execute_moves_rolls_back_created_copy_when_later_copy_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first_source = root / "one.epub"
            second_source = root / "two.epub"
            first_dest = root / "out-one.epub"
            second_dest = root / "out-two.epub"
            first_source.write_text("one", encoding="utf-8")
            second_source.write_text("two", encoding="utf-8")
            moves = [
                RenameMove(first_source, None, first_dest, None, "copy"),
                RenameMove(second_source, None, second_dest, None, "copy"),
            ]

            original_copy2 = fs_ops.shutil.copy2
            call_count = {"value": 0}

            def flaky_copy(source, destination, *args, **kwargs):
                call_count["value"] += 1
                if call_count["value"] == 2:
                    raise OSError("boom")
                return original_copy2(source, destination, *args, **kwargs)

            with mock.patch.object(fs_ops.shutil, "copy2", side_effect=flaky_copy):
                errors = fs_ops.execute_moves(moves)

            self.assertTrue(errors)
            self.assertFalse(first_dest.exists())
            self.assertFalse(second_dest.exists())

    def test_execute_moves_rolls_back_moved_files_when_later_move_fails(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp:
            source_root = Path(src_tmp)
            destination_root = Path(dst_tmp)
            first_source = source_root / "one.epub"
            second_source = source_root / "two.epub"
            first_dest = destination_root / "out-one.epub"
            second_dest = destination_root / "out-two.epub"
            first_source.write_text("one", encoding="utf-8")
            second_source.write_text("two", encoding="utf-8")
            moves = [
                RenameMove(first_source, None, first_dest, None, "move"),
                RenameMove(second_source, None, second_dest, None, "move"),
            ]

            original_move = fs_ops.shutil.move
            call_count = {"value": 0}

            def flaky_move(source, destination, *args, **kwargs):
                call_count["value"] += 1
                if call_count["value"] == 2:
                    raise OSError("boom")
                return original_move(source, destination, *args, **kwargs)

            with mock.patch.object(fs_ops.shutil, "move", side_effect=flaky_move):
                errors = fs_ops.execute_moves(moves)

            self.assertTrue(errors)
            self.assertTrue(first_source.exists())
            self.assertTrue(second_source.exists())
            self.assertFalse(first_dest.exists())
            self.assertFalse(second_dest.exists())

    def test_execute_moves_rolls_back_copied_files_when_archive_move_fails(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp, tempfile.TemporaryDirectory() as archive_tmp:
            source_root = Path(src_tmp)
            destination_root = Path(dst_tmp)
            archive_root = Path(archive_tmp)
            source = source_root / "one.epub"
            source.write_text("one", encoding="utf-8")
            copy_move = RenameMove(source, None, destination_root / "out-one.epub", None, "copy")
            archive_move = RenameMove(source, None, archive_root / "one.epub", None, "move")

            def flaky_move(source_path, destination_path, *args, **kwargs):
                raise OSError("boom")

            with mock.patch.object(fs_ops.shutil, "move", side_effect=flaky_move):
                errors = fs_ops.execute_moves([copy_move, archive_move])

            self.assertTrue(errors)
            self.assertTrue(source.exists())
            self.assertFalse(copy_move.destination.exists())
            self.assertFalse(archive_move.destination.exists())


if __name__ == "__main__":
    unittest.main()
