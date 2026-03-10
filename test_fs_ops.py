import csv
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import fs_ops
from models_core import RenameMove


class FsOpsTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
