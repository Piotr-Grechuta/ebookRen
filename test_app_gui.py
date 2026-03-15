from pathlib import Path
import tempfile
import unittest
from unittest import mock

import app_gui


class AppGuiTests(unittest.TestCase):
    def test_load_gui_state_returns_empty_dict_for_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "gui_state.json"
            with mock.patch.object(app_gui, "gui_state_path", return_value=state_path):
                self.assertEqual(app_gui.load_gui_state(), {})

    def test_save_and_load_gui_state_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "gui_state.json"
            with mock.patch.object(app_gui, "gui_state_path", return_value=state_path):
                app_gui.save_gui_state(
                    source_folder="g:\\books",
                    destination_folder="g:\\books_out",
                    archive_folder="g:\\books_done",
                    online_mode="PL+",
                )

                state = app_gui.load_gui_state()

        self.assertEqual(
            state,
            {
                "source_folder": "g:\\books",
                "destination_folder": "g:\\books_out",
                "archive_folder": "g:\\books_done",
                "online_mode": "PL+",
            },
        )


if __name__ == "__main__":
    unittest.main()
