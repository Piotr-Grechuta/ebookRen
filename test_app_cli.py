import unittest
from pathlib import Path
from unittest import mock

import app_cli
import app_runtime


class AppCliTests(unittest.TestCase):
    def test_parse_args_accepts_cli_and_apply(self) -> None:
        args = app_cli.parse_args(["--cli", "--apply"])
        self.assertTrue(args.cli)
        self.assertTrue(args.apply)

    def test_main_uses_gui_by_default(self) -> None:
        with mock.patch.object(app_cli.app_gui, "launch_gui", return_value=0) as launch_gui:
            code = app_cli.main([])
        self.assertEqual(code, 0)
        launch_gui.assert_called_once()
        self.assertEqual(launch_gui.call_args.args[0], app_runtime.DEFAULT_SOURCE_FOLDER)

    def test_main_runs_cli_mode(self) -> None:
        with mock.patch.object(app_runtime, "run_job", return_value=(0, ["ok"])) as run_job:
            code = app_cli.main(["--cli", "--folder", str(Path.cwd())])
        self.assertEqual(code, 0)
        run_job.assert_called_once()


if __name__ == "__main__":
    unittest.main()
