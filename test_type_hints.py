import inspect
import typing
import unittest

import infer_engine
import infer_flow
import job_runner
import runtime_metadata
import runtime_online


class TypeHintsTests(unittest.TestCase):
    def test_public_functions_resolve_type_hints(self) -> None:
        modules = [infer_engine, infer_flow, job_runner, runtime_metadata, runtime_online]
        for module in modules:
            for name, obj in inspect.getmembers(module):
                if name.startswith("_"):
                    continue
                if inspect.isfunction(obj):
                    typing.get_type_hints(obj)


if __name__ == "__main__":
    unittest.main()
