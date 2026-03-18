from __future__ import annotations

import argparse
import time
from pathlib import Path

import author_catalog


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Buduje binarny cache runtime dla katalogu autorow.")
    parser.add_argument("--input", default="author_patterns.csv", help="Sciezka do katalogu autorow w CSV.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    input_path = Path(args.input).resolve()
    start = time.perf_counter()
    catalog = author_catalog.load_author_catalog(input_path)
    elapsed = time.perf_counter() - start
    cache_path = author_catalog.catalog_cache_path(input_path)

    print(f"input={input_path}")
    print(f"cache={cache_path}")
    print(f"aliases={len(catalog.aliases_to_canonical)}")
    print(f"initial_buckets={len(catalog.initials_to_canonical)}")
    print(f"first_token_keys={len(catalog.first_token_keys)}")
    print(f"last_token_keys={len(catalog.last_token_keys)}")
    print(f"seconds={elapsed:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
