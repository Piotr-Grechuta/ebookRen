from __future__ import annotations

import argparse
from pathlib import Path

import app_runtime as runtime


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Uzupelnia metadane osadzone w juz przemianowanych ebookach na podstawie wzorca nazwy pliku."
    )
    parser.add_argument("folder", nargs="?", default=".", help="Folder z ebookami")
    parser.add_argument("--recursive", action="store_true", help="Przetwarzaj podfoldery")
    parser.add_argument("--apply", action="store_true", help="Zapisz metadane do plikow")
    parser.add_argument("--tag", action="append", default=[], help="Dodatkowy tag/subject do dopisania, mozna podac wiele razy")
    parser.add_argument("--killim", action="store_true", help="Dodaj tag Killim")
    parser.add_argument("--limit", type=int, default=0, help="Limit liczby plikow, 0 = bez limitu")
    parser.add_argument("--calibre-folder", default="", help="Folder instalacji calibre, jesli nie ma go w PATH")
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()
    folder = Path(args.folder).expanduser()
    if not folder.exists():
        print(f"Folder nie istnieje: {folder}")
        return 2

    extra_tags = list(args.tag or [])
    if args.killim:
        extra_tags.append("Killim")
    calibre_folder = Path(args.calibre_folder).expanduser() if str(args.calibre_folder).strip() else None

    code, lines = runtime.run_metadata_backfill(
        folder,
        recursive=args.recursive,
        tags_text=", ".join(extra_tags),
        apply_changes=args.apply,
        limit=args.limit,
        calibre_folder=calibre_folder,
    )
    for line in lines:
        print(line)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
