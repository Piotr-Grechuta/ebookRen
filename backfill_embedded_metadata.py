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
    return parser


def iter_book_files(folder: Path, *, recursive: bool) -> list[Path]:
    iterator = folder.rglob("*") if recursive else folder.iterdir()
    files = [path for path in iterator if path.is_file() and runtime.is_supported_book_file(path)]
    return sorted(files, key=lambda path: str(path).lower())


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

    files = iter_book_files(folder, recursive=args.recursive)
    if args.limit and args.limit > 0:
        files = files[: args.limit]
    if not files:
        print("Brak obslugiwanych plikow ebook.")
        return 0

    runtime.configure_logging()
    written = 0
    skipped = 0
    errors = 0

    for index, path in enumerate(files, start=1):
        meta = runtime.read_book_metadata(path)
        record = runtime.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        summary = (
            f"[{index}/{len(files)}] {path.name} -> "
            f"author={record.author} | series={record.series} | volume={record.volume} | title={record.title}"
        )
        if not args.apply:
            print(f"DRY  {summary}")
            continue
        try:
            runtime.write_book_metadata(path, record, extra_tags=extra_tags)
            written += 1
            print(f"OK   {summary}")
        except ValueError as exc:
            skipped += 1
            print(f"SKIP {summary} | {exc}")
        except Exception as exc:
            errors += 1
            print(f"ERR  {summary} | {exc}")

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(
        f"MODE={mode} TOTAL={len(files)} WRITTEN={written} SKIPPED={skipped} ERRORS={errors}"
    )
    if not args.apply:
        print("Dodaj --apply, aby zapisac metadane do plikow.")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
