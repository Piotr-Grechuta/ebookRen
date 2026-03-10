from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Sequence

import app_gui
import app_runtime as runtime


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"{runtime.APP_NAME}: rename/copy ebook files with optional online enrichment.")
    parser.add_argument("--folder", default=runtime.DEFAULT_SOURCE_FOLDER, help="Folder z plikami ebookow.")
    parser.add_argument(
        "--destination",
        default="",
        help="Opcjonalny folder docelowy. Jesli podany i rozny od source, pliki beda kopiowane z nowa nazwa.",
    )
    parser.add_argument("--cli", action="store_true", help="Uruchom tryb tekstowy zamiast domyslnego GUI.")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--apply", action="store_true", help="Wykonaj zmiany na dysku. Domyslnie tylko podglad.")
    mode_group.add_argument("--dry-run", action="store_true", help="Tylko podglad bez zmiany nazw plikow.")
    parser.add_argument(
        "--online",
        action="store_true",
        help="Uzupelniaj brakujace title/author z publicznych API (Google Books, Open Library).",
    )
    parser.add_argument(
        "--providers",
        default=runtime.DEFAULT_PROVIDERS,
        help="Lista providerow online rozdzielona przecinkami: google, openlibrary, crossref, hathitrust, lubimyczytac.",
    )
    parser.add_argument("--timeout", type=float, default=runtime.DEFAULT_HTTP_TIMEOUT, help="Timeout dla zapytan HTTP.")
    parser.add_argument("--limit", type=int, default=0, help="Przetworz tylko pierwsze N plikow.")
    parser.add_argument(
        "--online-workers",
        type=int,
        default=runtime.DEFAULT_INFER_WORKERS,
        help="Maksymalna liczba rownoleglych rekordow inferencji; rownolegle lookupy HTTP nadal ogranicza semaphore.",
    )
    parser.add_argument(
        "--skip-processed",
        action="store_true",
        help="Pomijaj pliki wejsciowe, ktore maja udany wpis w manifeście JSON i nie zmienily sie od ostatniego przetworzenia.",
    )
    parser.add_argument("--undo", default="", help="Sciezka do logu CSV z wykonanego rename, aby cofnac zmiany.")
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    runtime.configure_logging()
    args = parse_args(argv)

    if args.undo:
        report_path = Path(args.undo)
        if not report_path.exists():
            runtime.LOGGER.error(f"Log undo nie istnieje: {report_path}")
            return 2
        folder_hint = Path(args.folder) if args.folder else None
        if folder_hint is not None and not folder_hint.exists():
            folder_hint = None
        return runtime.execute_undo(report_path, folder_hint)

    if not args.cli:
        return app_gui.launch_gui(
            args.folder,
            args.destination,
            args.providers,
            args.timeout,
            args.limit,
            args.online,
            args.online_workers,
            default_skip_processed=args.skip_processed,
        )

    folder = Path(args.folder)
    destination = Path(args.destination) if args.destination else None
    providers = [item.strip().lower() for item in args.providers.split(",") if item.strip()]
    code, lines = runtime.run_job(
        folder,
        destination_folder=destination,
        apply_changes=args.apply,
        use_online=args.online,
        providers=providers,
        timeout=args.timeout,
        limit=args.limit,
        online_workers=args.online_workers,
        skip_previously_processed=args.skip_processed,
    )
    runtime.log_lines(lines, level=logging.ERROR if code == 2 else logging.INFO)
    return code
