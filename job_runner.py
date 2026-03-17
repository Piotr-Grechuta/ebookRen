from __future__ import annotations

import ctypes
import csv
from functools import cmp_to_key
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, TypeAlias

import embedded_metadata as embedded_metadata_mod


RecordLike: TypeAlias = Any
IsSupportedBookFile: TypeAlias = Callable[[Path], bool]
MakeRecordClone: TypeAlias = Callable[..., RecordLike]
FormatVolume: TypeAlias = Callable[[tuple[int, str] | None], str]
ReadBookMetadata: TypeAlias = Callable[[Path], Any]
InferRecord: TypeAlias = Callable[..., RecordLike]
BuildMoves: TypeAlias = Callable[[list[RecordLike], Path, Path, Path | None, str], list[Any]]
ExecuteMoves: TypeAlias = Callable[[list[Any]], list[str]]
WriteReportFn: TypeAlias = Callable[..., None]
FlushOnlineCache: TypeAlias = Callable[..., None]
EmitProgress: TypeAlias = Callable[[str], None]
WriteEmbeddedMetadata: TypeAlias = Callable[[Path, RecordLike], None]
ProcessedManifest: TypeAlias = dict[str, dict[str, Any]]

PROCESSED_MANIFEST_NAME = "ebookren_processed.json"
SUCCESSFUL_PROCESSING_STATUSES = {"copied", "copied+archived", "moved", "renamed", "unchanged"}
SUCCESSFUL_METADATA_STATUSES = {"copied", "copied+archived", "renamed", "unchanged"}

if hasattr(ctypes, "windll") and hasattr(ctypes.windll, "shlwapi"):
    _STRCMP_LOGICALW = ctypes.windll.shlwapi.StrCmpLogicalW
else:
    _STRCMP_LOGICALW = None


def compare_paths_windows_style(left: Path, right: Path) -> int:
    if _STRCMP_LOGICALW is not None:
        return int(_STRCMP_LOGICALW(str(left.name), str(right.name)))
    def natural_key(path: Path) -> list[object]:
        parts = re.split(r"(\d+)", str(path.name).lower())
        key: list[object] = []
        for part in parts:
            if not part:
                continue
            key.append(int(part) if part.isdigit() else part)
        return key

    left_key = natural_key(left)
    right_key = natural_key(right)
    if left_key < right_key:
        return -1
    if left_key > right_key:
        return 1
    return 0


def sort_paths_windows_style(paths: list[Path]) -> list[Path]:
    return sorted(paths, key=cmp_to_key(compare_paths_windows_style))


def set_output_folder(records: list[RecordLike], folder: Path) -> list[RecordLike]:
    for record in records:
        record.output_folder = folder
    return records


def actual_output_folder(record: RecordLike, default_folder: Path) -> Path:
    folder = getattr(record, "output_folder", None)
    return folder if isinstance(folder, Path) else default_folder


def target_path_for_record(record: RecordLike, default_folder: Path) -> Path:
    return actual_output_folder(record, default_folder) / record.filename


def planned_embedded_metadata_status(record: RecordLike, target_folder: Path, *, enabled: bool) -> str:
    if not enabled:
        return "disabled"
    suffix = target_path_for_record(record, target_folder).suffix.lower()
    if suffix == ".epub" or suffix in embedded_metadata_mod.CALIBRE_WRITE_FORMATS:
        return "planned"
    return "skip-not-writable"


def write_embedded_metadata_for_record(
    record: RecordLike,
    *,
    target_folder: Path,
    enabled: bool,
    execution_status: dict[Path, str],
    write_book_metadata: WriteEmbeddedMetadata,
) -> tuple[str, str]:
    if not enabled:
        return "disabled", ""
    status = execution_status.get(record.path.resolve(), "")
    if status not in SUCCESSFUL_METADATA_STATUSES:
        return "not-run", ""
    destination = target_path_for_record(record, target_folder)
    suffix = destination.suffix.lower()
    if suffix != ".epub" and suffix not in embedded_metadata_mod.CALIBRE_WRITE_FORMATS:
        return "skip-not-writable", ""
    if not destination.exists():
        return "failed", f"metadata-missing-target:{destination.name}"
    try:
        write_book_metadata(destination, record)
    except Exception as exc:
        return "failed", f"metadata:{destination.name}: {exc}"
    return "written", ""


def _path_key(path: Path) -> str:
    return str(path.absolute()).lower()


def next_available_path(path: Path) -> Path:
    if not path.exists():
        return path
    suffix = path.suffix
    stem = path.stem
    counter = 1
    while True:
        candidate = path.with_name(f"{stem} ({counter}){suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def assign_archive_source_paths(records: list[RecordLike], archive_folder: Path | None) -> list[RecordLike]:
    if archive_folder is None:
        for record in records:
            setattr(record, "archive_source_path", None)
        return records

    reserved: set[str] = set()
    if archive_folder.exists():
        reserved = {_path_key(existing) for existing in archive_folder.iterdir()}

    for record in records:
        candidate = archive_folder / record.path.name
        counter = 0
        while _path_key(candidate) in reserved:
            counter += 1
            candidate = archive_folder / f"{record.path.stem} ({counter}){record.path.suffix}"
        setattr(record, "archive_source_path", candidate)
        reserved.add(_path_key(candidate))
    return records


def dedupe_destinations(
    records: list[RecordLike],
    folder: Path,
    *,
    is_supported_book_file: IsSupportedBookFile,
    make_record_clone: MakeRecordClone,
) -> list[RecordLike]:
    source_paths = {record.path.resolve() for record in records}
    duplicate_folder = folder / "dubel"
    reserved_destinations: set[tuple[Path, str]] = set()
    for candidate_folder in (folder, duplicate_folder):
        existing_items = candidate_folder.iterdir() if candidate_folder.exists() else []
        for existing in existing_items:
            if not is_supported_book_file(existing):
                continue
            if existing.resolve() not in source_paths:
                reserved_destinations.add((candidate_folder.resolve(), existing.name.lower()))

    used_destinations = set(reserved_destinations)
    final: list[RecordLike] = []

    for record in records:
        final_record = record
        destination_folder = actual_output_folder(final_record, folder)
        destination_key = (destination_folder.resolve(), final_record.filename.lower())
        if destination_key in reserved_destinations and destination_folder.resolve() == folder.resolve():
            final_record = make_record_clone(
                record,
                notes=record.notes + ["dedupe-dubel", "existing-file-conflict"],
                confidence=max(0, record.confidence - 3),
                review_reasons=record.review_reasons + ["kolizja-nazwy"],
                decision_reasons=record.decision_reasons + ["dedupe:dubel-folder"],
                output_folder=duplicate_folder,
            )
            destination_folder = actual_output_folder(final_record, folder)
            destination_key = (destination_folder.resolve(), final_record.filename.lower())

        suffix_no = 0
        while destination_key in used_destinations:
            suffix_no += 1
            final_record = make_record_clone(
                final_record,
                notes=final_record.notes + ["dedupe-suffix", "existing-file-conflict"],
                confidence=max(0, final_record.confidence - 5),
                review_reasons=final_record.review_reasons + ["kolizja-nazwy"],
                decision_reasons=final_record.decision_reasons + [f"dedupe:filename-suffix-{suffix_no}"],
                filename_suffix=f"({suffix_no})",
                output_folder=actual_output_folder(final_record, folder),
            )
            destination_folder = actual_output_folder(final_record, folder)
            destination_key = (destination_folder.resolve(), final_record.filename.lower())

        used_destinations.add(destination_key)
        final.append(final_record)

    return final


def write_report(
    path: Path,
    rows: list[RecordLike],
    dry_run: bool,
    source_folder: Path,
    target_folder: Path,
    operation: str,
    *,
    format_volume: FormatVolume,
    execution_status: dict[Path, str] | None = None,
    embedded_metadata_status: dict[Path, str] | None = None,
) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(
            [
                "source_name",
                "target_name",
                "source_folder",
                "target_folder",
                "old_name",
                "new_name",
                "author",
                "series",
                "tom",
                "title",
                "genre",
                "source",
                "identifiers",
                "notes",
                "confidence",
                "review",
                "review_reasons",
                "decision_reasons",
                "online_checked",
                "online_applied",
                "change_status",
                "execution_status",
                "operation",
                "archive_source_name",
                "archive_source_folder",
                "embedded_metadata_status",
                "mode",
            ]
        )
        for row in rows:
            target_folder_for_row = actual_output_folder(row, target_folder)
            target_name = row.filename
            archive_path = getattr(row, "archive_source_path", None)
            status = ""
            if execution_status is not None:
                status = execution_status.get(row.path.resolve(), "")
            metadata_status = ""
            if embedded_metadata_status is not None:
                metadata_status = embedded_metadata_status.get(row.path.resolve(), "")
            if not status:
                if row.needs_review:
                    status = "review-required"
                elif source_folder.resolve() == target_folder_for_row.resolve() and row.path.name == target_name:
                    status = "unchanged"
                else:
                    status = "planned"
            writer.writerow(
                [
                    row.path.name,
                    target_name,
                    str(source_folder),
                    str(target_folder_for_row),
                    row.path.name,
                    target_name,
                    row.author,
                    row.series,
                    format_volume(row.volume),
                    row.title,
                    row.genre,
                    row.source,
                    ",".join(row.identifiers),
                    " | ".join(row.notes),
                    row.confidence,
                    "CHECK" if row.needs_review else "OK",
                    " | ".join(row.review_reasons),
                    " | ".join(row.decision_reasons),
                    "yes" if row.online_checked else "no",
                    "yes" if row.online_applied else "no",
                    operation if row.path.name != target_name or source_folder.resolve() != target_folder_for_row.resolve() else "unchanged",
                    status,
                    operation,
                    archive_path.name if isinstance(archive_path, Path) else "",
                    str(archive_path.parent) if isinstance(archive_path, Path) else "",
                    metadata_status,
                    "dry-run" if dry_run else "apply",
                ]
            )


def processed_manifest_path(folder: Path) -> Path:
    return folder / PROCESSED_MANIFEST_NAME


def processed_file_key(path: Path) -> str:
    return path.name.lower()


def file_signature(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def load_processed_manifest(folder: Path) -> ProcessedManifest:
    path = processed_manifest_path(folder)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    entries = payload.get("entries", {}) if isinstance(payload, dict) else {}
    if not isinstance(entries, dict):
        return {}

    normalized_entries: ProcessedManifest = {}
    for raw_key, raw_entry in entries.items():
        if not isinstance(raw_entry, dict):
            continue
        source_name = str(raw_entry.get("source_name") or "").strip()
        if not source_name:
            source_name = Path(str(raw_key)).name
        normalized_key = source_name.lower()
        if not normalized_key:
            continue
        normalized_entry = dict(raw_entry)
        normalized_entry["source_name"] = source_name
        normalized_entries[normalized_key] = normalized_entry
    return normalized_entries


def save_processed_manifest(folder: Path, entries: ProcessedManifest) -> None:
    path = processed_manifest_path(folder)
    payload = {
        "version": 1,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "entries": entries,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def should_skip_processed_file(path: Path, manifest: ProcessedManifest) -> bool:
    entry = manifest.get(processed_file_key(path))
    if not isinstance(entry, dict):
        return False
    if (entry.get("status") or "").strip().lower() not in SUCCESSFUL_PROCESSING_STATUSES:
        return False
    try:
        size, mtime_ns = file_signature(path)
    except OSError:
        return False
    return entry.get("size") == size and entry.get("mtime_ns") == mtime_ns


def update_processed_manifest_entry(
    manifest: ProcessedManifest,
    path: Path,
    *,
    status: str,
    target_name: str,
    signature: tuple[int, int] | None = None,
) -> None:
    if status not in SUCCESSFUL_PROCESSING_STATUSES:
        return
    if signature is None:
        size, mtime_ns = file_signature(path)
    else:
        size, mtime_ns = signature
    manifest[processed_file_key(path)] = {
        "source_name": path.name,
        "target_name": target_name,
        "status": status,
        "size": size,
        "mtime_ns": mtime_ns,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def build_progress_lines(record: RecordLike, *, index: int, total: int, target_folder: Path) -> list[str]:
    steps: list[str] = []
    inference = next(
        (
            reason.split(":", 1)[1]
            for reason in getattr(record, "decision_reasons", [])
            if reason.startswith("inference:")
        ),
        getattr(record, "source", "") or "nieznana-sciezka",
    )
    if inference:
        steps.append(inference)
    for reason in getattr(record, "decision_reasons", []):
        if reason.startswith("online-role-") and reason.endswith(":yes"):
            steps.append(reason.rsplit(":", 1)[0])
    if getattr(record, "online_checked", False) and not any(step.startswith("online-role-") for step in steps):
        steps.append("online-check")

    deduped_steps: list[str] = []
    for step in steps:
        if step and step not in deduped_steps:
            deduped_steps.append(step)

    review_suffix = " [CHECK]" if getattr(record, "needs_review", False) else ""
    destination = target_path_for_record(record, target_folder)
    try:
        destination_label = str(destination.relative_to(target_folder))
    except ValueError:
        destination_label = destination.name
    lines = [
        f"[{index}/{total}] {record.path.name}{review_suffix}",
        f"  sciezka: {' -> '.join(deduped_steps) if deduped_steps else 'nieznana'}",
        f"  wynik: {record.path.name} -> {destination_label}",
    ]
    archive_path = getattr(record, "archive_source_path", None)
    if isinstance(archive_path, Path):
        lines.append(f"  oryginal: {record.path.name} -> {archive_path.name}")
    return lines


def build_skip_progress_lines(path: Path, *, index: int, total: int) -> list[str]:
    return [
        f"[{index}/{total}] {path.name}",
        "  sciezka: pomijanie-manifest",
        "  wynik: pominięty, plik był już wcześniej przetworzony",
    ]


def build_manifest_progress_lines(
    manifest: ProcessedManifest,
    *,
    files: list[Path],
    skip_previously_processed: bool,
) -> list[str]:
    matched_entries = sum(1 for path in files if processed_file_key(path) in manifest)
    status = "wlaczone" if skip_previously_processed else "wylaczone"
    return [
        "Manifest przetworzonych plikow",
        f"  wpisy: {len(manifest)}",
        f"  zgodne z obecnym folderem: {matched_entries}",
        f"  pomijanie: {status}",
    ]


def emit_file_progress(
    emit_progress: EmitProgress | None,
    *,
    path: Path,
    index: int,
    total: int,
    stage: str,
    detail: str = "",
) -> None:
    if emit_progress is None:
        return
    line = f"[{index}/{total}] {path.name} | etap={stage}"
    if detail:
        line = f"{line} | {detail}"
    emit_progress(line)


def emit_trace_snapshot(
    emit_trace: EmitProgress | None,
    *,
    path: Path,
    stage: str,
    meta: Any | None = None,
    extra_lines: list[str] | None = None,
) -> None:
    if emit_trace is None:
        return
    lines = [path.name, f"  etap: {stage}"]
    if meta is not None:
        lines.append(f"  wejscie: stem={getattr(meta, 'stem', path.stem)}")
        meta_title = getattr(meta, "title", "")
        meta_creators = getattr(meta, "creators", [])
        if meta_title:
            lines.append(f"  meta.title: {meta_title}")
        if meta_creators:
            lines.append(f"  meta.creators: {' | '.join(meta_creators)}")
        meta_series = getattr(meta, "meta_series", "")
        meta_volume = getattr(meta, "meta_volume", None)
        if meta_series or meta_volume is not None:
            lines.append(f"  meta.series: {meta_series or '(brak)'}")
            lines.append(f"  meta.volume: {meta_volume}")
    if extra_lines:
        lines.extend(f"  {item}" for item in extra_lines if item)
    emit_trace("\n".join(lines))


def make_stage_emitter(
    emit_progress: EmitProgress | None,
    *,
    path: Path,
    index: int,
    total: int,
) -> Callable[[str, str], None]:
    def _emit(stage: str, detail: str = "") -> None:
        emit_file_progress(emit_progress, path=path, index=index, total=total, stage=stage, detail=detail)

    return _emit


def call_infer_record(
    infer_record: InferRecord,
    meta: Any,
    *,
    use_online: bool,
    providers: list[str],
    timeout: float,
    online_mode: str,
    emit_stage: Callable[[str, str], None] | None = None,
    emit_trace: Callable[[str], None] | None = None,
) -> RecordLike:
    if emit_stage is None and emit_trace is None:
        return infer_record(meta, use_online=use_online, providers=providers, timeout=timeout, online_mode=online_mode)
    try:
        return infer_record(
            meta,
            use_online=use_online,
            providers=providers,
            timeout=timeout,
            online_mode=online_mode,
            emit_stage=emit_stage,
            emit_trace=emit_trace,
        )
    except TypeError as exc:
        if "emit_stage" not in str(exc) and "emit_trace" not in str(exc) and "online_mode" not in str(exc):
            raise
        return infer_record(meta, use_online=use_online, providers=providers, timeout=timeout)


def run_job(
    folder: Path,
    *,
    destination_folder: Path | None,
    archive_folder: Path | None,
    online_mode: str,
    apply_changes: bool,
    use_online: bool,
    providers: list[str],
    timeout: float,
    limit: int,
    online_workers: int,
    default_infer_workers: int,
    online_http_slots: int,
    is_supported_book_file: IsSupportedBookFile,
    read_book_metadata: ReadBookMetadata,
    infer_record: InferRecord,
    write_book_metadata: WriteEmbeddedMetadata,
    build_moves: BuildMoves,
    execute_moves: ExecuteMoves,
    format_volume: FormatVolume,
    write_report_fn: WriteReportFn,
    set_output_folder_fn: Callable[[list[RecordLike], Path], list[RecordLike]],
    dedupe_destinations_fn: Callable[[list[RecordLike], Path], list[RecordLike]],
    flush_online_cache_if_needed: FlushOnlineCache,
    write_epub_metadata: bool = True,
    emit_progress: EmitProgress | None = None,
    emit_trace: EmitProgress | None = None,
    skip_previously_processed: bool = False,
) -> tuple[int, list[str]]:
    started_at = time.perf_counter()
    if not folder.exists():
        return 2, [f"Folder nie istnieje: {folder}"]
    target_folder = destination_folder if destination_folder is not None else folder
    if archive_folder is not None:
        if target_folder.resolve() == folder.resolve():
            return 2, ["Folder archiwum oryginalow wymaga osobnego folderu docelowego ze zmieniona nazwa."]
        if archive_folder.resolve() == folder.resolve():
            return 2, ["Folder archiwum oryginalow nie moze byc taki sam jak folder zrodlowy."]
        if archive_folder.resolve() == target_folder.resolve():
            return 2, ["Folder archiwum oryginalow nie moze byc taki sam jak folder docelowy zmian nazw."]
    if target_folder.resolve() == folder.resolve():
        operation = "rename"
    elif archive_folder is not None:
        operation = "copy+archive"
    else:
        operation = "copy"
    if apply_changes and operation in {"copy", "copy+archive"}:
        target_folder.mkdir(parents=True, exist_ok=True)
    if apply_changes and archive_folder is not None:
        archive_folder.mkdir(parents=True, exist_ok=True)

    files = sort_paths_windows_style([path for path in folder.iterdir() if is_supported_book_file(path)])
    if not files:
        return 0, ["Brak obslugiwanych plikow ebook."]

    processed_manifest = load_processed_manifest(folder) if (skip_previously_processed or apply_changes) else {}
    manifest_progress_lines = build_manifest_progress_lines(
        processed_manifest,
        files=files,
        skip_previously_processed=skip_previously_processed,
    )
    if emit_progress is not None:
        emit_progress("\n".join(manifest_progress_lines))
    skipped_files: list[Path] = []
    if skip_previously_processed:
        remaining_files: list[Path] = []
        total_candidates = len(files)
        for path in files:
            if should_skip_processed_file(path, processed_manifest):
                skipped_files.append(path)
                if emit_progress is not None:
                    emit_progress("\n".join(build_skip_progress_lines(path, index=len(skipped_files), total=total_candidates)))
                continue
            remaining_files.append(path)
        files = remaining_files
    if limit > 0:
        files = files[:limit]
    if not files:
        lines = [f"Brak plikow do przetworzenia. Pominieto={len(skipped_files)}"]
        if skip_previously_processed:
            lines.append(f"MANIFEST={processed_manifest_path(folder)}")
        return 0, lines

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dry_run = not apply_changes
    report_name = "rename_books_preview" if dry_run else "rename_books_log"
    report_path = folder / f"{report_name}_{stamp}.csv"
    lines: list[str] = []
    lines.extend(manifest_progress_lines)

    if apply_changes:
        infer_workers = 1
        read_ms = 0
        infer_ms = 0
        execute_ms = 0
        records: list = []
        execution_status: dict[Path, str] = {}
        embedded_metadata_status: dict[Path, str] = {}
        errors: list[str] = []
        metadata_errors: list[str] = []
        to_write = 0
        written_total = 0
        metadata_written_total = 0
        source_signatures: dict[Path, tuple[int, int]] = {}

        for index, path in enumerate(files, start=1):
            emit_file_progress(emit_progress, path=path, index=index, total=len(files), stage="start")
            emit_trace_snapshot(
                emit_trace,
                path=path,
                stage="start",
                extra_lines=[f"plik: {path.name}"],
            )
            read_started_at = time.perf_counter()
            meta = read_book_metadata(path)
            read_ms += int((time.perf_counter() - read_started_at) * 1000)
            emit_file_progress(emit_progress, path=path, index=index, total=len(files), stage="metadane")
            emit_trace_snapshot(emit_trace, path=path, stage="metadane", meta=meta)

            infer_started_at = time.perf_counter()
            record = call_infer_record(
                infer_record,
                meta,
                use_online=use_online,
                providers=providers,
                timeout=timeout,
                online_mode=online_mode,
                emit_stage=make_stage_emitter(emit_progress, path=path, index=index, total=len(files)),
                emit_trace=emit_trace,
            )
            infer_ms += int((time.perf_counter() - infer_started_at) * 1000)
            source_signatures[record.path.resolve()] = file_signature(record.path)

            record.output_folder = target_folder
            record = dedupe_destinations_fn([record], target_folder)[0]
            assign_archive_source_paths([record], archive_folder)
            records.append(record)
            embedded_metadata_status[record.path.resolve()] = planned_embedded_metadata_status(
                record,
                target_folder,
                enabled=write_epub_metadata,
            )
            if emit_progress is not None:
                emit_progress("\n".join(build_progress_lines(record, index=len(records), total=len(files), target_folder=target_folder)))

            if operation == "rename" and record.path.name == record.filename:
                execution_status[record.path.resolve()] = "unchanged"
            else:
                moves = build_moves([record], folder, target_folder, archive_folder, stamp)
                if not moves:
                    execution_status[record.path.resolve()] = "unchanged"
                else:
                    to_write += len(moves)
                    execute_started_at = time.perf_counter()
                    move_errors = execute_moves(moves)
                    execute_ms += int((time.perf_counter() - execute_started_at) * 1000)
                    if move_errors:
                        execution_status[record.path.resolve()] = "failed"
                        errors.extend(move_errors)
                    else:
                        if operation == "rename":
                            execution_status[record.path.resolve()] = "renamed"
                        elif operation == "copy+archive":
                            execution_status[record.path.resolve()] = "copied+archived"
                        else:
                            execution_status[record.path.resolve()] = "copied"
                        written_total += len(moves)

            metadata_status, metadata_error = write_embedded_metadata_for_record(
                record,
                target_folder=target_folder,
                enabled=write_epub_metadata,
                execution_status=execution_status,
                write_book_metadata=write_book_metadata,
            )
            embedded_metadata_status[record.path.resolve()] = metadata_status
            if metadata_status == "written":
                metadata_written_total += 1
            if metadata_error:
                metadata_errors.append(metadata_error)
            elif execution_status.get(record.path.resolve(), "") in SUCCESSFUL_PROCESSING_STATUSES:
                update_processed_manifest_entry(
                    processed_manifest,
                    record.path,
                    status=execution_status[record.path.resolve()],
                    target_name=record.filename,
                    signature=source_signatures.get(record.path.resolve()),
                )

        if apply_changes:
            for record in records:
                status = execution_status.get(record.path.resolve(), "")
                metadata_status = embedded_metadata_status.get(record.path.resolve(), "")
                if status == "unchanged" and metadata_status != "failed":
                    update_processed_manifest_entry(
                        processed_manifest,
                        record.path,
                        status=status,
                        target_name=record.filename,
                        signature=source_signatures.get(record.path.resolve()),
                    )
            save_processed_manifest(folder, processed_manifest)

        write_report_fn(
            report_path,
            records,
            dry_run=False,
            source_folder=folder,
            target_folder=target_folder,
            operation=operation,
            execution_status=execution_status,
            embedded_metadata_status=embedded_metadata_status,
        )
        review_total = sum(1 for record in records if record.needs_review)
        lines.append(
            f"TOTAL={len(records)} | SKIPPED={len(skipped_files)} | TO_WRITE={to_write} | WRITTEN={written_total} | REVIEW={review_total} | ERRORS={len(errors)}"
        )
        lines.extend(
            [
                f"OPERATION={operation.upper()}",
                f"SOURCE={folder}",
                f"DESTINATION={target_folder}",
                f"ARCHIVE={archive_folder or ''}",
                f"MANIFEST={processed_manifest_path(folder)}",
                f"SKIPPED={len(skipped_files)}",
                f"INFER_WORKERS={infer_workers}",
                f"ONLINE_HTTP_SLOTS={online_http_slots}",
                f"TO_WRITE={to_write}",
                f"WRITTEN={written_total}",
                f"EMBEDDED_METADATA={'ON' if write_epub_metadata else 'OFF'}",
                f"EMBEDDED_METADATA_WRITTEN={metadata_written_total}",
                f"METADATA_ERRORS={len(metadata_errors)}",
                f"REVIEW={review_total}",
                f"ERRORS={len(errors)}",
                f"REPORT={report_path}",
                f"PROFILE_READ_MS={read_ms}",
                f"PROFILE_INFER_MS={infer_ms}",
                f"PROFILE_EXECUTE_MS={execute_ms}",
                f"PROFILE_TOTAL_MS={int((time.perf_counter() - started_at) * 1000)}",
            ]
        )
        flush_online_cache_if_needed(force=True)
        if errors:
            lines.append("---ERRORS---")
            lines.extend(errors[:20])
            return 1, lines
        if metadata_errors:
            lines.append("---METADATA-WARNINGS---")
            lines.extend(metadata_errors[:20])
        return 0, lines

    if emit_progress is not None:
        infer_workers = 1
        metas: list[Any] = []
        records = []
        read_started_at = time.perf_counter()
        for index, path in enumerate(files, start=1):
            emit_file_progress(emit_progress, path=path, index=index, total=len(files), stage="start")
            emit_trace_snapshot(
                emit_trace,
                path=path,
                stage="start",
                extra_lines=[f"plik: {path.name}"],
            )
            metas.append(read_book_metadata(path))
            emit_file_progress(emit_progress, path=path, index=index, total=len(files), stage="metadane")
            emit_trace_snapshot(emit_trace, path=path, stage="metadane", meta=metas[-1])
        read_ms = int((time.perf_counter() - read_started_at) * 1000)

        infer_started_at = time.perf_counter()
        for index, meta in enumerate(metas, start=1):
            record = call_infer_record(
                infer_record,
                meta,
                use_online=use_online,
                providers=providers,
                timeout=timeout,
                online_mode=online_mode,
                emit_stage=make_stage_emitter(emit_progress, path=meta.path, index=index, total=len(metas)),
                emit_trace=emit_trace,
            )
            record.output_folder = target_folder
            record = dedupe_destinations_fn([record], target_folder)[0]
            assign_archive_source_paths([record], archive_folder)
            records.append(record)
            emit_progress(
                "\n".join(
                    build_progress_lines(
                        record,
                        index=index,
                        total=len(metas),
                        target_folder=target_folder,
                    )
                )
            )
        infer_ms = int((time.perf_counter() - infer_started_at) * 1000)
    else:
        read_started_at = time.perf_counter()
        with ThreadPoolExecutor(max_workers=max(1, min(8, len(files)))) as executor:
            metas = list(executor.map(read_book_metadata, files))
        read_ms = int((time.perf_counter() - read_started_at) * 1000)

        infer_started_at = time.perf_counter()
        infer_cap = max(1, online_workers) if use_online else 8
        infer_workers = max(1, min(8, infer_cap, len(metas)))
        records_by_index: list[RecordLike | None] = [None] * len(metas)
        with ThreadPoolExecutor(max_workers=infer_workers) as executor:
            future_to_index = {
                executor.submit(infer_record, meta, use_online=use_online, providers=providers, timeout=timeout): index
                for index, meta in enumerate(metas)
            }
            completed_records = 0
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                record = future.result()
                record.output_folder = target_folder
                record = dedupe_destinations_fn([record], target_folder)[0]
                assign_archive_source_paths([record], archive_folder)
                records_by_index[index] = record
                completed_records += 1
                if emit_progress is not None:
                    emit_progress(
                        "\n".join(
                            build_progress_lines(
                                record,
                                index=completed_records,
                                total=len(metas),
                                target_folder=target_folder,
                            )
                        )
                    )
        records = [record for record in records_by_index if record is not None]
        infer_ms = int((time.perf_counter() - infer_started_at) * 1000)

    records = set_output_folder_fn(records, target_folder)
    records = dedupe_destinations_fn(records, target_folder)
    records = assign_archive_source_paths(records, archive_folder)
    execution_status: dict[Path, str] = {}
    embedded_metadata_status: dict[Path, str] = {}
    for record in records:
        if operation == "rename" and record.path.name == record.filename:
            execution_status[record.path.resolve()] = "unchanged"
        else:
            execution_status[record.path.resolve()] = "planned" if dry_run else "pending"
        embedded_metadata_status[record.path.resolve()] = planned_embedded_metadata_status(
            record,
            target_folder,
            enabled=write_epub_metadata,
        )

    if dry_run:
        write_report_fn(
            report_path,
            records,
            dry_run=True,
            source_folder=folder,
            target_folder=target_folder,
            operation=operation,
            execution_status=execution_status,
            embedded_metadata_status=embedded_metadata_status,
        )
        to_write = len(build_moves(records, folder, target_folder, archive_folder, stamp))
        review_count = sum(1 for record in records if record.needs_review)
        lines.append(f"TOTAL={len(records)} | SKIPPED={len(skipped_files)} | REVIEW={review_count} | TO_WRITE={to_write}")
        lines.extend(
            [
                "MODE=DRY-RUN",
                f"OPERATION={operation.upper()}",
                f"SOURCE={folder}",
                f"DESTINATION={target_folder}",
                f"ARCHIVE={archive_folder or ''}",
                f"MANIFEST={processed_manifest_path(folder)}",
                f"SKIPPED={len(skipped_files)}",
                f"INFER_WORKERS={infer_workers}",
                f"ONLINE_HTTP_SLOTS={online_http_slots}",
                f"EMBEDDED_METADATA={'ON' if write_epub_metadata else 'OFF'}",
                f"REVIEW={review_count}",
                f"REPORT={report_path}",
                f"PROFILE_READ_MS={read_ms}",
                f"PROFILE_INFER_MS={infer_ms}",
                f"PROFILE_TOTAL_MS={int((time.perf_counter() - started_at) * 1000)}",
            ]
        )
        for record in records[:10]:
            flag = " [CHECK]" if record.needs_review else ""
            lines.append(f"{record.path.name} -> {target_path_for_record(record, target_folder)} (confidence={record.confidence}){flag}")
        flush_online_cache_if_needed(force=True)
        return 0, lines

    moves = build_moves(records, folder, target_folder, archive_folder, stamp)
    execute_started_at = time.perf_counter()
    errors = execute_moves(moves)
    execute_ms = int((time.perf_counter() - execute_started_at) * 1000)
    metadata_written_total = 0
    metadata_errors: list[str] = []
    move_execution_failed = bool(errors)
    if errors:
        for move in moves:
            execution_status[move.source.resolve()] = "failed"
    else:
        for move in moves:
            if move.operation == "rename":
                execution_status[move.source.resolve()] = "renamed"
            elif move.operation == "move":
                if operation == "copy+archive":
                    execution_status[move.source.resolve()] = "copied+archived"
                else:
                    execution_status[move.source.resolve()] = "moved"
            elif move.operation == "copy":
                execution_status[move.source.resolve()] = "copied"
    for record in records:
        metadata_status, metadata_error = write_embedded_metadata_for_record(
            record,
            target_folder=target_folder,
            enabled=write_epub_metadata,
            execution_status=execution_status,
            write_book_metadata=write_book_metadata,
        )
        embedded_metadata_status[record.path.resolve()] = metadata_status
        if metadata_status == "written":
            metadata_written_total += 1
        if metadata_error:
            metadata_errors.append(metadata_error)
    write_report_fn(
        report_path,
        records,
        dry_run=False,
        source_folder=folder,
        target_folder=target_folder,
        operation=operation,
        execution_status=execution_status,
        embedded_metadata_status=embedded_metadata_status,
    )
    review_total = sum(1 for record in records if record.needs_review)
    written_total = len(moves) if not move_execution_failed else 0
    if not move_execution_failed:
        for move in moves:
            status = execution_status.get(move.source.resolve(), "")
            if embedded_metadata_status.get(move.source.resolve(), "") != "failed":
                update_processed_manifest_entry(
                    processed_manifest,
                    move.source,
                    status=status,
                    target_name=move.record.filename,
                    signature=file_signature(move.source),
                )
        for record in records:
            status = execution_status.get(record.path.resolve(), "")
            metadata_status = embedded_metadata_status.get(record.path.resolve(), "")
            if status == "unchanged" and metadata_status != "failed":
                update_processed_manifest_entry(processed_manifest, record.path, status=status, target_name=record.filename)
        save_processed_manifest(folder, processed_manifest)

    lines.append(
        f"TOTAL={len(records)} | SKIPPED={len(skipped_files)} | TO_WRITE={len(moves)} | WRITTEN={written_total} | REVIEW={review_total} | ERRORS={len(errors)}"
    )
    lines.extend(
        [
            f"OPERATION={operation.upper()}",
            f"SOURCE={folder}",
            f"DESTINATION={target_folder}",
            f"ARCHIVE={archive_folder or ''}",
            f"MANIFEST={processed_manifest_path(folder)}",
            f"SKIPPED={len(skipped_files)}",
            f"INFER_WORKERS={infer_workers}",
            f"ONLINE_HTTP_SLOTS={online_http_slots}",
            f"TO_WRITE={len(moves)}",
            f"WRITTEN={written_total}",
            f"EMBEDDED_METADATA={'ON' if write_epub_metadata else 'OFF'}",
            f"EMBEDDED_METADATA_WRITTEN={metadata_written_total}",
            f"METADATA_ERRORS={len(metadata_errors)}",
            f"REVIEW={review_total}",
            f"ERRORS={len(errors)}",
            f"REPORT={report_path}",
            f"PROFILE_READ_MS={read_ms}",
            f"PROFILE_INFER_MS={infer_ms}",
            f"PROFILE_EXECUTE_MS={execute_ms}",
            f"PROFILE_TOTAL_MS={int((time.perf_counter() - started_at) * 1000)}",
        ]
    )
    flush_online_cache_if_needed(force=True)
    if errors:
        lines.append("---ERRORS---")
        lines.extend(errors[:20])
        return 1, lines
    if metadata_errors:
        lines.append("---METADATA-WARNINGS---")
        lines.extend(metadata_errors[:20])
    return 0, lines
