from __future__ import annotations

import csv
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from models_core import RenameMove, UndoPlan


def build_moves(records: list[Any], source_folder: Path, target_folder: Path, stamp: str) -> list[RenameMove]:
    moves: list[RenameMove] = []
    same_folder = source_folder.resolve() == target_folder.resolve()
    for index, record in enumerate(records, start=1):
        destination = target_folder / record.filename
        if record.path.parent.resolve() == destination.parent.resolve() and record.path.name == destination.name:
            continue
        if same_folder:
            temp = source_folder / f"__tmp_rename_{stamp}_{index:04d}{record.path.suffix.lower()}"
            moves.append(RenameMove(record.path, temp, destination, record, "rename"))
        else:
            moves.append(RenameMove(record.path, None, destination, record, "copy"))
    return moves


def validate_move_collisions(moves: list[RenameMove]) -> list[str]:
    errors: list[str] = []
    source_paths = {move.source.resolve() for move in moves}
    seen_destinations: set[str] = set()

    for move in moves:
        if move.operation != "delete":
            destination_key = str(move.destination.resolve()).lower()
            if destination_key in seen_destinations:
                errors.append(f"duplicate-destination:{move.destination.name}")
                continue
            seen_destinations.add(destination_key)

            if move.destination.exists() and (move.operation != "rename" or move.destination.resolve() not in source_paths):
                errors.append(f"destination-exists:{move.destination.name}")

        if move.temp is not None and move.temp.exists() and move.temp.resolve() not in source_paths:
            errors.append(f"temp-exists:{move.temp.name}")

    return errors


def rollback_moves(moves: list[RenameMove], stage2_done: list[RenameMove]) -> None:
    for move in reversed(stage2_done):
        if move.destination.exists():
            os.replace(move.destination, move.source)
    moved_to_stage2 = {move.destination for move in stage2_done}
    for move in moves:
        if move.destination in moved_to_stage2:
            continue
        if move.temp is not None and move.temp.exists():
            os.replace(move.temp, move.source)


def execute_moves(moves: list[RenameMove]) -> list[str]:
    if not moves:
        return []
    operations = {move.operation for move in moves}
    if len(operations) > 1:
        errors: list[str] = []
        rename_like = [move for move in moves if move.operation == "rename"]
        copy_like = [move for move in moves if move.operation == "copy"]
        delete_like = [move for move in moves if move.operation == "delete"]
        for chunk in (rename_like, copy_like, delete_like):
            if not chunk:
                continue
            errors.extend(execute_moves(chunk))
            if errors:
                return errors
        return []

    operation = next(iter(operations))
    if operation == "copy":
        validation_errors = validate_move_collisions(moves)
        if validation_errors:
            return validation_errors
        created: list[Path] = []
        try:
            for move in moves:
                move.destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(move.source, move.destination)
                created.append(move.destination)
        except Exception as exc:
            for path in reversed(created):
                if path.exists():
                    path.unlink()
            return [f"copy:{move.source.name}: {exc}"]
        return []

    if operation == "delete":
        validation_errors = validate_move_collisions(moves)
        if validation_errors:
            return validation_errors
        backed_up: list[RenameMove] = []
        deleted: list[RenameMove] = []
        try:
            for move in moves:
                if move.temp is None:
                    return [f"delete-temp-missing:{move.source.name}"]
                move.temp.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(move.source, move.temp)
                backed_up.append(move)
        except Exception as exc:
            for item in reversed(backed_up):
                if item.temp is not None and item.temp.exists():
                    item.temp.unlink()
            return [f"delete-backup:{move.source.name}: {exc}"]
        try:
            for move in moves:
                if move.source.exists():
                    move.source.unlink()
                    deleted.append(move)
        except Exception as exc:
            for item in reversed(deleted):
                if item.temp is not None and item.temp.exists():
                    shutil.copy2(item.temp, item.source)
            for item in reversed(backed_up):
                if item.temp is not None and item.temp.exists():
                    item.temp.unlink()
            return [f"delete:{move.source.name}: {exc}"]
        cleanup_errors: list[str] = []
        for move in reversed(backed_up):
            if move.temp is None or not move.temp.exists():
                continue
            try:
                move.temp.unlink()
            except Exception as exc:
                cleanup_errors.append(f"delete-cleanup:{move.temp.name}: {exc}")
        if cleanup_errors:
            return cleanup_errors
        return []

    errors: list[str] = []
    stage1_done: list[RenameMove] = []
    stage2_done: list[RenameMove] = []

    validation_errors = validate_move_collisions(moves)
    if validation_errors:
        return validation_errors

    try:
        for move in moves:
            os.replace(move.source, move.temp)
            stage1_done.append(move)
    except Exception as exc:
        errors.append(f"stage1:{move.source.name}: {exc}")
        for item in reversed(stage1_done):
            if item.temp.exists():
                os.replace(item.temp, item.source)
        return errors

    try:
        for move in moves:
            os.replace(move.temp, move.destination)
            stage2_done.append(move)
    except Exception as exc:
        errors.append(f"stage2:{move.source.name}: {exc}")
        rollback_moves(moves, stage2_done)
        return errors

    return errors


def build_undo_plan(report_path: Path, folder_hint: Path | None = None) -> UndoPlan:
    folder = folder_hint if folder_hint and folder_hint.exists() else report_path.parent
    moves: list[RenameMove] = []
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    total_rows = 0

    with report_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        required = {"source_name", "target_name", "mode", "execution_status"}
        if not required.issubset(set(reader.fieldnames or [])):
            missing = ", ".join(sorted(required - set(reader.fieldnames or [])))
            raise ValueError(f"Brak kolumn w logu: {missing}")

        for index, row in enumerate(reader, start=1):
            total_rows += 1
            if (row.get("mode") or "").strip().lower() != "apply":
                continue
            execution_status = (row.get("execution_status") or "").strip().lower()
            source_name = row.get("source_name") or ""
            target_name = row.get("target_name") or ""
            if not source_name or not target_name or source_name == target_name:
                continue
            source_folder = Path(row.get("source_folder") or folder)
            target_folder = Path(row.get("target_folder") or folder)
            operation = (row.get("operation") or "rename").strip().lower()
            expected_status = "copied" if operation == "copy" else "renamed"
            if execution_status != expected_status:
                continue
            current = target_folder / target_name
            destination = source_folder / source_name
            suffix = destination.suffix.lower() or current.suffix.lower() or ".tmp"
            if operation == "copy":
                temp = target_folder / f"__tmp_undo_delete_{stamp}_{index:04d}{suffix}"
                moves.append(RenameMove(current, temp, destination, None, "delete"))
            else:
                temp = target_folder / f"__tmp_undo_{stamp}_{index:04d}{suffix}"
                moves.append(RenameMove(current, temp, destination, None, "rename"))

    return UndoPlan(folder=folder, moves=moves, total_rows=total_rows)


def execute_undo(
    report_path: Path,
    folder_hint: Path | None = None,
    *,
    log_error: Callable[[str], None],
    emit_lines: Callable[[list[str], int], None],
) -> int:
    try:
        plan = build_undo_plan(report_path, folder_hint)
    except Exception as exc:
        log_error(f"Nie mozna odczytac logu undo: {exc}")
        return 2

    if not plan.moves:
        emit_lines(
            [
                f"UNDO_REPORT={report_path}",
                f"UNDO_FOLDER={plan.folder}",
                "UNDO_MOVES=0",
                "UNDO_ERRORS=0",
            ],
            logging.INFO,
        )
        return 0

    missing = [move.source.name for move in plan.moves if not move.source.exists()]
    if missing:
        emit_lines(
            [
                f"UNDO_REPORT={report_path}",
                f"UNDO_FOLDER={plan.folder}",
                f"UNDO_MOVES={len(plan.moves)}",
                f"UNDO_ERRORS={len(missing)}",
                "---ERRORS---",
            ],
            logging.INFO,
        )
        emit_lines([f"missing-current-file:{name}" for name in missing[:20]], logging.ERROR)
        return 1

    errors = execute_moves(plan.moves)
    completed = len(plan.moves) if not errors else 0
    deleted = sum(1 for move in plan.moves if move.operation == "delete") if not errors else 0
    renamed = sum(1 for move in plan.moves if move.operation == "rename") if not errors else 0
    emit_lines(
        [
            f"UNDO_REPORT={report_path}",
            f"UNDO_FOLDER={plan.folder}",
            f"UNDO_TOTAL_ROWS={plan.total_rows}",
            f"UNDO_MOVES={len(plan.moves)}",
            f"UNDO_DONE={completed}",
            f"UNDO_RENAMED={renamed}",
            f"UNDO_DELETED={deleted}",
            f"UNDO_ERRORS={len(errors)}",
        ],
        logging.INFO,
    )
    if errors:
        emit_lines(["---ERRORS---"], logging.INFO)
        emit_lines(errors[:20], logging.ERROR)
        return 1
    return 0
