from __future__ import annotations

import json
import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

import app_runtime as runtime


GUI_STATE_STRING_KEYS = (
    "source_folder",
    "destination_folder",
    "archive_folder",
    "online_mode",
    "ai_mode",
    "metadata_folder",
    "metadata_tags",
    "conversion_source_folder",
    "conversion_destination_folder",
    "conversion_tags",
    "calibre_folder",
)
GUI_STATE_BOOL_KEYS = (
    "write_epub_metadata",
    "metadata_recursive",
    "metadata_apply_changes",
    "conversion_recursive",
    "conversion_write_metadata",
    "conversion_trash_sources",
)


def gui_state_path() -> Path:
    base_dir = Path.home() / "AppData" / "Local" if Path.home().drive else Path.home()
    return base_dir / runtime.APP_NAME / "gui_state.json"


def load_gui_state() -> dict[str, str]:
    path = gui_state_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}

    state: dict[str, str] = {}
    for key in GUI_STATE_STRING_KEYS:
        value = payload.get(key)
        if isinstance(value, str):
            state[key] = value
    for key in GUI_STATE_BOOL_KEYS:
        value = payload.get(key)
        if isinstance(value, bool):
            state[key] = "true" if value else "false"
    return state


def save_gui_state(**state_values: object) -> None:
    path = gui_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    payload: dict[str, object] = {}
    for key in GUI_STATE_STRING_KEYS:
        value = state_values.get(key, "")
        payload[key] = value.strip() if isinstance(value, str) else ""
    for key in GUI_STATE_BOOL_KEYS:
        if key in state_values:
            payload[key] = bool(state_values[key])

    online_mode = str(payload.get("online_mode", "")).strip().upper()
    payload["online_mode"] = online_mode or runtime.DEFAULT_ONLINE_MODE
    ai_mode = str(payload.get("ai_mode", "")).strip().upper()
    payload["ai_mode"] = ai_mode or runtime.DEFAULT_AI_MODE
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _state_bool(state: dict[str, str], key: str, default: bool) -> bool:
    value = (state.get(key) or ("true" if default else "false")).strip().lower()
    return value == "true"


def launch_gui(
    default_folder: str,
    default_destination: str,
    default_archive_folder: str,
    default_online_mode: str,
    default_ai_mode: str,
    default_providers: str,
    default_timeout: float,
    default_limit: int,
    default_online: bool,
    default_online_workers: int,
    default_skip_processed: bool,
    default_write_epub_metadata: bool,
) -> int:
    saved_state = load_gui_state()
    detected_calibre_folder = runtime.embedded_metadata_mod.detect_calibre_folder()

    root = tk.Tk()
    root.title(runtime.APP_NAME)
    root.geometry("1220x720")

    worker_state = {"running": False}

    folder_var = tk.StringVar(value=saved_state.get("source_folder") or default_folder)
    destination_var = tk.StringVar(value=saved_state.get("destination_folder") or default_destination)
    archive_var = tk.StringVar(value=saved_state.get("archive_folder") or default_archive_folder)
    online_mode_var = tk.StringVar(value=(saved_state.get("online_mode") or default_online_mode).upper())
    ai_mode_var = tk.StringVar(value=(saved_state.get("ai_mode") or default_ai_mode).upper())
    timeout_var = tk.StringVar(value=str(default_timeout))
    limit_var = tk.StringVar(value=str(default_limit))
    online_workers_var = tk.StringVar(value=str(default_online_workers))
    online_var = tk.BooleanVar(value=default_online)
    apply_var = tk.BooleanVar(value=False)
    skip_processed_var = tk.BooleanVar(value=default_skip_processed)
    write_epub_metadata_var = tk.BooleanVar(
        value=_state_bool(saved_state, "write_epub_metadata", default_write_epub_metadata)
    )

    metadata_folder_var = tk.StringVar(
        value=saved_state.get("metadata_folder") or saved_state.get("source_folder") or default_folder
    )
    metadata_tags_var = tk.StringVar(value=saved_state.get("metadata_tags") or "Killim")
    metadata_recursive_var = tk.BooleanVar(value=_state_bool(saved_state, "metadata_recursive", True))
    metadata_apply_changes_var = tk.BooleanVar(value=_state_bool(saved_state, "metadata_apply_changes", True))

    conversion_source_var = tk.StringVar(
        value=saved_state.get("conversion_source_folder") or saved_state.get("source_folder") or default_folder
    )
    conversion_destination_var = tk.StringVar(
        value=saved_state.get("conversion_destination_folder") or saved_state.get("destination_folder") or default_destination
    )
    conversion_tags_var = tk.StringVar(value=saved_state.get("conversion_tags") or "Killim")
    conversion_recursive_var = tk.BooleanVar(value=_state_bool(saved_state, "conversion_recursive", False))
    conversion_write_metadata_var = tk.BooleanVar(value=_state_bool(saved_state, "conversion_write_metadata", True))
    conversion_trash_sources_var = tk.BooleanVar(value=_state_bool(saved_state, "conversion_trash_sources", False))

    calibre_folder_var = tk.StringVar(
        value=saved_state.get("calibre_folder") or (str(detected_calibre_folder) if detected_calibre_folder else "")
    )

    rename_status_var = tk.StringVar(value="Gotowe.")
    metadata_status_var = tk.StringVar(value="Gotowe.")
    conversion_status_var = tk.StringVar(value="Gotowe.")

    def persist_gui_state() -> None:
        save_gui_state(
            source_folder=folder_var.get(),
            destination_folder=destination_var.get(),
            archive_folder=archive_var.get(),
            online_mode=online_mode_var.get(),
            ai_mode=ai_mode_var.get(),
            write_epub_metadata=write_epub_metadata_var.get(),
            metadata_folder=metadata_folder_var.get(),
            metadata_tags=metadata_tags_var.get(),
            metadata_recursive=metadata_recursive_var.get(),
            metadata_apply_changes=metadata_apply_changes_var.get(),
            conversion_source_folder=conversion_source_var.get(),
            conversion_destination_folder=conversion_destination_var.get(),
            conversion_tags=conversion_tags_var.get(),
            conversion_recursive=conversion_recursive_var.get(),
            conversion_write_metadata=conversion_write_metadata_var.get(),
            conversion_trash_sources=conversion_trash_sources_var.get(),
            calibre_folder=calibre_folder_var.get(),
        )

    def choose_directory(target_var: tk.StringVar, fallback: str = "") -> None:
        selected = filedialog.askdirectory(initialdir=target_var.get() or fallback or default_folder)
        if selected:
            target_var.set(selected)
            persist_gui_state()

    def start_background_task(
        *,
        run_button: ttk.Button,
        output_widget: tk.Text,
        status_var: tk.StringVar,
        worker_fn,
        trace_widget: tk.Text | None = None,
        success_title: str = "Zakonczono",
        warning_title: str = "Problem",
    ) -> None:
        if worker_state["running"]:
            messagebox.showwarning("Zajete", "Poczekaj na zakonczenie aktualnej operacji.")
            return

        result_queue: queue.Queue[tuple[int, list[str]] | tuple[str, str]] = queue.Queue()
        trace_state = {"current_file": ""}

        worker_state["running"] = True
        run_button.config(state=tk.DISABLED)
        status_var.set("Start...")
        output_widget.delete("1.0", tk.END)
        if trace_widget is not None:
            trace_widget.delete("1.0", tk.END)

        def poll_result_queue() -> None:
            finished: tuple[int, list[str]] | None = None
            while True:
                try:
                    item = result_queue.get_nowait()
                except queue.Empty:
                    break

                if item and item[0] == "progress":
                    _, message = item
                    if output_widget.index("end-1c") != "1.0":
                        output_widget.insert(tk.END, "\n\n")
                    output_widget.insert(tk.END, message)
                    output_widget.see(tk.END)
                    status_var.set(message.splitlines()[0] if message else "Przetwarzanie...")
                    continue

                if item and item[0] == "trace" and trace_widget is not None:
                    _, message = item
                    trace_file = message.splitlines()[0].strip() if message else ""
                    if trace_file != trace_state["current_file"]:
                        trace_state["current_file"] = trace_file
                        trace_widget.delete("1.0", tk.END)
                    elif trace_widget.index("end-1c") != "1.0":
                        trace_widget.insert(tk.END, "\n\n")
                    trace_widget.insert(tk.END, message)
                    trace_widget.see(tk.END)
                    continue

                if item and item[0] == "error":
                    worker_state["running"] = False
                    run_button.config(state=tk.NORMAL)
                    _, message = item
                    status_var.set("Blad.")
                    messagebox.showerror("Blad", message)
                    return

                finished = item  # type: ignore[assignment]

            if finished is not None:
                worker_state["running"] = False
                run_button.config(state=tk.NORMAL)
                code, lines = finished
                if output_widget.index("end-1c") != "1.0":
                    output_widget.insert(tk.END, "\n\n--- PODSUMOWANIE ---\n")
                output_widget.insert(tk.END, "\n".join(lines))
                output_widget.see(tk.END)
                status_var.set(lines[-1] if lines else "Gotowe.")
                if code == 0:
                    messagebox.showinfo(success_title, lines[-1] if lines else "Gotowe.")
                else:
                    messagebox.showwarning(warning_title, lines[-1] if lines else "Wystapil problem.")
                return

            if worker_state["running"]:
                root.after(150, poll_result_queue)

        def worker() -> None:
            try:
                result_queue.put(
                    worker_fn(
                        lambda message: result_queue.put(("progress", message)),
                        lambda message: result_queue.put(("trace", message)),
                    )
                )
            except Exception as exc:
                result_queue.put(("error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()
        root.after(150, poll_result_queue)

    def run_renamer_from_gui() -> None:
        folder = Path(folder_var.get().strip())
        destination_text = destination_var.get().strip()
        archive_text = archive_var.get().strip()
        destination = Path(destination_text) if destination_text else None
        archive_folder = Path(archive_text) if archive_text else None
        providers = [item.strip().lower() for item in default_providers.split(",") if item.strip()]
        try:
            timeout = float(timeout_var.get().strip())
        except ValueError:
            messagebox.showerror("Blad", "Timeout musi byc liczba.")
            return
        try:
            limit = int(limit_var.get().strip() or "0")
        except ValueError:
            messagebox.showerror("Blad", "Limit musi byc liczba calkowita.")
            return
        try:
            online_workers = int(online_workers_var.get().strip() or str(runtime.DEFAULT_INFER_WORKERS))
        except ValueError:
            messagebox.showerror("Blad", "Infer workers musi byc liczba calkowita.")
            return
        apply_changes = apply_var.get()
        use_online = online_var.get()
        skip_processed = skip_processed_var.get()
        write_epub_metadata = write_epub_metadata_var.get()
        online_mode = online_mode_var.get().strip().upper() or runtime.DEFAULT_ONLINE_MODE
        ai_mode = ai_mode_var.get().strip().upper() or runtime.DEFAULT_AI_MODE
        persist_gui_state()

        start_background_task(
            run_button=rename_run_button,
            output_widget=rename_output,
            trace_widget=rename_trace_output,
            status_var=rename_status_var,
            success_title="Renamer",
            warning_title="Renamer",
            worker_fn=lambda emit_progress, emit_trace: runtime.run_job(
                folder,
                destination_folder=destination,
                archive_folder=archive_folder,
                online_mode=online_mode,
                ai_mode=ai_mode,
                apply_changes=apply_changes,
                use_online=use_online,
                providers=providers,
                timeout=timeout,
                limit=limit,
                online_workers=online_workers,
                write_epub_metadata=write_epub_metadata,
                emit_progress=emit_progress,
                emit_trace=emit_trace,
                skip_previously_processed=skip_processed,
            ),
        )

    def run_metadata_from_gui() -> None:
        folder_text = metadata_folder_var.get().strip()
        if not folder_text:
            messagebox.showerror("Blad", "Wskaz folder do uzupelnienia metadanych.")
            return
        folder = Path(folder_text)
        calibre_folder_text = calibre_folder_var.get().strip()
        calibre_folder = Path(calibre_folder_text) if calibre_folder_text else None
        persist_gui_state()

        start_background_task(
            run_button=metadata_run_button,
            output_widget=metadata_output,
            status_var=metadata_status_var,
            success_title="Metadane",
            warning_title="Metadane",
            worker_fn=lambda emit_progress, _emit_trace: runtime.run_metadata_backfill(
                folder,
                recursive=metadata_recursive_var.get(),
                tags_text=metadata_tags_var.get(),
                apply_changes=metadata_apply_changes_var.get(),
                calibre_folder=calibre_folder,
                emit_progress=emit_progress,
            ),
        )

    def run_conversion_from_gui() -> None:
        source_text = conversion_source_var.get().strip()
        destination_text = conversion_destination_var.get().strip()
        if not source_text or not destination_text:
            messagebox.showerror("Blad", "Wskaz folder zrodlowy i docelowy dla eksportu EPUB.")
            return

        source_folder = Path(source_text)
        destination_folder = Path(destination_text)
        try:
            if source_folder.resolve() == destination_folder.resolve():
                messagebox.showerror("Blad", "Folder zrodlowy i docelowy musza byc rozne.")
                return
        except Exception:
            pass

        calibre_folder_text = calibre_folder_var.get().strip()
        calibre_folder = Path(calibre_folder_text) if calibre_folder_text else None
        persist_gui_state()

        start_background_task(
            run_button=conversion_run_button,
            output_widget=conversion_output,
            status_var=conversion_status_var,
            success_title="Eksport EPUB",
            warning_title="Eksport EPUB",
            worker_fn=lambda emit_progress, _emit_trace: runtime.run_epub_export(
                source_folder,
                destination_folder,
                recursive=conversion_recursive_var.get(),
                calibre_folder=calibre_folder,
                tags_text=conversion_tags_var.get(),
                write_metadata_after_export=conversion_write_metadata_var.get(),
                trash_sources_after_convert=conversion_trash_sources_var.get(),
                emit_progress=emit_progress,
            ),
        )

    def close_window() -> None:
        persist_gui_state()
        root.destroy()

    notebook = ttk.Notebook(root)
    notebook.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)

    rename_tab = ttk.Frame(notebook, padding=12)
    metadata_tab = ttk.Frame(notebook, padding=12)
    conversion_tab = ttk.Frame(notebook, padding=12)
    notebook.add(rename_tab, text="Renamer")
    notebook.add(metadata_tab, text="Metadane")
    notebook.add(conversion_tab, text="Konwersja EPUB")

    ttk.Label(rename_tab, text="Folder zrodlowy").grid(row=0, column=0, sticky="w")
    ttk.Entry(rename_tab, textvariable=folder_var, width=72).grid(row=1, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(rename_tab, text="Wybierz", command=lambda: choose_directory(folder_var, default_folder)).grid(row=1, column=1, sticky="ew")

    ttk.Label(rename_tab, text="Folder docelowy zmienionej nazwy").grid(row=2, column=0, sticky="w", pady=(10, 0))
    ttk.Entry(rename_tab, textvariable=destination_var, width=72).grid(row=3, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(rename_tab, text="Wybierz", command=lambda: choose_directory(destination_var, folder_var.get() or default_folder)).grid(row=3, column=1, sticky="ew")
    ttk.Label(
        rename_tab,
        text="Tutaj trafia kopia pliku ze zmieniona nazwa. Gdy pole jest puste, zmiana odbywa sie w folderze zrodlowym.",
    ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(4, 0))

    ttk.Label(rename_tab, text="Folder archiwum oryginalow").grid(row=5, column=0, sticky="w", pady=(10, 0))
    ttk.Entry(rename_tab, textvariable=archive_var, width=72).grid(row=6, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(rename_tab, text="Wybierz", command=lambda: choose_directory(archive_var, folder_var.get() or default_folder)).grid(row=6, column=1, sticky="ew")
    ttk.Label(
        rename_tab,
        text="Po udanym utworzeniu kopii ze zmieniona nazwa oryginal zostanie przeniesiony tutaj.",
    ).grid(row=7, column=0, columnspan=2, sticky="w", pady=(4, 0))

    rename_options = ttk.Frame(rename_tab)
    rename_options.grid(row=8, column=0, columnspan=2, sticky="ew", pady=(10, 0))
    ttk.Checkbutton(rename_options, text="Online", variable=online_var).grid(row=0, column=0, sticky="w")
    ttk.Checkbutton(rename_options, text="Apply", variable=apply_var).grid(row=0, column=1, sticky="w", padx=(12, 0))
    ttk.Checkbutton(rename_options, text="Pomijaj przetworzone", variable=skip_processed_var).grid(row=0, column=2, sticky="w", padx=(12, 0))
    ttk.Checkbutton(
        rename_options,
        text="Zapisz metadane w pliku",
        variable=write_epub_metadata_var,
        command=persist_gui_state,
    ).grid(row=0, column=3, sticky="w", padx=(12, 0))
    ttk.Label(rename_options, text="Tryb online").grid(row=0, column=4, sticky="w", padx=(20, 0))
    ttk.Radiobutton(rename_options, text="PL", value="PL", variable=online_mode_var, command=persist_gui_state).grid(row=0, column=5, sticky="w")
    ttk.Radiobutton(rename_options, text="PL+", value="PL+", variable=online_mode_var, command=persist_gui_state).grid(row=0, column=6, sticky="w")
    ttk.Radiobutton(rename_options, text="EN", value="EN", variable=online_mode_var, command=persist_gui_state).grid(row=0, column=7, sticky="w")
    ttk.Label(rename_options, text="Tryb AI").grid(row=0, column=8, sticky="w", padx=(20, 0))
    ai_mode_combo = ttk.Combobox(
        rename_options,
        textvariable=ai_mode_var,
        values=("OFF", "REVIEW", "ASSIST", "AUTO"),
        state="readonly",
        width=10,
    )
    ai_mode_combo.grid(row=0, column=9, sticky="w")
    ai_mode_combo.bind("<<ComboboxSelected>>", lambda _event: persist_gui_state())
    ttk.Label(rename_options, text="Timeout").grid(row=0, column=10, sticky="w", padx=(20, 0))
    ttk.Entry(rename_options, textvariable=timeout_var, width=8).grid(row=0, column=11, sticky="w")
    ttk.Label(rename_options, text="Limit").grid(row=0, column=12, sticky="w", padx=(20, 0))
    ttk.Entry(rename_options, textvariable=limit_var, width=8).grid(row=0, column=13, sticky="w")
    ttk.Label(rename_options, text="Infer workers").grid(row=0, column=14, sticky="w", padx=(20, 0))
    ttk.Entry(rename_options, textvariable=online_workers_var, width=8).grid(row=0, column=15, sticky="w")

    rename_run_button = ttk.Button(rename_tab, text="Uruchom renamer", command=run_renamer_from_gui)
    rename_run_button.grid(row=9, column=0, columnspan=2, sticky="ew", pady=(12, 0))
    ttk.Label(rename_tab, textvariable=rename_status_var).grid(row=10, column=0, columnspan=2, sticky="w", pady=(8, 0))

    rename_panes = ttk.Panedwindow(rename_tab, orient=tk.HORIZONTAL)
    rename_panes.grid(row=11, column=0, columnspan=2, sticky="nsew", pady=(12, 0))
    rename_progress_frame = ttk.Frame(rename_panes)
    rename_trace_frame = ttk.Frame(rename_panes)
    rename_panes.add(rename_progress_frame, weight=1)
    rename_panes.add(rename_trace_frame, weight=1)

    ttk.Label(rename_progress_frame, text="Przebieg").pack(anchor="w")
    rename_output = tk.Text(rename_progress_frame, wrap="word", height=22)
    rename_output.pack(fill=tk.BOTH, expand=True)

    ttk.Label(rename_trace_frame, text="Transformacja aktualnego pliku").pack(anchor="w")
    rename_trace_output = tk.Text(rename_trace_frame, wrap="word", height=22)
    rename_trace_output.pack(fill=tk.BOTH, expand=True)

    rename_tab.columnconfigure(0, weight=1)
    rename_tab.rowconfigure(11, weight=1)

    ttk.Label(metadata_tab, text="Folder z ebookami do uzupelnienia metadanych").grid(row=0, column=0, sticky="w")
    ttk.Entry(metadata_tab, textvariable=metadata_folder_var, width=72).grid(row=1, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(metadata_tab, text="Wybierz", command=lambda: choose_directory(metadata_folder_var, default_folder)).grid(row=1, column=1, sticky="ew")

    ttk.Label(metadata_tab, text="Tagi dopisywane do metadanych").grid(row=2, column=0, sticky="w", pady=(10, 0))
    ttk.Entry(metadata_tab, textvariable=metadata_tags_var, width=72).grid(row=3, column=0, sticky="ew", padx=(0, 8))
    ttk.Label(metadata_tab, text="Mozesz podac kilka tagow rozdzielonych przecinkami. Domyslnie: Killim").grid(
        row=4, column=0, columnspan=2, sticky="w", pady=(4, 0)
    )

    ttk.Label(metadata_tab, text="Folder calibre (opcjonalnie)").grid(row=5, column=0, sticky="w", pady=(10, 0))
    ttk.Entry(metadata_tab, textvariable=calibre_folder_var, width=72).grid(row=6, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(metadata_tab, text="Wybierz", command=lambda: choose_directory(calibre_folder_var, str(detected_calibre_folder or default_folder))).grid(row=6, column=1, sticky="ew")

    metadata_options = ttk.Frame(metadata_tab)
    metadata_options.grid(row=7, column=0, columnspan=2, sticky="ew", pady=(10, 0))
    ttk.Checkbutton(metadata_options, text="Przetwarzaj podfoldery", variable=metadata_recursive_var, command=persist_gui_state).grid(row=0, column=0, sticky="w")
    ttk.Checkbutton(metadata_options, text="Zapisz do plikow", variable=metadata_apply_changes_var, command=persist_gui_state).grid(row=0, column=1, sticky="w", padx=(12, 0))

    metadata_run_button = ttk.Button(metadata_tab, text="Uzupelnij metadane", command=run_metadata_from_gui)
    metadata_run_button.grid(row=8, column=0, columnspan=2, sticky="ew", pady=(12, 0))
    ttk.Label(metadata_tab, textvariable=metadata_status_var).grid(row=9, column=0, columnspan=2, sticky="w", pady=(8, 0))

    ttk.Label(
        metadata_tab,
        text="To narzedzie bierze dane z juz ustalonego wzorca nazwy pliku i zapisuje je do osadzonych metadanych ebooka.",
    ).grid(row=10, column=0, columnspan=2, sticky="w")

    metadata_output = tk.Text(metadata_tab, wrap="word", height=24)
    metadata_output.grid(row=11, column=0, columnspan=2, sticky="nsew", pady=(12, 0))
    metadata_tab.columnconfigure(0, weight=1)
    metadata_tab.rowconfigure(11, weight=1)

    ttk.Label(conversion_tab, text="Folder zrodlowy z plikami do eksportu EPUB").grid(row=0, column=0, sticky="w")
    ttk.Entry(conversion_tab, textvariable=conversion_source_var, width=72).grid(row=1, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(conversion_tab, text="Wybierz", command=lambda: choose_directory(conversion_source_var, default_folder)).grid(row=1, column=1, sticky="ew")

    ttk.Label(conversion_tab, text="Folder docelowy dla EPUB").grid(row=2, column=0, sticky="w", pady=(10, 0))
    ttk.Entry(conversion_tab, textvariable=conversion_destination_var, width=72).grid(row=3, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(conversion_tab, text="Wybierz", command=lambda: choose_directory(conversion_destination_var, default_folder)).grid(row=3, column=1, sticky="ew")

    ttk.Label(conversion_tab, text="Folder calibre").grid(row=4, column=0, sticky="w", pady=(10, 0))
    ttk.Entry(conversion_tab, textvariable=calibre_folder_var, width=72).grid(row=5, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(conversion_tab, text="Wybierz", command=lambda: choose_directory(calibre_folder_var, str(detected_calibre_folder or default_folder))).grid(row=5, column=1, sticky="ew")

    ttk.Label(conversion_tab, text="Tagi do dopisania po eksporcie EPUB").grid(row=6, column=0, sticky="w", pady=(10, 0))
    ttk.Entry(conversion_tab, textvariable=conversion_tags_var, width=72).grid(row=7, column=0, sticky="ew", padx=(0, 8))

    conversion_options = ttk.Frame(conversion_tab)
    conversion_options.grid(row=8, column=0, columnspan=2, sticky="ew", pady=(10, 0))
    ttk.Checkbutton(conversion_options, text="Przetwarzaj podfoldery", variable=conversion_recursive_var, command=persist_gui_state).grid(row=0, column=0, sticky="w")
    ttk.Checkbutton(conversion_options, text="Po eksporcie dopisz metadane", variable=conversion_write_metadata_var, command=persist_gui_state).grid(row=0, column=1, sticky="w", padx=(12, 0))
    ttk.Checkbutton(conversion_options, text="Po udanej konwersji wrzuc zrodla do kosza", variable=conversion_trash_sources_var, command=persist_gui_state).grid(row=0, column=2, sticky="w", padx=(12, 0))

    ttk.Label(
        conversion_tab,
        text="Jesli w folderze zrodlowym jest juz EPUB o tej samej nazwie bazowej, EPUB trafia do celu, a pozostale formaty z tej grupy leca do kosza.",
    ).grid(row=9, column=0, columnspan=2, sticky="w", pady=(4, 0))

    conversion_run_button = ttk.Button(conversion_tab, text="Eksportuj do EPUB", command=run_conversion_from_gui)
    conversion_run_button.grid(row=10, column=0, columnspan=2, sticky="ew", pady=(12, 0))
    ttk.Label(conversion_tab, textvariable=conversion_status_var).grid(row=11, column=0, columnspan=2, sticky="w", pady=(8, 0))

    conversion_output = tk.Text(conversion_tab, wrap="word", height=22)
    conversion_output.grid(row=12, column=0, columnspan=2, sticky="nsew", pady=(12, 0))
    conversion_tab.columnconfigure(0, weight=1)
    conversion_tab.rowconfigure(12, weight=1)

    ttk.Label(root, text=runtime.GUI_FOOTER_TEXT, font=("Segoe UI", 8)).pack(anchor="e", padx=18, pady=(0, 8))

    root.protocol("WM_DELETE_WINDOW", close_window)
    root.mainloop()
    return 0
