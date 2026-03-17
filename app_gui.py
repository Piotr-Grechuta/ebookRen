from __future__ import annotations

import json
import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

import app_runtime as runtime


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
    for key in ("source_folder", "destination_folder", "archive_folder", "online_mode"):
        value = payload.get(key)
        if isinstance(value, str):
            state[key] = value
    if isinstance(payload.get("write_epub_metadata"), bool):
        state["write_epub_metadata"] = "true" if payload["write_epub_metadata"] else "false"
    return state


def save_gui_state(*, source_folder: str, destination_folder: str, archive_folder: str, online_mode: str, write_epub_metadata: bool) -> None:
    path = gui_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source_folder": source_folder.strip(),
        "destination_folder": destination_folder.strip(),
        "archive_folder": archive_folder.strip(),
        "online_mode": online_mode.strip().upper() or runtime.DEFAULT_ONLINE_MODE,
        "write_epub_metadata": bool(write_epub_metadata),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def launch_gui(
    default_folder: str,
    default_destination: str,
    default_archive_folder: str,
    default_online_mode: str,
    default_providers: str,
    default_timeout: float,
    default_limit: int,
    default_online: bool,
    default_online_workers: int,
    default_skip_processed: bool,
    default_write_epub_metadata: bool,
) -> int:
    saved_state = load_gui_state()
    root = tk.Tk()
    root.title(runtime.APP_NAME)
    root.geometry("1180x640")
    result_queue: queue.Queue[tuple[int, list[str]] | tuple[str, str]] = queue.Queue()
    worker_state = {"running": False}
    trace_state = {"current_file": ""}

    folder_var = tk.StringVar(value=saved_state.get("source_folder") or default_folder)
    destination_var = tk.StringVar(value=saved_state.get("destination_folder") or default_destination)
    archive_var = tk.StringVar(value=saved_state.get("archive_folder") or default_archive_folder)
    online_mode_var = tk.StringVar(value=(saved_state.get("online_mode") or default_online_mode).upper())
    timeout_var = tk.StringVar(value=str(default_timeout))
    limit_var = tk.StringVar(value=str(default_limit))
    online_workers_var = tk.StringVar(value=str(default_online_workers))
    online_var = tk.BooleanVar(value=default_online)
    apply_var = tk.BooleanVar(value=False)
    skip_processed_var = tk.BooleanVar(value=default_skip_processed)
    write_epub_metadata_var = tk.BooleanVar(
        value=(saved_state.get("write_epub_metadata") or ("true" if default_write_epub_metadata else "false")).lower() == "true"
    )
    status_var = tk.StringVar(value="Gotowe.")

    def persist_gui_state() -> None:
        save_gui_state(
            source_folder=folder_var.get(),
            destination_folder=destination_var.get(),
            archive_folder=archive_var.get(),
            online_mode=online_mode_var.get(),
            write_epub_metadata=write_epub_metadata_var.get(),
        )

    def choose_folder() -> None:
        selected = filedialog.askdirectory(initialdir=folder_var.get() or default_folder)
        if selected:
            folder_var.set(selected)
            persist_gui_state()

    def choose_destination() -> None:
        selected = filedialog.askdirectory(initialdir=destination_var.get() or folder_var.get() or default_folder)
        if selected:
            destination_var.set(selected)
            persist_gui_state()

    def choose_archive_folder() -> None:
        selected = filedialog.askdirectory(initialdir=archive_var.get() or folder_var.get() or default_folder)
        if selected:
            archive_var.set(selected)
            persist_gui_state()

    def poll_result_queue() -> None:
        finished: tuple[int, list[str]] | None = None
        while True:
            try:
                item = result_queue.get_nowait()
            except queue.Empty:
                break

            if item and item[0] == "progress":
                _, message = item
                if output.index("end-1c") != "1.0":
                    output.insert(tk.END, "\n\n")
                output.insert(tk.END, message)
                output.see(tk.END)
                status_var.set(message.splitlines()[0] if message else "Przetwarzanie...")
                continue

            if item and item[0] == "trace":
                _, message = item
                trace_file = message.splitlines()[0].strip() if message else ""
                if trace_file != trace_state["current_file"]:
                    trace_state["current_file"] = trace_file
                    trace_output.delete("1.0", tk.END)
                elif trace_output.index("end-1c") != "1.0":
                    trace_output.insert(tk.END, "\n\n")
                trace_output.insert(tk.END, message)
                trace_output.see(tk.END)
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
            if output.index("end-1c") != "1.0":
                output.insert(tk.END, "\n\n--- PODSUMOWANIE ---\n")
            output.insert(tk.END, "\n".join(lines))
            output.see(tk.END)
            status_var.set(lines[0] if lines else "Gotowe.")
            if code == 0:
                messagebox.showinfo("Zakonczono", lines[0] if lines else "Gotowe.")
            else:
                messagebox.showwarning("Problem", lines[0] if lines else "Wystapil problem.")
            return

        if worker_state["running"]:
            root.after(150, poll_result_queue)

    def run_from_gui() -> None:
        if worker_state["running"]:
            return
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
        persist_gui_state()

        worker_state["running"] = True
        run_button.config(state=tk.DISABLED)
        status_var.set("Start...")
        output.delete("1.0", tk.END)
        trace_output.delete("1.0", tk.END)
        trace_state["current_file"] = ""

        def worker() -> None:
            try:
                result_queue.put(
                    runtime.run_job(
                        folder,
                        destination_folder=destination,
                        archive_folder=archive_folder,
                        online_mode=online_mode,
                        apply_changes=apply_changes,
                        use_online=use_online,
                        providers=providers,
                        timeout=timeout,
                        limit=limit,
                        online_workers=online_workers,
                        write_epub_metadata=write_epub_metadata,
                        emit_progress=lambda message: result_queue.put(("progress", message)),
                        emit_trace=lambda message: result_queue.put(("trace", message)),
                        skip_previously_processed=skip_processed,
                    )
                )
            except Exception as exc:
                result_queue.put(("error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()
        root.after(150, poll_result_queue)

    def close_window() -> None:
        persist_gui_state()
        root.destroy()

    frame = ttk.Frame(root, padding=12)
    frame.pack(fill=tk.BOTH, expand=True)

    ttk.Label(frame, text="Folder zrodlowy").grid(row=0, column=0, sticky="w")
    ttk.Entry(frame, textvariable=folder_var, width=70).grid(row=1, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(frame, text="Wybierz", command=choose_folder).grid(row=1, column=1, sticky="ew")

    ttk.Label(frame, text="Folder docelowy zmienionej nazwy").grid(row=2, column=0, sticky="w", pady=(10, 0))
    ttk.Entry(frame, textvariable=destination_var, width=70).grid(row=3, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(frame, text="Wybierz", command=choose_destination).grid(row=3, column=1, sticky="ew")
    ttk.Label(
        frame,
        text="Tutaj trafia kopia pliku ze zmieniona nazwa. Gdy pole jest puste, zmiana odbywa sie w folderze zrodlowym.",
    ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(4, 0))

    ttk.Label(frame, text="Folder archiwum oryginalow").grid(row=5, column=0, sticky="w", pady=(10, 0))
    ttk.Entry(frame, textvariable=archive_var, width=70).grid(row=6, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(frame, text="Wybierz", command=choose_archive_folder).grid(row=6, column=1, sticky="ew")
    ttk.Label(
        frame,
        text="Po udanym utworzeniu kopii ze zmieniona nazwa oryginal zostanie przeniesiony tutaj.",
    ).grid(row=7, column=0, columnspan=2, sticky="w", pady=(4, 0))

    options = ttk.Frame(frame)
    options.grid(row=8, column=0, columnspan=2, sticky="ew", pady=(10, 0))
    ttk.Checkbutton(options, text="Online", variable=online_var).grid(row=0, column=0, sticky="w")
    ttk.Checkbutton(options, text="Apply", variable=apply_var).grid(row=0, column=1, sticky="w", padx=(12, 0))
    ttk.Checkbutton(options, text="Pomijaj przetworzone", variable=skip_processed_var).grid(row=0, column=2, sticky="w", padx=(12, 0))
    ttk.Checkbutton(options, text="Zapisz metadane w pliku", variable=write_epub_metadata_var, command=persist_gui_state).grid(row=0, column=3, sticky="w", padx=(12, 0))
    ttk.Label(options, text="Tryb online").grid(row=0, column=4, sticky="w", padx=(20, 0))
    ttk.Radiobutton(options, text="PL", value="PL", variable=online_mode_var, command=persist_gui_state).grid(row=0, column=5, sticky="w")
    ttk.Radiobutton(options, text="PL+", value="PL+", variable=online_mode_var, command=persist_gui_state).grid(row=0, column=6, sticky="w")
    ttk.Radiobutton(options, text="EN", value="EN", variable=online_mode_var, command=persist_gui_state).grid(row=0, column=7, sticky="w")
    ttk.Label(options, text="Timeout").grid(row=0, column=8, sticky="w", padx=(20, 0))
    ttk.Entry(options, textvariable=timeout_var, width=8).grid(row=0, column=9, sticky="w")
    ttk.Label(options, text="Limit").grid(row=0, column=10, sticky="w", padx=(20, 0))
    ttk.Entry(options, textvariable=limit_var, width=8).grid(row=0, column=11, sticky="w")
    ttk.Label(options, text="Infer workers").grid(row=0, column=12, sticky="w", padx=(20, 0))
    ttk.Entry(options, textvariable=online_workers_var, width=8).grid(row=0, column=13, sticky="w")

    run_button = ttk.Button(frame, text="Uruchom", command=run_from_gui)
    run_button.grid(row=9, column=0, columnspan=2, sticky="ew", pady=(12, 0))

    ttk.Label(frame, textvariable=status_var).grid(row=10, column=0, columnspan=2, sticky="w", pady=(8, 0))

    panes = ttk.Panedwindow(frame, orient=tk.HORIZONTAL)
    panes.grid(row=11, column=0, columnspan=2, sticky="nsew", pady=(12, 0))

    progress_frame = ttk.Frame(panes)
    trace_frame = ttk.Frame(panes)
    panes.add(progress_frame, weight=1)
    panes.add(trace_frame, weight=1)

    ttk.Label(progress_frame, text="Przebieg").pack(anchor="w")
    output = tk.Text(progress_frame, wrap="word", height=20)
    output.pack(fill=tk.BOTH, expand=True)

    ttk.Label(trace_frame, text="Transformacja Aktualnego Pliku").pack(anchor="w")
    trace_output = tk.Text(trace_frame, wrap="word", height=20)
    trace_output.pack(fill=tk.BOTH, expand=True)

    ttk.Label(frame, text=runtime.GUI_FOOTER_TEXT, font=("Segoe UI", 8)).grid(
        row=12, column=0, columnspan=2, sticky="e", pady=(6, 0)
    )

    frame.columnconfigure(0, weight=1)
    frame.rowconfigure(11, weight=1)
    root.protocol("WM_DELETE_WINDOW", close_window)
    root.mainloop()
    return 0
