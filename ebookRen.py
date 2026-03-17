import app_gui
import app_runtime as runtime


def main() -> int:
    runtime.configure_logging()
    return app_gui.launch_gui(
        runtime.DEFAULT_SOURCE_FOLDER,
        "",
        "",
        runtime.DEFAULT_ONLINE_MODE,
        runtime.DEFAULT_PROVIDERS,
        runtime.DEFAULT_HTTP_TIMEOUT,
        0,
        False,
        runtime.DEFAULT_INFER_WORKERS,
        False,
        True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
