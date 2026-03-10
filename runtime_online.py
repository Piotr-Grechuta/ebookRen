from __future__ import annotations

import html
import json
import logging
import re
import time
from typing import Callable


class MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


def configure_logging(logger: logging.Logger) -> None:
    if logger.handlers:
        return
    logger.setLevel(logging.INFO)
    logger.propagate = False

    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.addFilter(MaxLevelFilter(logging.WARNING - 1))
    stdout_handler.setFormatter(logging.Formatter("%(message)s"))

    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)


def log_lines(logger: logging.Logger, lines: list[str], *, level: int = logging.INFO) -> None:
    for line in lines:
        logger.log(level, line)


def strip_html_tags(text: str | None, *, clean: Callable[[str | None], str]) -> str:
    if not text:
        return ""
    return clean(html.unescape(re.sub(r"<[^>]+>", " ", text)))


def load_online_cache(state, *, cache_online_mod, sync_state_back: Callable[[object], None]) -> None:
    cache_online_mod.load_online_cache(state)
    sync_state_back(state)


def save_online_cache(state, *, cache_online_mod, sync_state_back: Callable[[object], None]) -> None:
    cache_online_mod.save_online_cache(state)
    sync_state_back(state)


def mark_online_cache_dirty(state, *, cache_online_mod, sync_state_back: Callable[[object], None]) -> None:
    cache_online_mod.mark_online_cache_dirty(state)
    sync_state_back(state)


def flush_online_cache_if_needed(state, *, cache_online_mod, sync_state_back: Callable[[object], None], force: bool = False) -> None:
    cache_online_mod.flush_online_cache_if_needed(state, force=force)
    sync_state_back(state)


def cache_online_error(state, cache_key: str, message: str, *, cache_online_mod, sync_state_back: Callable[[object], None]) -> None:
    cache_online_mod.cache_online_error(state, cache_key, message)
    sync_state_back(state)


def reserve_lubimyczytac_request_delay(state, *, cache_online_mod, sync_state_back: Callable[[object], None], uniform_func, now: float | None = None) -> float:
    delay = cache_online_mod.reserve_lubimyczytac_request_delay(state, now=now, uniform_func=uniform_func)
    sync_state_back(state)
    return delay


def wait_for_lubimyczytac_request_slot(state, *, cache_online_mod, sync_state_back: Callable[[object], None], uniform_func) -> None:
    cache_online_mod.wait_for_lubimyczytac_request_slot(state, sleep_func=time.sleep, uniform_func=uniform_func)
    sync_state_back(state)


def ensure_lubimyczytac_session(state, timeout: float, *, cache_online_mod, sync_state_back: Callable[[object], None]) -> None:
    cache_online_mod.ensure_lubimyczytac_session(state, timeout)
    sync_state_back(state)


def online_fetch(
    url: str,
    timeout: float,
    *,
    kind: str,
    online_cache_key: Callable[[str, str], str],
    get_cached_online_error: Callable[[str], str | None],
    cache_lock,
    cache: dict[str, object | None],
    inflight: dict[str, object],
    build_online_request: Callable[[str], object],
    is_lubimyczytac_url: Callable[[str], bool],
    ensure_lubimyczytac_session: Callable[[float], None],
    wait_for_lubimyczytac_request_slot: Callable[[], None],
    opener,
    should_persist_online_cache_entry: Callable[[str, object | None], bool],
    mark_online_cache_dirty: Callable[[], None],
    flush_online_cache_if_needed: Callable[..., None],
    cache_online_error: Callable[[str, str], None],
) -> object | None:
    cache_key = online_cache_key(kind, url)
    cached_error = get_cached_online_error(cache_key)
    if cached_error is not None:
        return None
    with cache_lock:
        if cache_key in cache:
            return cache[cache_key]
        in_flight = inflight.get(cache_key)
        if in_flight is None:
            import threading

            in_flight = threading.Event()
            inflight[cache_key] = in_flight
            is_owner = True
        else:
            is_owner = False

    if not is_owner:
        in_flight.wait()
        with cache_lock:
            return cache.get(cache_key)

    try:
        request = build_online_request(url)
        if is_lubimyczytac_url(url):
            ensure_lubimyczytac_session(timeout)
            wait_for_lubimyczytac_request_slot()
        with opener.open(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            raw = response.read().decode(charset, errors="ignore")
        payload: object = json.loads(raw) if kind == "json" else raw
        with cache_lock:
            cache[cache_key] = payload
        if should_persist_online_cache_entry(cache_key, payload):
            mark_online_cache_dirty()
            flush_online_cache_if_needed()
        return payload
    except Exception as exc:
        cache_online_error(cache_key, str(exc))
        return None
    finally:
        with cache_lock:
            event = inflight.pop(cache_key, None)
            if event is not None:
                event.set()


def online_query(url: str, timeout: float, *, online_fetch: Callable[..., object | None]) -> dict | None:
    payload = online_fetch(url, timeout, kind="json")
    return payload if isinstance(payload, dict) else None


def online_text_query(url: str, timeout: float, *, online_fetch: Callable[..., object | None]) -> str | None:
    payload = online_fetch(url, timeout, kind="text")
    return payload if isinstance(payload, str) else None
