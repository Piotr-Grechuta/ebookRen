from __future__ import annotations

import http.cookiejar
import json
import os
import random
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path


@dataclass
class OnlineCacheState:
    cache: dict[str, object | None]
    cache_lock: threading.Lock
    inflight: dict[str, threading.Event]
    error_cache: dict[str, tuple[float, str]]
    opener: urllib.request.OpenerDirector
    cache_path: Path
    cache_dirty: bool
    cache_pending_writes: int
    cache_last_save: float
    cache_save_every: int
    cache_save_interval: float
    error_cache_ttl: float
    lubimyczytac_host: str
    lubimyczytac_delay_range: tuple[float, float]
    lubimyczytac_rate_lock: threading.Lock
    lubimyczytac_next_request_at: float
    lubimyczytac_session_ready: bool
    lubimyczytac_session_lock: threading.Lock
    app_name: str
    app_version: str


def should_persist_online_cache_entry(key: str, value: object | None) -> bool:
    if value is None:
        return True
    return not isinstance(value, str)


def build_persistent_online_cache_snapshot(state: OnlineCacheState) -> dict[str, object | None]:
    with state.cache_lock:
        return {
            key: value
            for key, value in state.cache.items()
            if should_persist_online_cache_entry(key, value)
        }


def load_online_cache(state: OnlineCacheState) -> None:
    if not state.cache_path.exists():
        return
    try:
        payload = json.loads(state.cache_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(payload, dict):
        return
    pruned_entries = False
    with state.cache_lock:
        state.cache.clear()
        for key, value in payload.items():
            if not isinstance(key, str):
                continue
            if should_persist_online_cache_entry(key, value):
                state.cache[key] = value
            else:
                pruned_entries = True
        state.cache_dirty = pruned_entries
        state.cache_pending_writes = 1 if pruned_entries else 0
        state.cache_last_save = time.perf_counter()


def save_online_cache(state: OnlineCacheState) -> None:
    with state.cache_lock:
        pending_writes = state.cache_pending_writes
    snapshot = build_persistent_online_cache_snapshot(state)
    state.cache_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = state.cache_path.with_suffix(".tmp")
    temp_path.write_text(json.dumps(snapshot, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(temp_path, state.cache_path)
    with state.cache_lock:
        state.cache_pending_writes = max(0, state.cache_pending_writes - pending_writes)
        state.cache_dirty = state.cache_pending_writes > 0
        state.cache_last_save = time.perf_counter()


def mark_online_cache_dirty(state: OnlineCacheState) -> None:
    with state.cache_lock:
        state.cache_dirty = True
        state.cache_pending_writes += 1


def flush_online_cache_if_needed(state: OnlineCacheState, *, force: bool = False) -> None:
    with state.cache_lock:
        pending_writes = state.cache_pending_writes
        last_save = state.cache_last_save
        dirty = state.cache_dirty
    if not dirty:
        return
    if not force and pending_writes < state.cache_save_every and (time.perf_counter() - last_save) < state.cache_save_interval:
        return
    save_online_cache(state)


def get_cached_online_error(state: OnlineCacheState, cache_key: str) -> str | None:
    with state.cache_lock:
        item = state.error_cache.get(cache_key)
        if item is None:
            return None
        expires_at, message = item
        if expires_at <= time.time():
            state.error_cache.pop(cache_key, None)
            return None
        return message


def cache_online_error(state: OnlineCacheState, cache_key: str, message: str) -> None:
    with state.cache_lock:
        state.error_cache[cache_key] = (time.time() + state.error_cache_ttl, message)


def online_cache_key(kind: str, url: str) -> str:
    return f"{kind}:{url}"


def is_lubimyczytac_url(state: OnlineCacheState, url: str) -> bool:
    hostname = urllib.parse.urlparse(url).netloc.lower()
    return hostname == state.lubimyczytac_host or hostname.endswith(f".{state.lubimyczytac_host}")


def reserve_lubimyczytac_request_delay(
    state: OnlineCacheState,
    *,
    now: float | None = None,
    uniform_func=random.uniform,
) -> float:
    current_time = time.monotonic() if now is None else now
    with state.lubimyczytac_rate_lock:
        sleep_for = max(0.0, state.lubimyczytac_next_request_at - current_time)
        scheduled_time = current_time + sleep_for
        state.lubimyczytac_next_request_at = scheduled_time + uniform_func(*state.lubimyczytac_delay_range)
    return sleep_for


def wait_for_lubimyczytac_request_slot(
    state: OnlineCacheState,
    *,
    sleep_func=time.sleep,
    uniform_func=random.uniform,
) -> None:
    delay = reserve_lubimyczytac_request_delay(state, uniform_func=uniform_func)
    if delay > 0:
        sleep_func(delay)


def build_online_request(state: OnlineCacheState, url: str) -> urllib.request.Request:
    headers = {"User-Agent": f"{state.app_name}/{state.app_version}"}
    if is_lubimyczytac_url(state, url):
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": "https://lubimyczytac.pl/",
        }
    return urllib.request.Request(url, headers=headers)


def ensure_lubimyczytac_session(state: OnlineCacheState, timeout: float) -> None:
    if state.lubimyczytac_session_ready:
        return
    with state.lubimyczytac_session_lock:
        if state.lubimyczytac_session_ready:
            return
        request = build_online_request(state, "https://lubimyczytac.pl/")
        with state.opener.open(request, timeout=timeout) as response:
            response.read()
        state.lubimyczytac_session_ready = True


def make_default_opener() -> urllib.request.OpenerDirector:
    cookie_jar = http.cookiejar.CookieJar()
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
