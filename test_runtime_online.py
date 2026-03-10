import json
import threading
import unittest
from types import SimpleNamespace

import runtime_online


class _Headers:
    def __init__(self, charset: str = "utf-8") -> None:
        self._charset = charset

    def get_content_charset(self) -> str:
        return self._charset


class _Response:
    def __init__(self, payload: str) -> None:
        self.headers = _Headers()
        self._payload = payload

    def read(self) -> bytes:
        return self._payload.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _Opener:
    def __init__(self, payload: str) -> None:
        self.payload = payload
        self.calls: list[tuple[object, float]] = []

    def open(self, request, timeout: float):
        self.calls.append((request, timeout))
        return _Response(self.payload)


class RuntimeOnlineTests(unittest.TestCase):
    def test_strip_html_tags_decodes_entities(self) -> None:
        value = runtime_online.strip_html_tags("<b>Królewska</b> &amp; <i>klatka</i>", clean=lambda text: " ".join((text or "").split()))
        self.assertEqual(value, "Królewska & klatka")

    def test_reserve_lubimyczytac_request_delay_updates_next_slot(self) -> None:
        state = SimpleNamespace(
            lubimyczytac_rate_lock=threading.Lock(),
            lubimyczytac_next_request_at=10.0,
            lubimyczytac_delay_range=(2.4, 5.6),
        )

        delay = runtime_online.reserve_lubimyczytac_request_delay(
            state,
            cache_online_mod=SimpleNamespace(reserve_lubimyczytac_request_delay=lambda state, now=None, uniform_func=None: 3.0),
            sync_state_back=lambda state: None,
            uniform_func=lambda left, right: 2.5,
            now=7.0,
        )

        self.assertEqual(delay, 3.0)

    def test_online_fetch_parses_and_caches_json_payload(self) -> None:
        cache: dict[str, object | None] = {}
        opener = _Opener(json.dumps({"ok": True}))
        result = runtime_online.online_fetch(
            "https://example.test/data.json",
            2.0,
            kind="json",
            online_cache_key=lambda kind, url: f"{kind}:{url}",
            get_cached_online_error=lambda key: None,
            cache_lock=threading.Lock(),
            cache=cache,
            inflight={},
            build_online_request=lambda url: {"url": url},
            is_lubimyczytac_url=lambda url: False,
            ensure_lubimyczytac_session=lambda timeout: None,
            wait_for_lubimyczytac_request_slot=lambda: None,
            opener=opener,
            should_persist_online_cache_entry=lambda key, payload: False,
            mark_online_cache_dirty=lambda: None,
            flush_online_cache_if_needed=lambda **kwargs: None,
            cache_online_error=lambda key, message: None,
        )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(cache["json:https://example.test/data.json"], {"ok": True})
        self.assertEqual(len(opener.calls), 1)


if __name__ == "__main__":
    unittest.main()
