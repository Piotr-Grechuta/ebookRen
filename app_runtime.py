import difflib
import logging
import random
import threading
import urllib.request
import atexit
from pathlib import Path
from typing import Callable, Iterable

import author_catalog as author_catalog_mod
import cache_online as cache_online_mod
import candidate_scorer as candidate_scorer_mod
import domain_naming as domain_naming_mod
import infer_engine as infer_engine_mod
import infer_flow as infer_flow_mod
import infer_core as infer_core_mod
import job_runner as job_runner_mod
import local_parser as local_parser_mod
import providers_online as providers_online_mod
import runtime_metadata as runtime_metadata_mod
import runtime_online as runtime_online_mod
import runtime_text as runtime_text_mod
from fs_ops import (
    build_moves as fs_build_moves,
    build_undo_plan as fs_build_undo_plan,
    execute_moves as fs_execute_moves,
    execute_undo as fs_execute_undo,
    rollback_moves as fs_rollback_moves,
    validate_move_collisions as fs_validate_move_collisions,
)
from models_core import (
    Candidate,
    EpubMetadata,
    HybridLocalParse,
    LocalPrototype,
    LubimyczytacResult,
    OnlineCandidate,
    OnlineRoleEvidence,
    OnlineVerification,
    RankedOnlineMatch,
    RenameMove,
    UndoPlan,
)
from runtime_config import (
    ANNA_ARCHIVE_RE,
    APP_NAME,
    APP_VERSION,
    BLOCKING_REVIEW_REASONS,
    BOX_SET_RE,
    CORE_COMMA_RE,
    CORE_INDEX_ONLY_RE,
    CORE_JOINED_RE,
    CORE_SPACED_RE,
    CORE_TITLE_AUTHOR_RE,
    DEFAULT_HTTP_TIMEOUT,
    DEFAULT_INFER_WORKERS,
    DEFAULT_ONLINE_MODE,
    DEFAULT_PROVIDERS,
    DEFAULT_SOURCE_FOLDER,
    DEVICE_NAMES,
    GENRE_SUFFIX_RE,
    GENRE_TAIL_RE,
    GUI_FOOTER_TEXT,
    HEX_NOISE_RE,
    INDEXED_TITLE_RE,
    INDEX_ONLY_RE,
    ISBN_RE,
    LEADING_INDEX_DOTTED_TITLE_RE,
    LEADING_INDEX_TITLE_RE,
    LUBIMYCZYTAC_HOST,
    LUBIMYCZYTAC_SEARCH_DELAY_RANGE,
    NULLISH_RE,
    ONLINE_AMBIGUITY_MARGIN,
    ONLINE_ERROR_CACHE_TTL,
    ONLINE_HTTP_SLOTS,
    PAREN_SERIES_RE,
    PROVIDER_SCORE_ADJUSTMENTS,
    PUBLISHER_LIKE_RE,
    QUERY_NOISE_PAREN_RE,
    SEGMENT_COMMA_RE,
    SEGMENT_HASH_RE,
    SEGMENT_YEAR_RE,
    SERIES_ONLY_PAREN_INDEX_RE,
    SERIES_SOURCE_PRIORITIES,
    SERIES_WORDS,
    SOURCE_ARTIFACT_RE,
    SUPPORTED_BOOK_EXTENSIONS,
    TITLE_COLON_SERIES_INDEX_RE,
    TITLE_DOTTED_SERIES_BOOK_RE,
    TITLE_DOUBLE_COLON_BOOK_RE,
    TITLE_WITH_SERIES_RE,
    TRAILING_BOOK_INDEX_RE,
    TRAILING_SERIES_SUFFIX_RE,
    VOLUME_INDEX_PATTERN,
)

__all__ = [
    "APP_NAME",
    "APP_VERSION",
    "LocalPrototype",
    "DEFAULT_HTTP_TIMEOUT",
    "DEFAULT_INFER_WORKERS",
    "DEFAULT_ONLINE_MODE",
    "DEFAULT_PROVIDERS",
    "DEFAULT_SOURCE_FOLDER",
    "GUI_FOOTER_TEXT",
    "BLOCKING_REVIEW_REASONS",
    "ONLINE_AMBIGUITY_MARGIN",
    "VOLUME_INDEX_PATTERN",
]

ONLINE_CACHE: dict[str, object | None] = {}
ONLINE_CACHE_LOCK = threading.Lock()
ONLINE_CACHE_INFLIGHT: dict[str, threading.Event] = {}
ONLINE_ERROR_CACHE: dict[str, tuple[float, str]] = {}
ONLINE_ENRICH_SEMAPHORE = threading.BoundedSemaphore(ONLINE_HTTP_SLOTS)
ONLINE_HTTP_OPENER = cache_online_mod.make_default_opener()
ONLINE_CACHE_PATH = Path(__file__).with_name("online_cache.json")
ONLINE_CACHE_DIRTY = False
ONLINE_CACHE_PENDING_WRITES = 0
ONLINE_CACHE_LAST_SAVE = 0.0
AUTHOR_PATTERNS_PATH = Path(__file__).with_name("author_patterns.csv")
_AUTHOR_CATALOG: author_catalog_mod.AuthorCatalog | None = None
ONLINE_CACHE_SAVE_EVERY = 10
ONLINE_CACHE_SAVE_INTERVAL = 5.0
LUBIMYCZYTAC_RATE_LOCK = threading.Lock()
LUBIMYCZYTAC_NEXT_REQUEST_AT = 0.0
LUBIMYCZYTAC_SESSION_READY = False
LUBIMYCZYTAC_SESSION_LOCK = threading.Lock()

LOGGER = logging.getLogger(APP_NAME)


_ONLINE_CACHE_STATE = cache_online_mod.OnlineCacheState(
    cache=ONLINE_CACHE,
    cache_lock=ONLINE_CACHE_LOCK,
    inflight=ONLINE_CACHE_INFLIGHT,
    error_cache=ONLINE_ERROR_CACHE,
    opener=ONLINE_HTTP_OPENER,
    cache_path=ONLINE_CACHE_PATH,
    cache_dirty=ONLINE_CACHE_DIRTY,
    cache_pending_writes=ONLINE_CACHE_PENDING_WRITES,
    cache_last_save=ONLINE_CACHE_LAST_SAVE,
    cache_save_every=ONLINE_CACHE_SAVE_EVERY,
    cache_save_interval=ONLINE_CACHE_SAVE_INTERVAL,
    error_cache_ttl=ONLINE_ERROR_CACHE_TTL,
    lubimyczytac_host=LUBIMYCZYTAC_HOST,
    lubimyczytac_delay_range=LUBIMYCZYTAC_SEARCH_DELAY_RANGE,
    lubimyczytac_rate_lock=LUBIMYCZYTAC_RATE_LOCK,
    lubimyczytac_next_request_at=LUBIMYCZYTAC_NEXT_REQUEST_AT,
    lubimyczytac_session_ready=LUBIMYCZYTAC_SESSION_READY,
    lubimyczytac_session_lock=LUBIMYCZYTAC_SESSION_LOCK,
    app_name=APP_NAME,
    app_version=APP_VERSION,
)


def _cache_state() -> cache_online_mod.OnlineCacheState:
    _ONLINE_CACHE_STATE.cache = ONLINE_CACHE
    _ONLINE_CACHE_STATE.cache_lock = ONLINE_CACHE_LOCK
    _ONLINE_CACHE_STATE.inflight = ONLINE_CACHE_INFLIGHT
    _ONLINE_CACHE_STATE.error_cache = ONLINE_ERROR_CACHE
    _ONLINE_CACHE_STATE.opener = ONLINE_HTTP_OPENER
    _ONLINE_CACHE_STATE.cache_path = ONLINE_CACHE_PATH
    _ONLINE_CACHE_STATE.cache_dirty = ONLINE_CACHE_DIRTY
    _ONLINE_CACHE_STATE.cache_pending_writes = ONLINE_CACHE_PENDING_WRITES
    _ONLINE_CACHE_STATE.cache_last_save = ONLINE_CACHE_LAST_SAVE
    _ONLINE_CACHE_STATE.cache_save_every = ONLINE_CACHE_SAVE_EVERY
    _ONLINE_CACHE_STATE.cache_save_interval = ONLINE_CACHE_SAVE_INTERVAL
    _ONLINE_CACHE_STATE.error_cache_ttl = ONLINE_ERROR_CACHE_TTL
    _ONLINE_CACHE_STATE.lubimyczytac_host = LUBIMYCZYTAC_HOST
    _ONLINE_CACHE_STATE.lubimyczytac_delay_range = LUBIMYCZYTAC_SEARCH_DELAY_RANGE
    _ONLINE_CACHE_STATE.lubimyczytac_rate_lock = LUBIMYCZYTAC_RATE_LOCK
    _ONLINE_CACHE_STATE.lubimyczytac_next_request_at = LUBIMYCZYTAC_NEXT_REQUEST_AT
    _ONLINE_CACHE_STATE.lubimyczytac_session_ready = LUBIMYCZYTAC_SESSION_READY
    _ONLINE_CACHE_STATE.lubimyczytac_session_lock = LUBIMYCZYTAC_SESSION_LOCK
    _ONLINE_CACHE_STATE.app_name = APP_NAME
    _ONLINE_CACHE_STATE.app_version = APP_VERSION
    return _ONLINE_CACHE_STATE


def _sync_cache_state_back(state: cache_online_mod.OnlineCacheState) -> None:
    global ONLINE_CACHE_DIRTY, ONLINE_CACHE_PENDING_WRITES, ONLINE_CACHE_LAST_SAVE
    global LUBIMYCZYTAC_NEXT_REQUEST_AT, LUBIMYCZYTAC_SESSION_READY
    ONLINE_CACHE_DIRTY = state.cache_dirty
    ONLINE_CACHE_PENDING_WRITES = state.cache_pending_writes
    ONLINE_CACHE_LAST_SAVE = state.cache_last_save
    LUBIMYCZYTAC_NEXT_REQUEST_AT = state.lubimyczytac_next_request_at
    LUBIMYCZYTAC_SESSION_READY = state.lubimyczytac_session_ready


BookRecord = domain_naming_mod.BookRecord
LubimyczytacSearchParser = providers_online_mod.build_lubimyczytac_search_parser_factory(
    clean=lambda text: infer_core_mod.clean(text),
    clean_series=lambda text: infer_core_mod.clean_series(text),
    parse_volume_parts=lambda text: infer_core_mod.parse_volume_parts(text),
    series_only_paren_index_re=SERIES_ONLY_PAREN_INDEX_RE,
    result_type=LubimyczytacResult,
)

clean = infer_core_mod.clean
clean_series = infer_core_mod.clean_series


def is_publisher_like(text: str | None) -> bool:
    return runtime_text_mod.is_publisher_like(text, clean=clean, publisher_like_re=PUBLISHER_LIKE_RE)


def strip_source_artifacts(text: str | None) -> str:
    return runtime_text_mod.strip_source_artifacts(
        text,
        clean=clean,
        source_artifact_re=SOURCE_ARTIFACT_RE,
        hex_noise_re=HEX_NOISE_RE,
    )


def is_source_artifact(text: str | None) -> bool:
    return runtime_text_mod.is_source_artifact(
        text,
        clean=clean,
        source_artifact_re=SOURCE_ARTIFACT_RE,
        nullish_re=NULLISH_RE,
    )


def looks_like_author_segment(text: str | None) -> bool:
    return runtime_text_mod.looks_like_author_segment(
        text,
        strip_source_artifacts=strip_source_artifacts,
        is_publisher_like=is_publisher_like,
        is_source_artifact=is_source_artifact,
    )


def clean_author_segment(text: str | None) -> str:
    return runtime_text_mod.clean_author_segment(text, strip_source_artifacts=strip_source_artifacts, clean=clean)


MaxLevelFilter = runtime_online_mod.MaxLevelFilter


def configure_logging() -> None:
    runtime_online_mod.configure_logging(LOGGER)


def log_lines(lines: list[str], *, level: int = logging.INFO) -> None:
    runtime_online_mod.log_lines(LOGGER, lines, level=level)


def strip_html_tags(text: str | None) -> str:
    return runtime_online_mod.strip_html_tags(text, clean=clean)


def load_online_cache() -> None:
    runtime_online_mod.load_online_cache(
        _cache_state(),
        cache_online_mod=cache_online_mod,
        sync_state_back=_sync_cache_state_back,
    )


def should_persist_online_cache_entry(key: str, value: object | None) -> bool:
    return cache_online_mod.should_persist_online_cache_entry(key, value)


def build_persistent_online_cache_snapshot() -> dict[str, object | None]:
    return cache_online_mod.build_persistent_online_cache_snapshot(_cache_state())


def save_online_cache() -> None:
    runtime_online_mod.save_online_cache(
        _cache_state(),
        cache_online_mod=cache_online_mod,
        sync_state_back=_sync_cache_state_back,
    )


def mark_online_cache_dirty() -> None:
    runtime_online_mod.mark_online_cache_dirty(
        _cache_state(),
        cache_online_mod=cache_online_mod,
        sync_state_back=_sync_cache_state_back,
    )


def flush_online_cache_if_needed(force: bool = False) -> None:
    runtime_online_mod.flush_online_cache_if_needed(
        _cache_state(),
        cache_online_mod=cache_online_mod,
        sync_state_back=_sync_cache_state_back,
        force=force,
    )


def get_cached_online_error(cache_key: str) -> str | None:
    return cache_online_mod.get_cached_online_error(_cache_state(), cache_key)


def cache_online_error(cache_key: str, message: str) -> None:
    runtime_online_mod.cache_online_error(
        _cache_state(),
        cache_key,
        message,
        cache_online_mod=cache_online_mod,
        sync_state_back=_sync_cache_state_back,
    )


def online_cache_key(kind: str, url: str) -> str:
    return cache_online_mod.online_cache_key(kind, url)


def is_lubimyczytac_url(url: str) -> bool:
    return cache_online_mod.is_lubimyczytac_url(_cache_state(), url)


def reserve_lubimyczytac_request_delay(now: float | None = None) -> float:
    return runtime_online_mod.reserve_lubimyczytac_request_delay(
        _cache_state(),
        cache_online_mod=cache_online_mod,
        sync_state_back=_sync_cache_state_back,
        uniform_func=random.uniform,
        now=now,
    )


def wait_for_lubimyczytac_request_slot() -> None:
    runtime_online_mod.wait_for_lubimyczytac_request_slot(
        _cache_state(),
        cache_online_mod=cache_online_mod,
        sync_state_back=_sync_cache_state_back,
        uniform_func=random.uniform,
    )


def build_online_request(url: str) -> urllib.request.Request:
    return cache_online_mod.build_online_request(_cache_state(), url)


def ensure_lubimyczytac_session(timeout: float) -> None:
    runtime_online_mod.ensure_lubimyczytac_session(
        _cache_state(),
        timeout,
        cache_online_mod=cache_online_mod,
        sync_state_back=_sync_cache_state_back,
    )


def is_strong_online_candidate(candidate: object) -> bool:
    return infer_flow_mod.is_strong_online_candidate(
        candidate,
        is_online_candidate=is_online_candidate,
        clean_series=clean_series,
    )


def register_online_role_text(bucket: dict[str, str], text: str | None, *, author_role: bool = False) -> None:
    infer_flow_mod.register_online_role_text(
        bucket,
        text,
        clean=clean,
        clean_author_segment=clean_author_segment,
        author_key=author_key,
        normalize_match_text=normalize_match_text,
        author_role=author_role,
    )


def collect_online_candidate_candidates(candidate: OnlineCandidate) -> list[Candidate]:
    return infer_flow_mod.collect_online_candidate_candidates(
        candidate,
        add_candidate=add_candidate,
        collect_title_candidates=collect_title_candidates,
        collect_core_candidates=collect_core_candidates,
    )


def collect_online_role_evidence(candidates: list[OnlineCandidate]) -> OnlineRoleEvidence:
    return infer_flow_mod.collect_online_role_evidence(
        candidates,
        is_strong_online_candidate=is_strong_online_candidate,
        canonicalize_authors=canonicalize_authors,
        register_online_role_text_fn=register_online_role_text,
        collect_online_candidate_candidates=collect_online_candidate_candidates,
        choose_series_candidate=choose_series_candidate,
        choose_title_candidate=choose_title_candidate,
    )


def best_matching_online_text(
    values: Iterable[str],
    evidence: dict[str, str],
    *,
    author_role: bool = False,
    threshold: float = 0.92,
) -> str | None:
    return infer_flow_mod.best_matching_online_text(
        values,
        evidence,
        clean=clean,
        clean_author_segment=clean_author_segment,
        author_match_keys=author_match_keys,
        normalize_match_text=normalize_match_text,
        similarity_score=similarity_score,
        author_role=author_role,
        threshold=threshold,
    )


def online_fetch(url: str, timeout: float, *, kind: str) -> object | None:
    return runtime_online_mod.online_fetch(
        url,
        timeout,
        kind=kind,
        online_cache_key=online_cache_key,
        get_cached_online_error=get_cached_online_error,
        cache_lock=ONLINE_CACHE_LOCK,
        cache=ONLINE_CACHE,
        inflight=ONLINE_CACHE_INFLIGHT,
        build_online_request=build_online_request,
        is_lubimyczytac_url=is_lubimyczytac_url,
        ensure_lubimyczytac_session=ensure_lubimyczytac_session,
        wait_for_lubimyczytac_request_slot=wait_for_lubimyczytac_request_slot,
        opener=ONLINE_HTTP_OPENER,
        should_persist_online_cache_entry=should_persist_online_cache_entry,
        mark_online_cache_dirty=mark_online_cache_dirty,
        flush_online_cache_if_needed=flush_online_cache_if_needed,
        cache_online_error=cache_online_error,
    )


def parse_lubimyczytac_detail_page(page: str) -> tuple[str, tuple[int, str] | None, list[str]]:
    return providers_online_mod.parse_lubimyczytac_detail_page(
        page,
        clean=clean,
        strip_html_tags=strip_html_tags,
        clean_series=clean_series,
        parse_volume_parts=parse_volume_parts,
        series_only_paren_index_re=SERIES_ONLY_PAREN_INDEX_RE,
    )


def enrich_lubimyczytac_result(result: LubimyczytacResult, timeout: float) -> LubimyczytacResult:
    return providers_online_mod.enrich_lubimyczytac_result(
        result,
        timeout,
        online_text_query=online_text_query,
        parse_detail_page=parse_lubimyczytac_detail_page,
        result_type=LubimyczytacResult,
    )





def extract_isbns(values: Iterable[str]) -> list[str]:
    return infer_core_mod.extract_isbns(values, isbn_re=ISBN_RE)


def fold_text(text: str | None) -> str:
    return infer_core_mod.fold_text(text)


def normalize_match_text(text: str | None) -> str:
    return infer_core_mod.normalize_match_text(text)


def _author_catalog() -> author_catalog_mod.AuthorCatalog:
    global _AUTHOR_CATALOG
    if _AUTHOR_CATALOG is None:
        _AUTHOR_CATALOG = author_catalog_mod.load_author_catalog(AUTHOR_PATTERNS_PATH)
    return _AUTHOR_CATALOG


def resolve_known_author(text: str | None) -> str:
    return _author_catalog().resolve(clean_author_segment(text))


def is_known_author(text: str | None) -> bool:
    return _author_catalog().is_known(clean_author_segment(text))


def split_known_author_prefix(text: str | None) -> tuple[str, str] | None:
    return _author_catalog().split_prefix(clean(text))


def split_known_author_suffix(text: str | None) -> tuple[str, str] | None:
    return _author_catalog().split_suffix(clean(text))


def resolve_author_segment(text: str | None) -> list[str]:
    return _author_catalog().resolve_authors(clean_author_segment(text))


def parse_hybrid_local(meta: EpubMetadata) -> HybridLocalParse:
    return local_parser_mod.parse_hybrid_local(
        meta,
        clean=clean,
        clean_author_segment=clean_author_segment,
        looks_like_author_segment=looks_like_author_segment,
        strip_leading_title_index=strip_leading_title_index,
        parse_volume_parts=parse_volume_parts,
        resolve_known_author=resolve_known_author,
        is_known_author=is_known_author,
        split_known_author_prefix=split_known_author_prefix,
        split_known_author_suffix=split_known_author_suffix,
        resolve_author_segment=resolve_author_segment,
    )


def choose_best_local_series_candidate(meta: EpubMetadata, candidates: list[Candidate]) -> Candidate | None:
    return candidate_scorer_mod.choose_best_local_series_candidate(
        meta,
        candidates,
        clean=clean,
        normalize_match_text=normalize_match_text,
        similarity_score=similarity_score,
        strip_leading_title_index=strip_leading_title_index,
        looks_like_author_segment=looks_like_author_segment,
        series_source_priorities=SERIES_SOURCE_PRIORITIES,
    )


def choose_best_local_title_candidate(meta: EpubMetadata, candidates: list[Candidate], selected_series: str) -> Candidate | None:
    return candidate_scorer_mod.choose_best_local_title_candidate(
        meta,
        candidates,
        selected_series,
        clean=clean,
        normalize_match_text=normalize_match_text,
        similarity_score=similarity_score,
        strip_leading_title_index=strip_leading_title_index,
        looks_like_author_segment=looks_like_author_segment,
    )


def infer_book_genre(labels: Iterable[str]) -> str:
    return infer_core_mod.infer_book_genre(labels)


def format_title_with_genre(title: str, genre: str) -> str:
    return infer_core_mod.format_title_with_genre(title, genre, genre_suffix_re=GENRE_SUFFIX_RE)


def split_title_genre_suffix(title: str) -> tuple[str, str]:
    return infer_core_mod.split_title_genre_suffix(title, genre_suffix_re=GENRE_SUFFIX_RE)


def similarity_score(left: str | None, right: str | None) -> float:
    return infer_core_mod.similarity_score(left, right)


def author_key(text: str) -> str:
    return infer_core_mod.author_key(text)


def parse_volume_parts(text: str | None) -> tuple[int, str] | None:
    return infer_core_mod.parse_volume_parts(text)


def format_volume(volume: tuple[int, str] | None) -> str:
    return infer_core_mod.format_volume(volume)


def volume_match_pattern(volume: tuple[int, str] | None) -> str:
    return infer_core_mod.volume_match_pattern(volume)


def sanitize_component(text: str) -> str:
    return infer_core_mod.sanitize_component(text, device_names=DEVICE_NAMES)


def trim_title_for_path(folder: Path, author: str, series: str, volume: str, title: str) -> str:
    return infer_core_mod.trim_title_for_path(folder, author, series, volume, title)


def split_authors(text: str) -> list[str]:
    return infer_core_mod.split_authors(text, clean_author_segment=clean_author_segment)


def canonicalize_authors(authors: Iterable[str]) -> list[str]:
    return infer_core_mod.canonicalize_authors(authors)


def to_last_first(name: str) -> str:
    return infer_core_mod.to_last_first(name)


def is_supported_book_file(path: Path) -> bool:
    return runtime_text_mod.is_supported_book_file(path, supported_extensions=SUPPORTED_BOOK_EXTENSIONS)


def author_match_keys(values: Iterable[str]) -> set[str]:
    return domain_naming_mod.author_match_keys(values)


def rank_online_candidate(meta: EpubMetadata, title: str, authors: list[str], identifiers: Iterable[str]) -> tuple[int, str]:
    return domain_naming_mod.rank_online_candidate(meta, title, authors, identifiers, split_authors=split_authors)


def online_confidence(score: int) -> int:
    return domain_naming_mod.online_confidence(score)


def build_online_candidates(
    meta: EpubMetadata,
    source: str,
    provider_label: str,
    candidates: Iterable[tuple[str, list[str], list[str]] | tuple[str, list[str], list[str], list[str]]],
) -> list[OnlineCandidate]:
    return domain_naming_mod.build_online_candidates(
        meta,
        source,
        provider_label,
        candidates,
        provider_score_adjustments=PROVIDER_SCORE_ADJUSTMENTS,
        split_authors=split_authors,
    )


def online_candidate_group_key(candidate: OnlineCandidate) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    return domain_naming_mod.online_candidate_group_key(candidate)


def is_online_candidate(candidate: object) -> bool:
    return domain_naming_mod.is_online_candidate(candidate)


def aggregate_online_candidates(candidates: Iterable[OnlineCandidate]) -> list[RankedOnlineMatch]:
    return domain_naming_mod.aggregate_online_candidates(candidates)


def pick_best_online_match(meta: EpubMetadata, candidates: Iterable[OnlineCandidate]) -> RankedOnlineMatch | None:
    return domain_naming_mod.pick_best_online_match(meta, candidates)


def build_online_record(meta: EpubMetadata, best: RankedOnlineMatch) -> BookRecord:
    record = domain_naming_mod.build_online_record(meta, best, extract_authors=extract_authors)
    if not record.identifiers:
        record.identifiers = extract_isbns(meta.identifiers)
    return record


def fetch_online_candidates(
    meta: EpubMetadata,
    providers: list[str],
    timeout: float,
    *,
    online_mode: str = DEFAULT_ONLINE_MODE,
    emit_stage: Callable[[str, str], None] | None = None,
    query_label: str = "",
) -> list[OnlineCandidate]:
    provider_labels = {
        "google": "Google",
        "openlibrary": "OpenLibrary",
        "crossref": "Crossref",
        "hathitrust": "HathiTrust",
        "lubimyczytac": "Lubimyczytac",
    }

    provider_functions = {
        "google": google_books_candidates,
        "openlibrary": open_library_candidates,
        "crossref": crossref_candidates,
        "hathitrust": hathitrust_candidates,
        "lubimyczytac": lubimyczytac_candidates,
    }
    def guarded_provider(provider_meta: EpubMetadata, provider_timeout: float, provider: str) -> list[OnlineCandidate]:
        func = provider_functions[provider]
        try:
            return func(provider_meta, provider_timeout)
        except Exception as exc:
            provider_meta.errors.append(f"{provider}: {exc}")
            if emit_stage is not None:
                label = provider_labels.get(provider, provider.title())
                emit_stage("sprawdzenie-online", f"{query_label} | {label}: blad {exc}")
            return []

    with ONLINE_ENRICH_SEMAPHORE:
        return providers_online_mod.fetch_online_candidates(
            meta,
            providers,
            timeout,
            online_mode=online_mode,
            emit_provider_progress=(
                (
                    lambda provider, outcome: emit_stage(
                        "sprawdzenie-online",
                        f"{query_label} | {provider_labels.get(provider, provider.title())}: {outcome}",
                    )
                )
                if emit_stage is not None
                else None
            ),
            provider_functions={
                provider: (lambda provider_meta, provider_timeout, provider_name=provider: guarded_provider(provider_meta, provider_timeout, provider_name))
                for provider in provider_functions
            },
        )


def build_online_query_variants(meta: EpubMetadata, record: BookRecord | LocalPrototype) -> list[EpubMetadata]:
    return infer_engine_mod.build_online_query_variants(
        meta,
        record,
        clean=clean,
        clean_author_segment=clean_author_segment,
        to_last_first=to_last_first,
        normalize_match_text=normalize_match_text,
        author_match_keys=author_match_keys,
        looks_like_author_segment=looks_like_author_segment,
        sanitize_title_for_online_query=sanitize_title_for_online_query,
    )


def online_candidate_matches_expected_author(record: BookRecord, meta: EpubMetadata, candidate: OnlineCandidate) -> bool:
    expected_keys = expected_author_match_keys(record, meta)
    candidate_keys = author_match_keys(candidate.authors)
    if not expected_keys:
        return True
    if expected_keys.intersection(candidate_keys):
        return True

    best_similarity = 0.0
    for expected in expected_keys:
        for current in candidate_keys:
            best_similarity = max(best_similarity, difflib.SequenceMatcher(None, expected, current).ratio())
    return best_similarity >= 0.86


def expected_author_match_keys(record: BookRecord, meta: EpubMetadata) -> set[str]:
    expected_authors: list[str] = []
    if record.author and record.author != "Nieznany Autor":
        expected_authors.extend(split_authors(record.author))
    for creator in meta.creators:
        expected_authors.extend(split_authors(creator))
    return author_match_keys(expected_authors)


def online_candidate_matches_expected_title(record: BookRecord, meta: EpubMetadata, candidate: OnlineCandidate) -> bool:
    title_probes = [
        record.title,
        strip_leading_title_index(record.title),
        meta.title,
        strip_leading_title_index(meta.title),
        sanitize_title(meta.core, record.series, record.volume),
        strip_leading_title_index(sanitize_title(meta.core, record.series, record.volume)),
    ]
    title_probes = [clean(probe) for probe in title_probes if clean(probe)]
    if not title_probes:
        return True

    candidate_titles = [clean(candidate.title)] if clean(candidate.title) else []
    candidate_titles.extend(
        clean(parsed.title_override)
        for parsed in collect_online_candidate_candidates(candidate)
        if parsed.title_override and clean(parsed.title_override)
    )

    for probe in title_probes:
        probe_key = normalize_match_text(probe)
        for candidate_title in candidate_titles:
            if not candidate_title:
                continue
            candidate_key = normalize_match_text(candidate_title)
            if probe_key and candidate_key == probe_key:
                return True
            if similarity_score(probe, candidate_title) >= 0.88:
                return True
    return False


def online_candidate_supports_record_context(record: BookRecord, meta: EpubMetadata, candidate: OnlineCandidate) -> bool:
    return infer_flow_mod.online_candidate_supports_record_context(
        record,
        meta,
        candidate,
        expected_author_match_keys_fn=expected_author_match_keys,
        online_candidate_matches_expected_author_fn=online_candidate_matches_expected_author,
        online_candidate_matches_expected_title_fn=online_candidate_matches_expected_title,
    )


def verify_record_against_online(record: BookRecord, meta: EpubMetadata, candidates: list[OnlineCandidate]) -> OnlineVerification:
    return infer_flow_mod.verify_record_against_online(
        record,
        meta,
        candidates,
        is_online_candidate=is_online_candidate,
        author_match_keys=author_match_keys,
        split_authors=split_authors,
        normalize_match_text=normalize_match_text,
        similarity_score=similarity_score,
        collect_online_candidate_candidates=collect_online_candidate_candidates,
        online_candidate_supports_record_context_fn=online_candidate_supports_record_context,
        verification_type=OnlineVerification,
    )


def clear_strong_lubimyczytac_review(record: BookRecord, verification: OnlineVerification) -> None:
    infer_flow_mod.clear_strong_lubimyczytac_review(record, verification)


def validate_record_components_with_online(
    record: BookRecord,
    meta: EpubMetadata,
    local_candidates: list[Candidate],
    online_candidates: list[OnlineCandidate],
    verification: OnlineVerification,
) -> OnlineVerification:
    return infer_flow_mod.validate_record_components_with_online(
        record,
        meta,
        local_candidates,
        online_candidates,
        verification,
        collect_online_role_evidence=collect_online_role_evidence,
        best_matching_online_text=best_matching_online_text,
        is_online_candidate=is_online_candidate,
        online_candidate_supports_record_context_fn=online_candidate_supports_record_context,
        series_candidate_priority=series_candidate_priority,
        clean_series=clean_series,
        is_strong_online_candidate=is_strong_online_candidate,
        strip_leading_title_index=strip_leading_title_index,
        sanitize_title=sanitize_title,
        clean=clean,
        clean_author_segment=clean_author_segment,
        split_authors=split_authors,
        similarity_score=similarity_score,
        normalize_match_text=normalize_match_text,
        verification_type=OnlineVerification,
        extract_trailing_author_from_core=extract_trailing_author_from_core,
    )


def parse_existing_filename(stem: str) -> tuple[str, str, tuple[int, str] | None, str, str] | None:
    return domain_naming_mod.parse_existing_filename(stem)



def add_candidate(
    candidates: list[Candidate],
    series: str,
    volume: tuple[int, str] | None,
    score: int,
    source: str,
    title_override: str | None = None,
) -> None:
    infer_engine_mod.add_candidate(
        candidates,
        series,
        volume,
        score,
        source,
        title_override,
        clean_series=clean_series,
        is_publisher_like=is_publisher_like,
        clean=clean,
    )


def series_candidate_priority(candidate: Candidate) -> tuple[int, int, int]:
    return infer_engine_mod.series_candidate_priority(candidate, series_source_priorities=SERIES_SOURCE_PRIORITIES)


def choose_series_candidate(candidates: list[Candidate]) -> Candidate | None:
    return infer_engine_mod.choose_series_candidate(candidates, series_source_priorities=SERIES_SOURCE_PRIORITIES)


def choose_title_candidate(candidates: list[Candidate]) -> Candidate | None:
    return infer_engine_mod.choose_title_candidate(candidates)


def source_needs_online_verification(source: str) -> bool:
    return infer_engine_mod.source_needs_online_verification(source)


def existing_format_needs_online_verification(record: BookRecord) -> bool:
    return infer_engine_mod.existing_format_needs_online_verification(record)


def extract_trailing_author_from_core(text: str) -> str:
    return infer_engine_mod.extract_trailing_author_from_core(
        text,
        strip_source_artifacts=strip_source_artifacts,
        clean_author_segment=clean_author_segment,
        looks_like_author_segment=looks_like_author_segment,
    )


def strip_leading_title_index(title: str) -> str:
    return infer_engine_mod.strip_leading_title_index(title, clean=clean, leading_index_title_re=LEADING_INDEX_TITLE_RE)


def sanitize_title_for_online_query(title: str, author: str, series: str, volume: tuple[int, str] | None) -> str:
    return infer_engine_mod.sanitize_title_for_online_query(
        title,
        author,
        series,
        volume,
        strip_source_artifacts=strip_source_artifacts,
        query_noise_paren_re=QUERY_NOISE_PAREN_RE,
        looks_like_author_segment=looks_like_author_segment,
        sanitize_title=sanitize_title,
        normalize_match_text=normalize_match_text,
        strip_author_from_title=strip_author_from_title,
        strip_leading_title_index=strip_leading_title_index,
        clean=clean,
    )


def lubimyczytac_author_query_terms(creators: Iterable[str]) -> list[str]:
    return infer_engine_mod.lubimyczytac_author_query_terms(
        creators,
        clean_author_segment=clean_author_segment,
        to_last_first=to_last_first,
        normalize_match_text=normalize_match_text,
        clean=clean,
    )


def normalize_lubimyczytac_query_title(title: str) -> str:
    return infer_engine_mod.normalize_lubimyczytac_query_title(
        title,
        sanitize_title_for_online_query=sanitize_title_for_online_query,
        clean=clean,
    )


def build_lubimyczytac_query_terms(meta: EpubMetadata) -> list[str]:
    return infer_engine_mod.build_lubimyczytac_query_terms(
        meta,
        clean=clean,
        normalize_lubimyczytac_query_title=normalize_lubimyczytac_query_title,
        lubimyczytac_author_query_terms=lubimyczytac_author_query_terms,
        normalize_match_text=normalize_match_text,
    )


def split_trailing_series_book(title: str) -> tuple[str, str, tuple[int, str] | None] | None:
    return infer_engine_mod.split_trailing_series_book(
        title,
        trailing_book_index_re=TRAILING_BOOK_INDEX_RE,
        parse_volume_parts=parse_volume_parts,
        clean=clean,
        clean_series=clean_series,
        is_publisher_like=is_publisher_like,
    )


def split_square_bracket_series_book(title: str) -> tuple[str, str, tuple[int, str] | None] | None:
    return infer_engine_mod.split_square_bracket_series_book(
        title,
        clean=clean,
        clean_series=clean_series,
        parse_volume_parts=parse_volume_parts,
    )


def collect_title_candidates(title: str, candidates: list[Candidate]) -> None:
    infer_engine_mod.collect_title_candidates(
        title,
        candidates,
        clean=clean,
        parse_volume_parts=parse_volume_parts,
        add_candidate=add_candidate,
        split_trailing_series_book=split_trailing_series_book,
        split_square_bracket_series_book=split_square_bracket_series_book,
        title_dotted_series_book_re=TITLE_DOTTED_SERIES_BOOK_RE,
        title_double_colon_book_re=TITLE_DOUBLE_COLON_BOOK_RE,
        title_with_series_re=TITLE_WITH_SERIES_RE,
        paren_series_re=PAREN_SERIES_RE,
        series_only_paren_index_re=SERIES_ONLY_PAREN_INDEX_RE,
        title_colon_series_index_re=TITLE_COLON_SERIES_INDEX_RE,
        indexed_title_re=INDEXED_TITLE_RE,
        index_only_re=INDEX_ONLY_RE,
        leading_index_dotted_title_re=LEADING_INDEX_DOTTED_TITLE_RE,
        box_set_re=BOX_SET_RE,
    )



def collect_core_candidates(core: str, candidates: list[Candidate]) -> None:
    infer_engine_mod.collect_core_candidates(
        core,
        candidates,
        clean=clean,
        add_candidate=add_candidate,
        parse_volume_parts=parse_volume_parts,
        looks_like_author_segment=looks_like_author_segment,
        box_set_re=BOX_SET_RE,
        paren_series_re=PAREN_SERIES_RE,
        core_title_author_re=CORE_TITLE_AUTHOR_RE,
        core_comma_re=CORE_COMMA_RE,
        core_joined_re=CORE_JOINED_RE,
        core_spaced_re=CORE_SPACED_RE,
        core_index_only_re=CORE_INDEX_ONLY_RE,
    )



def collect_segment_candidates(segments: list[str], candidates: list[Candidate]) -> None:
    infer_engine_mod.collect_segment_candidates(
        segments,
        candidates,
        strip_source_artifacts=strip_source_artifacts,
        is_source_artifact=is_source_artifact,
        is_publisher_like=is_publisher_like,
        segment_hash_re=SEGMENT_HASH_RE,
        segment_comma_re=SEGMENT_COMMA_RE,
        segment_year_re=SEGMENT_YEAR_RE,
        add_candidate=add_candidate,
        parse_volume_parts=parse_volume_parts,
    )



def sanitize_title(title: str, series: str, volume: tuple[int, str] | None) -> str:
    return infer_engine_mod.sanitize_title(
        title,
        series,
        volume,
        strip_source_artifacts=strip_source_artifacts,
        genre_tail_re=GENRE_TAIL_RE,
        trailing_series_suffix_re=TRAILING_SERIES_SUFFIX_RE,
        volume_match_pattern=volume_match_pattern,
        is_series_volume_only_title=is_series_volume_only_title,
        clean=clean,
    )


def is_series_volume_only_title(title: str, series: str, volume: tuple[int, str] | None) -> bool:
    return infer_engine_mod.is_series_volume_only_title(
        title,
        series,
        volume,
        clean=clean,
        clean_series=clean_series,
        volume_match_pattern=volume_match_pattern,
        series_words=SERIES_WORDS,
    )



def strip_author_from_title(title: str, author: str) -> str:
    return infer_engine_mod.strip_author_from_title(
        title,
        author,
        clean=clean,
        looks_like_author_segment=looks_like_author_segment,
    )



def read_book_metadata(path: Path) -> EpubMetadata:
    return runtime_metadata_mod.read_book_metadata(
        path,
        metadata_type=EpubMetadata,
        strip_source_artifacts=strip_source_artifacts,
        clean=clean,
        clean_series=clean_series,
        parse_volume_parts=parse_volume_parts,
        epub_module=None,
    )



def extract_authors(creators: list[str], segment_author: str) -> str:
    return infer_engine_mod.extract_authors(
        creators,
        segment_author,
        resolve_author_segment=resolve_author_segment,
        split_authors=split_authors,
        canonicalize_authors=canonicalize_authors,
        to_last_first=to_last_first,
    )



def online_query(url: str, timeout: float) -> dict | None:
    return runtime_online_mod.online_query(url, timeout, online_fetch=online_fetch)


def online_text_query(url: str, timeout: float) -> str | None:
    return runtime_online_mod.online_text_query(url, timeout, online_fetch=online_fetch)



def google_books_candidates(meta: EpubMetadata, timeout: float) -> list[OnlineCandidate]:
    return providers_online_mod.google_books_candidates(
        meta,
        timeout,
        clean=clean,
        extract_isbns=extract_isbns,
        online_query=online_query,
        build_online_candidates=build_online_candidates,
    )



def open_library_candidates(meta: EpubMetadata, timeout: float) -> list[OnlineCandidate]:
    return providers_online_mod.open_library_candidates(
        meta,
        timeout,
        clean=clean,
        extract_isbns=extract_isbns,
        online_query=online_query,
        build_online_candidates=build_online_candidates,
    )


def crossref_candidates(meta: EpubMetadata, timeout: float) -> list[OnlineCandidate]:
    return providers_online_mod.crossref_candidates(
        meta,
        timeout,
        clean=clean,
        extract_isbns=extract_isbns,
        online_query=online_query,
        build_online_candidates=build_online_candidates,
    )


def hathitrust_candidates(meta: EpubMetadata, timeout: float) -> list[OnlineCandidate]:
    return providers_online_mod.hathitrust_candidates(
        meta,
        timeout,
        clean=clean,
        extract_isbns=extract_isbns,
        online_query=online_query,
        build_online_candidates=build_online_candidates,
    )


def lubimyczytac_candidates(meta: EpubMetadata, timeout: float) -> list[OnlineCandidate]:
    return providers_online_mod.lubimyczytac_candidates(
        meta,
        timeout,
        extract_isbns=extract_isbns,
        build_lubimyczytac_query_terms=build_lubimyczytac_query_terms,
        normalize_match_text=normalize_match_text,
        online_text_query=online_text_query,
        parser_factory=LubimyczytacSearchParser,
        enrich_result=enrich_lubimyczytac_result,
        rank_online_candidate=rank_online_candidate,
        clean=clean,
        clean_series=clean_series,
        provider_score_adjustments=PROVIDER_SCORE_ADJUSTMENTS,
        infer_book_genre=infer_book_genre,
        candidate_type=OnlineCandidate,
    )



def enrich_from_online(meta: EpubMetadata, providers: list[str], timeout: float) -> BookRecord | None:
    all_candidates = fetch_online_candidates(meta, providers, timeout)
    best = pick_best_online_match(meta, all_candidates)
    if best is None:
        return None
    return build_online_record(meta, best)



def finalize_record_quality(record: BookRecord, meta: EpubMetadata, base_confidence: int, title_from_core: bool) -> BookRecord:
    return domain_naming_mod.finalize_record_quality(
        record,
        meta,
        base_confidence,
        title_from_core,
        hex_noise_re=HEX_NOISE_RE,
        anna_archive_re=ANNA_ARCHIVE_RE,
    )



def infer_record(
    meta: EpubMetadata,
    use_online: bool,
    providers: list[str],
    timeout: float,
    *,
    online_mode: str = DEFAULT_ONLINE_MODE,
    emit_stage: Callable[[str, str], None] | None = None,
    emit_trace: Callable[[str], None] | None = None,
) -> BookRecord:
    return infer_flow_mod.infer_record(
        meta,
        use_online,
        providers,
        timeout,
        online_mode=online_mode,
        parse_existing_filename=parse_existing_filename,
        book_record_type=BookRecord,
        extract_isbns=extract_isbns,
        infer_book_genre=infer_book_genre,
        existing_format_needs_online_verification=existing_format_needs_online_verification,
        finalize_record_quality=finalize_record_quality,
        add_candidate=add_candidate,
        clean_author_segment=clean_author_segment,
        looks_like_author_segment=looks_like_author_segment,
        extract_trailing_author_from_core=extract_trailing_author_from_core,
        extract_authors=extract_authors,
        parse_hybrid_local=parse_hybrid_local,
        collect_title_candidates=collect_title_candidates,
        collect_core_candidates=collect_core_candidates,
        collect_segment_candidates=collect_segment_candidates,
        choose_best_local_series_candidate=choose_best_local_series_candidate,
        choose_best_local_title_candidate=choose_best_local_title_candidate,
        choose_series_candidate=choose_series_candidate,
        choose_title_candidate=choose_title_candidate,
        sanitize_title=sanitize_title,
        strip_leading_title_index=strip_leading_title_index,
        strip_author_from_title=strip_author_from_title,
        clean=clean,
        hex_noise_re=HEX_NOISE_RE,
        anna_archive_re=ANNA_ARCHIVE_RE,
        clean_series=clean_series,
        fetch_online_candidates=fetch_online_candidates,
        build_online_query_variants=build_online_query_variants,
        pick_best_online_match=pick_best_online_match,
        build_online_record=build_online_record,
        split_authors=split_authors,
        normalize_match_text=normalize_match_text,
        resolve_author_segment=resolve_author_segment,
        online_candidate_supports_record_context_fn=online_candidate_supports_record_context,
        collect_online_candidate_candidates=collect_online_candidate_candidates,
        source_needs_online_verification=source_needs_online_verification,
        verify_record_against_online_fn=verify_record_against_online,
        validate_record_components_with_online_fn=validate_record_components_with_online,
        clear_strong_lubimyczytac_review_fn=clear_strong_lubimyczytac_review,
        online_candidate_type=OnlineCandidate,
        emit_stage=emit_stage,
        emit_trace=emit_trace,
    )



def make_record_clone(
    record: BookRecord,
    *,
    title: str | None = None,
    notes: list[str] | None = None,
    confidence: int | None = None,
    review_reasons: list[str] | None = None,
    decision_reasons: list[str] | None = None,
    filename_suffix: str | None = None,
    output_folder: Path | None = None,
) -> BookRecord:
    return domain_naming_mod.make_record_clone(
        record,
        title=title,
        notes=notes,
        confidence=confidence,
        review_reasons=review_reasons,
        decision_reasons=decision_reasons,
        filename_suffix=filename_suffix,
        output_folder=output_folder,
    )


def set_output_folder(records: list[BookRecord], folder: Path) -> list[BookRecord]:
    return job_runner_mod.set_output_folder(records, folder)



def dedupe_destinations(records: list[BookRecord], folder: Path) -> list[BookRecord]:
    return job_runner_mod.dedupe_destinations(
        records,
        folder,
        is_supported_book_file=is_supported_book_file,
        make_record_clone=make_record_clone,
    )



def build_moves(
    records: list[BookRecord],
    source_folder: Path,
    target_folder: Path,
    archive_folder: Path | None,
    stamp: str,
) -> list[RenameMove]:
    return fs_build_moves(records, source_folder, target_folder, archive_folder, stamp)



def validate_move_collisions(moves: list[RenameMove]) -> list[str]:
    return fs_validate_move_collisions(moves)



def rollback_moves(moves: list[RenameMove], stage2_done: list[RenameMove]) -> None:
    fs_rollback_moves(moves, stage2_done)



def execute_moves(moves: list[RenameMove]) -> list[str]:
    return fs_execute_moves(moves)



def write_report(
    path: Path,
    rows: list[BookRecord],
    dry_run: bool,
    source_folder: Path,
    target_folder: Path,
    operation: str,
    execution_status: dict[Path, str] | None = None,
) -> None:
    job_runner_mod.write_report(
        path,
        rows,
        dry_run,
        source_folder,
        target_folder,
        operation,
        format_volume=format_volume,
        execution_status=execution_status,
    )



def build_undo_plan(report_path: Path, folder_hint: Path | None = None) -> UndoPlan:
    return fs_build_undo_plan(report_path, folder_hint)



def execute_undo(report_path: Path, folder_hint: Path | None = None) -> int:
    return fs_execute_undo(
        report_path,
        folder_hint,
        log_error=LOGGER.error,
        emit_lines=lambda lines, level=logging.INFO: log_lines(lines, level=level),
    )


def run_job(
    folder: Path,
    *,
    destination_folder: Path | None = None,
    archive_folder: Path | None = None,
    online_mode: str = DEFAULT_ONLINE_MODE,
    apply_changes: bool,
    use_online: bool,
    providers: list[str],
    timeout: float,
    limit: int,
    online_workers: int = DEFAULT_INFER_WORKERS,
    emit_progress: Callable[[str], None] | None = None,
    emit_trace: Callable[[str], None] | None = None,
    skip_previously_processed: bool = False,
) -> tuple[int, list[str]]:
    return job_runner_mod.run_job(
        folder,
        destination_folder=destination_folder,
        archive_folder=archive_folder,
        online_mode=online_mode,
        apply_changes=apply_changes,
        use_online=use_online,
        providers=providers,
        timeout=timeout,
        limit=limit,
        online_workers=online_workers,
        default_infer_workers=DEFAULT_INFER_WORKERS,
        online_http_slots=ONLINE_HTTP_SLOTS,
        is_supported_book_file=is_supported_book_file,
        read_book_metadata=read_book_metadata,
        infer_record=infer_record,
        build_moves=build_moves,
        execute_moves=execute_moves,
        format_volume=format_volume,
        write_report_fn=write_report,
        set_output_folder_fn=set_output_folder,
        dedupe_destinations_fn=dedupe_destinations,
        flush_online_cache_if_needed=flush_online_cache_if_needed,
        emit_progress=emit_progress,
        emit_trace=emit_trace,
        skip_previously_processed=skip_previously_processed,
    )


load_online_cache()
atexit.register(lambda: flush_online_cache_if_needed(force=True))
