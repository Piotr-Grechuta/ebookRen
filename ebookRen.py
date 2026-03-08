import argparse
import csv
import difflib
import html
import json
import logging
import os
import queue
import re
import shutil
import threading
import time
import unicodedata
import sys
import tkinter as tk
import urllib.parse
import urllib.request
import atexit
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Iterable

from ebooklib import epub


SUPPORTED_BOOK_EXTENSIONS = {
    ".epub",
    ".mobi",
    ".azw",
    ".azw3",
    ".pdf",
    ".lit",
    ".fb2",
    ".rtf",
    ".txt",
}

DEVICE_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}

VOLUME_INDEX_PATTERN = r"(?:\d+(?:\.\d+)?|[IVXLCDM]+)"
SERIES_WORDS = r"(?:Book|Tom|Volume|Vol\.?|#|Part|Cykl|Czesc|Część|Ksiega|Księga)"
TITLE_WITH_SERIES_RE = re.compile(
    rf"^(.*?)\s*:\s*(.+?)\s*\(\s*{SERIES_WORDS}\s*({VOLUME_INDEX_PATTERN})\s*\)\s*$",
    re.IGNORECASE,
)
PAREN_SERIES_RE = re.compile(
    rf"^(.*?)\s*\(([^()]+?)\s*{SERIES_WORDS}\s*({VOLUME_INDEX_PATTERN})\)\s*$",
    re.IGNORECASE,
)
TITLE_COLON_SERIES_INDEX_RE = re.compile(
    rf"^(.*?)\s*:\s*(.+?)\s*,\s*{SERIES_WORDS}\s*({VOLUME_INDEX_PATTERN})\s*$",
    re.IGNORECASE,
)
INDEXED_TITLE_RE = re.compile(rf"^(.+?)\s+({VOLUME_INDEX_PATTERN})\s*[:\-]\s*(.+)$", re.IGNORECASE)
INDEX_ONLY_RE = re.compile(rf"^(.+?)\s*[-:]\s*({VOLUME_INDEX_PATTERN})$", re.IGNORECASE)
CORE_COMMA_RE = re.compile(rf"^(.+?)\s+({VOLUME_INDEX_PATTERN})\s*,\s*(.+)$", re.IGNORECASE)
CORE_JOINED_RE = re.compile(rf"^(.+?)\s+({VOLUME_INDEX_PATTERN})\s*[-_:]\s*(.+)$", re.IGNORECASE)
CORE_SPACED_RE = re.compile(rf"^(.+?)\s+({VOLUME_INDEX_PATTERN})\s+(.+)$", re.IGNORECASE)
CORE_INDEX_ONLY_RE = re.compile(rf"^(.+?)\s+({VOLUME_INDEX_PATTERN})$", re.IGNORECASE)
SEGMENT_HASH_RE = re.compile(rf"(.+?)\s*#\s*({VOLUME_INDEX_PATTERN})\b", re.IGNORECASE)
SEGMENT_COMMA_RE = re.compile(rf"^([^,]{{3,}}?),\s*({VOLUME_INDEX_PATTERN})\s*(?:,|$)", re.IGNORECASE)
SEGMENT_YEAR_RE = re.compile(rf"^(.+?)\s+({VOLUME_INDEX_PATTERN})\s*,\s*\d{{4}}\b", re.IGNORECASE)
TRAILING_SERIES_SUFFIX_RE = re.compile(
    rf"\s*\(([^()]*(?:{SERIES_WORDS})\s*{VOLUME_INDEX_PATTERN}[^()]*)\)\s*$",
    re.IGNORECASE,
)
ANNA_ARCHIVE_RE = re.compile(r"\bAnna.?s Archive\b", re.IGNORECASE)
HEX_NOISE_RE = re.compile(r"\b[0-9a-f]{12,}\b", re.IGNORECASE)
ISBN_RE = re.compile(r"(97[89][0-9]{10}|[0-9]{9}[0-9Xx])")
ONLINE_CACHE: dict[str, object | None] = {}
ONLINE_CACHE_LOCK = threading.Lock()
ONLINE_CACHE_INFLIGHT: dict[str, threading.Event] = {}
ONLINE_ERROR_CACHE: dict[str, tuple[float, str]] = {}
ONLINE_AMBIGUITY_MARGIN = 25
ONLINE_HTTP_SLOTS = 4
ONLINE_ENRICH_SEMAPHORE = threading.BoundedSemaphore(ONLINE_HTTP_SLOTS)
APP_NAME = "ebookRen"
APP_VERSION = "15.0"
GUI_FOOTER_TEXT = "v15. 2026. Piotr Grechuta"
DEFAULT_SOURCE_FOLDER = str(Path.cwd())
DEFAULT_PROVIDERS = "google,openlibrary,crossref,hathitrust,lubimyczytac"
DEFAULT_HTTP_TIMEOUT = 8.0

ONLINE_CACHE_PATH = Path(__file__).with_name("online_cache.json")
ONLINE_CACHE_DIRTY = False
ONLINE_CACHE_PENDING_WRITES = 0
ONLINE_CACHE_LAST_SAVE = 0.0
ONLINE_CACHE_SAVE_EVERY = 10
ONLINE_CACHE_SAVE_INTERVAL = 5.0
DEFAULT_INFER_WORKERS = 2
ONLINE_ERROR_CACHE_TTL = 60.0
BLOCKING_REVIEW_REASONS = {
    "online-niejednoznaczne",
    "online-best-effort",
    "nieznany-autor",
    "brak-tytulu",
    "fallback",
    "szum-w-tytule",
    "artefakt-zrodla",
}

LOGGER = logging.getLogger(APP_NAME)


@dataclass(frozen=True)
class Candidate:
    score: int
    series: str
    volume: tuple[int, str] | None
    title_override: str | None
    source: str


@dataclass
class EpubMetadata:
    path: Path
    stem: str
    segments: list[str]
    core: str
    title: str = ""
    creators: list[str] = field(default_factory=list)
    identifiers: list[str] = field(default_factory=list)
    meta_series: str = ""
    meta_volume: tuple[int, str] | None = None
    errors: list[str] = field(default_factory=list)


@dataclass
class BookRecord:
    path: Path
    author: str
    series: str
    volume: tuple[int, str] | None
    title: str
    source: str
    identifiers: list[str]
    notes: list[str]
    confidence: int = 0
    review_reasons: list[str] = field(default_factory=list)
    decision_reasons: list[str] = field(default_factory=list)
    filename_suffix: str = ""
    output_folder: Path | None = None
    online_checked: bool = False
    online_applied: bool = False

    @property
    def needs_review(self) -> bool:
        return self.confidence < 65 or any(reason in BLOCKING_REVIEW_REASONS for reason in self.review_reasons)

    @property
    def filename(self) -> str:
        return self._filename_for_folder(self.output_folder or self.path.parent)

    def _filename_for_folder(self, folder: Path) -> str:
        author = sanitize_component(self.author)
        series = sanitize_component(self.series or "Standalone")
        volume = format_volume(self.volume)
        title_for_path = self.title
        if self.filename_suffix:
            title_for_path = clean(f"{title_for_path} {self.filename_suffix}")
        title = sanitize_component(title_for_path)
        title = trim_title_for_path(folder, author, series, volume, title)
        return f"{author} - {series} - {volume} - {sanitize_component(title)}{self.path.suffix.lower()}"


@dataclass
class RenameMove:
    source: Path
    temp: Path | None
    destination: Path
    record: BookRecord | None = None
    operation: str = "rename"


@dataclass
class UndoPlan:
    folder: Path
    moves: list[RenameMove]
    total_rows: int


@dataclass
class InferenceResult:
    record: BookRecord
    base_confidence: int
    title_from_core: bool


@dataclass(frozen=True)
class RankedOnlineMatch:
    providers: list[str]
    sources: list[str]
    title: str
    authors: list[str]
    identifiers: list[str]
    score: int
    reason: str


@dataclass(frozen=True)
class OnlineCandidate:
    provider: str
    source: str
    title: str
    authors: list[str]
    identifiers: list[str]
    score: int
    reason: str


SERIES_SOURCE_PRIORITIES = {
    "opf": 140,
    "title:series-book": 136,
    "title:paren-series": 132,
    "title:colon-series-index": 130,
    "core:paren-series": 126,
    "segment:hash": 122,
    "segment:year": 118,
    "segment:comma": 116,
    "core:comma": 110,
    "core:joined": 106,
    "title:indexed": 104,
    "title:index-only": 100,
    "core:spaced": 92,
    "core:index-only": 88,
}

PROVIDER_SCORE_ADJUSTMENTS = {
    "google-books": 18,
    "open-library": 14,
    "hathitrust": 6,
    "crossref": -22,
    "lubimyczytac": -34,
}



def clean(text: str | None) -> str:
    if not text:
        return ""
    text = text.replace("_", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text.strip(" .-")



def clean_series(text: str | None) -> str:
    return clean(text).strip(" ,")


class MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


def configure_logging() -> None:
    if LOGGER.handlers:
        return
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.addFilter(MaxLevelFilter(logging.WARNING - 1))
    stdout_handler.setFormatter(logging.Formatter("%(message)s"))

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(logging.Formatter("%(message)s"))

    LOGGER.addHandler(stdout_handler)
    LOGGER.addHandler(stderr_handler)


def log_lines(lines: list[str], *, level: int = logging.INFO) -> None:
    for line in lines:
        LOGGER.log(level, line)


def roman_to_int(text: str) -> int | None:
    values = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    token = clean(text).upper()
    if not token or not re.fullmatch(r"[IVXLCDM]+", token):
        return None
    total = 0
    previous = 0
    for char in reversed(token):
        value = values[char]
        if value < previous:
            total -= value
        else:
            total += value
            previous = value
    # Validate by round-tripping the common canonical range.
    if total <= 0 or total > 3999:
        return None
    return total


def strip_html_tags(text: str | None) -> str:
    if not text:
        return ""
    return clean(html.unescape(re.sub(r"<[^>]+>", " ", text)))


def load_online_cache() -> None:
    if not ONLINE_CACHE_PATH.exists():
        return
    try:
        payload = json.loads(ONLINE_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(payload, dict):
        return
    with ONLINE_CACHE_LOCK:
        ONLINE_CACHE.clear()
        for key, value in payload.items():
            if isinstance(key, str):
                ONLINE_CACHE[key] = value if isinstance(value, (dict, str)) or value is None else None
        global ONLINE_CACHE_DIRTY, ONLINE_CACHE_PENDING_WRITES, ONLINE_CACHE_LAST_SAVE
        ONLINE_CACHE_DIRTY = False
        ONLINE_CACHE_PENDING_WRITES = 0
        ONLINE_CACHE_LAST_SAVE = time.perf_counter()


def save_online_cache() -> None:
    with ONLINE_CACHE_LOCK:
        global ONLINE_CACHE_DIRTY, ONLINE_CACHE_PENDING_WRITES, ONLINE_CACHE_LAST_SAVE
        snapshot = dict(ONLINE_CACHE)
        pending_writes = ONLINE_CACHE_PENDING_WRITES
    ONLINE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = ONLINE_CACHE_PATH.with_suffix(".tmp")
    temp_path.write_text(json.dumps(snapshot, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(temp_path, ONLINE_CACHE_PATH)
    with ONLINE_CACHE_LOCK:
        ONLINE_CACHE_PENDING_WRITES = max(0, ONLINE_CACHE_PENDING_WRITES - pending_writes)
        ONLINE_CACHE_DIRTY = ONLINE_CACHE_PENDING_WRITES > 0
        ONLINE_CACHE_LAST_SAVE = time.perf_counter()


def mark_online_cache_dirty() -> None:
    with ONLINE_CACHE_LOCK:
        global ONLINE_CACHE_DIRTY, ONLINE_CACHE_PENDING_WRITES
        ONLINE_CACHE_DIRTY = True
        ONLINE_CACHE_PENDING_WRITES += 1


def flush_online_cache_if_needed(force: bool = False) -> None:
    with ONLINE_CACHE_LOCK:
        pending_writes = ONLINE_CACHE_PENDING_WRITES
        last_save = ONLINE_CACHE_LAST_SAVE
        dirty = ONLINE_CACHE_DIRTY
    if not dirty:
        return
    if not force and pending_writes < ONLINE_CACHE_SAVE_EVERY and (time.perf_counter() - last_save) < ONLINE_CACHE_SAVE_INTERVAL:
        return
    save_online_cache()


def get_cached_online_error(cache_key: str) -> str | None:
    with ONLINE_CACHE_LOCK:
        item = ONLINE_ERROR_CACHE.get(cache_key)
        if item is None:
            return None
        expires_at, message = item
        if expires_at <= time.time():
            ONLINE_ERROR_CACHE.pop(cache_key, None)
            return None
        return message


def cache_online_error(cache_key: str, message: str) -> None:
    with ONLINE_CACHE_LOCK:
        ONLINE_ERROR_CACHE[cache_key] = (time.time() + ONLINE_ERROR_CACHE_TTL, message)


def online_cache_key(kind: str, url: str) -> str:
    return f"{kind}:{url}"


def online_fetch(url: str, timeout: float, *, kind: str) -> object | None:
    cache_key = online_cache_key(kind, url)
    cached_error = get_cached_online_error(cache_key)
    if cached_error is not None:
        return None
    with ONLINE_CACHE_LOCK:
        if cache_key in ONLINE_CACHE:
            return ONLINE_CACHE[cache_key]
        in_flight = ONLINE_CACHE_INFLIGHT.get(cache_key)
        if in_flight is None:
            in_flight = threading.Event()
            ONLINE_CACHE_INFLIGHT[cache_key] = in_flight
            is_owner = True
        else:
            is_owner = False

    if not is_owner:
        in_flight.wait()
        with ONLINE_CACHE_LOCK:
            return ONLINE_CACHE.get(cache_key)

    try:
        request = urllib.request.Request(
            url,
            headers={"User-Agent": f"{APP_NAME}/{APP_VERSION}"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            raw = response.read().decode(charset, errors="ignore")
        payload: object
        if kind == "json":
            payload = json.loads(raw)
        else:
            payload = raw
        with ONLINE_CACHE_LOCK:
            ONLINE_CACHE[cache_key] = payload
        mark_online_cache_dirty()
        flush_online_cache_if_needed()
        return payload
    except Exception as exc:
        cache_online_error(cache_key, str(exc))
        return None
    finally:
        with ONLINE_CACHE_LOCK:
            event = ONLINE_CACHE_INFLIGHT.pop(cache_key, None)
            if event is not None:
                event.set()


class LubimyczytacSearchParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.results: list[tuple[str, list[str]]] = []
        self._capture_title = False
        self._capture_author_block = False
        self._capture_author_name = False
        self._title_parts: list[str] = []
        self._author_parts: list[str] = []
        self._authors: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        class_name = attr_map.get("class") or ""
        if tag == "a" and "authorAllBooks__singleTextTitle" in class_name:
            self._flush_current()
            self._capture_title = True
            self._title_parts = []
            self._authors = []
            self._author_parts = []
            return
        if tag == "div" and "authorAllBooks__singleTextAuthor" in class_name:
            self._capture_author_block = True
            self._author_parts = []
            return
        if self._capture_author_block and tag == "a":
            self._capture_author_name = True
            self._author_parts = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._capture_title:
            self._capture_title = False
            return
        if tag == "a" and self._capture_author_name:
            author = clean("".join(self._author_parts))
            if author:
                self._authors.append(author)
            self._capture_author_name = False
            self._author_parts = []
            return
        if tag == "div" and self._capture_author_block:
            self._capture_author_block = False
            self._flush_current()

    def handle_data(self, data: str) -> None:
        if self._capture_title:
            self._title_parts.append(data)
        elif self._capture_author_name:
            self._author_parts.append(data)

    def _flush_current(self) -> None:
        title = clean("".join(self._title_parts))
        if title:
            authors = [author for author in self._authors if author]
            self.results.append((title, authors))
        self._title_parts = []
        self._authors = []
        self._author_parts = []


def fold_text(text: str | None) -> str:
    normalized = unicodedata.normalize("NFKD", clean(text))
    folded = "".join(char for char in normalized if not unicodedata.combining(char))
    return (
        folded.replace("Ł", "L")
        .replace("ł", "l")
        .replace("Ø", "O")
        .replace("ø", "o")
        .replace("Đ", "D")
        .replace("đ", "d")
    )


def normalize_match_text(text: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", fold_text(text).lower()).strip()


def similarity_score(left: str | None, right: str | None) -> float:
    left_norm = normalize_match_text(left)
    right_norm = normalize_match_text(right)
    if not left_norm or not right_norm:
        return 0.0
    return difflib.SequenceMatcher(None, left_norm, right_norm).ratio()



def author_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", fold_text(text).lower())



def parse_volume_parts(text: str | None) -> tuple[int, str] | None:
    if text is None:
        return None
    token = clean(str(text))
    if not token:
        return None
    match = re.search(r"(\d+)(?:\.(\d+))?", token)
    if match:
        major = int(match.group(1))
        minor = (match.group(2) or "00").zfill(2)
        return major, minor
    roman_value = roman_to_int(token)
    if roman_value is not None:
        return roman_value, "00"
    return None



def format_volume(volume: tuple[int, str] | None) -> str:
    if volume is None:
        return "Tom 00.00"
    major, minor = volume
    minor_text = str(minor).zfill(2)
    return f"Tom {major:02d}.{minor_text}"


def volume_match_pattern(volume: tuple[int, str] | None) -> str:
    if volume is None:
        return r"\d+(?:\.\d+)?"
    major, minor = volume
    minor_text = str(minor).zfill(2)
    minor_int = str(int(minor_text))
    decimal_variants = {minor_text, minor_int}
    decimal_variants_text = "|".join(re.escape(item) for item in sorted(decimal_variants))
    return rf"0*{major}(?:\.(?:{decimal_variants_text})|\.0+)?"


def sanitize_component(text: str) -> str:
    text = clean(text)
    text = re.sub(r'[<>:"/\\|?*]', "-", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    if not text:
        text = "Brak"
    if text.upper() in DEVICE_NAMES:
        text = f"_{text}"
    return text



def trim_title_for_path(folder: Path, author: str, series: str, volume: str, title: str) -> str:
    filename = f"{author} - {series} - {volume} - {title}"
    budget = 230 - len(str(folder))
    if budget < 80:
        budget = 80
    if len(filename) <= budget:
        return title
    overflow = len(filename) - budget
    trimmed = title[:-overflow].rstrip(" .-")
    return trimmed or "Bez tytulu"



def split_authors(text: str) -> list[str]:
    text = clean(text)
    if not text:
        return []
    text = re.sub(r"\[.*?\]", "", text)
    text = text.replace(";", " & ")
    text = re.sub(r"\s+(?:and|i)\s+", " & ", text, flags=re.IGNORECASE)
    return [clean(part) for part in re.split(r"\s*&\s*", text) if clean(part)]


def canonicalize_authors(authors: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for author in authors:
        normalized = to_last_first(author)
        key = author_key(normalized)
        if normalized and key and key not in seen:
            seen.add(key)
            result.append(normalized)
    result.sort(key=lambda item: author_key(item))
    return result


def to_last_first(name: str) -> str:
    name = clean(re.sub(r"\[.*?\]", "", name))
    if not name:
        return ""
    if "," in name:
        parts = [part.strip() for part in name.split(",") if part.strip()]
        if len(parts) == 2:
            return f"{parts[0]} {parts[1]}"
        if len(parts) > 2:
            return name
    words = name.split()
    if len(words) >= 2:
        surname_particles = {
            "al",
            "bin",
            "da",
            "de",
            "del",
            "della",
            "der",
            "di",
            "du",
            "ibn",
            "la",
            "le",
            "san",
            "st",
            "st.",
            "van",
            "von",
        }
        surname_start = len(words) - 1
        while surname_start > 0 and words[surname_start - 1].lower().rstrip(".") in surname_particles:
            surname_start -= 1
        surname = " ".join(words[surname_start:])
        given = " ".join(words[:surname_start])
        if surname and given:
            return f"{surname} {given}".strip()
    return name



def extract_isbns(values: Iterable[str]) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for value in values:
        for match in ISBN_RE.findall(value or ""):
            normalized = match.upper()
            if normalized not in seen:
                seen.add(normalized)
                found.append(normalized)
    return found


def is_supported_book_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in SUPPORTED_BOOK_EXTENSIONS


def author_match_keys(values: Iterable[str]) -> set[str]:
    keys: set[str] = set()
    for value in values:
        cleaned = clean(value)
        if not cleaned:
            continue
        for variant in (cleaned, to_last_first(cleaned)):
            normalized = normalize_match_text(variant)
            if normalized:
                keys.add(normalized)
    return keys


def rank_online_candidate(meta: EpubMetadata, title: str, authors: list[str], identifiers: Iterable[str]) -> tuple[int, str]:
    meta_isbns = set(extract_isbns(meta.identifiers))
    candidate_isbns = set(extract_isbns(identifiers))
    if meta_isbns and candidate_isbns and meta_isbns.intersection(candidate_isbns):
        return 420, "isbn-exact"

    meta_title = clean(meta.title or meta.core)
    title_exact = bool(meta_title) and normalize_match_text(meta_title) == normalize_match_text(title)
    title_similarity = similarity_score(meta_title, title)

    meta_authors: list[str] = []
    for creator in meta.creators:
        meta_authors.extend(split_authors(creator))
    meta_author_keys = author_match_keys(meta_authors)
    candidate_author_keys = author_match_keys(authors)
    author_exact = bool(meta_author_keys and candidate_author_keys and meta_author_keys.intersection(candidate_author_keys))

    author_similarity = 0.0
    for meta_author in meta_author_keys:
        for candidate_author in candidate_author_keys:
            author_similarity = max(
                author_similarity,
                difflib.SequenceMatcher(None, meta_author, candidate_author).ratio(),
            )

    if title_exact and author_exact:
        return 320, "title-author-exact"
    if title_exact:
        return 250 + int(author_similarity * 40), "title-exact"

    blended = int(title_similarity * 100) + int(author_similarity * 35)
    if title_similarity >= 0.75 and author_similarity < 0.25:
        blended -= 20
    if title_similarity >= 0.82 and (author_exact or author_similarity >= 0.68):
        return 180 + blended, "title-author-approx"
    if title_similarity >= 0.9:
        return 150 + blended, "title-approx"
    return 100 + blended, "approx"


def online_confidence(score: int) -> int:
    if score >= 420:
        return 78
    if score >= 320:
        return 72
    if score >= 250:
        return 66
    if score >= 180:
        return 60
    return 54


def build_online_candidates(
    meta: EpubMetadata,
    source: str,
    provider_label: str,
    candidates: Iterable[tuple[str, list[str], list[str]]],
) -> list[OnlineCandidate]:
    ranked: list[OnlineCandidate] = []
    for title, authors, identifiers in candidates:
        cleaned_title = clean(title)
        cleaned_authors = [clean(author) for author in authors if clean(author)]
        cleaned_identifiers = [clean(identifier) for identifier in identifiers if clean(identifier)]
        if not cleaned_title and not cleaned_authors:
            continue
        score, reason = rank_online_candidate(meta, cleaned_title, cleaned_authors, cleaned_identifiers)
        score += PROVIDER_SCORE_ADJUSTMENTS.get(provider_label, 0)
        ranked.append(
            OnlineCandidate(
                provider=provider_label,
                source=source,
                title=cleaned_title,
                authors=cleaned_authors,
                identifiers=cleaned_identifiers,
                score=score,
                reason=reason,
            )
        )
    return ranked


def online_candidate_group_key(candidate: OnlineCandidate) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    identifiers = tuple(sorted(extract_isbns(candidate.identifiers)))
    title_key = normalize_match_text(candidate.title)
    author_keys = tuple(sorted(author_match_keys(candidate.authors)))
    return title_key, author_keys, identifiers


def aggregate_online_candidates(candidates: Iterable[OnlineCandidate]) -> list[RankedOnlineMatch]:
    grouped: dict[tuple[str, tuple[str, ...], tuple[str, ...]], list[OnlineCandidate]] = {}
    for candidate in candidates:
        key = online_candidate_group_key(candidate)
        grouped.setdefault(key, []).append(candidate)

    aggregated: list[RankedOnlineMatch] = []
    for group in grouped.values():
        providers: list[str] = []
        sources: list[str] = []
        identifiers: list[str] = []
        best = max(group, key=lambda item: item.score)
        for item in group:
            if item.provider not in providers:
                providers.append(item.provider)
            if item.source not in sources:
                sources.append(item.source)
            for identifier in item.identifiers:
                normalized = clean(identifier)
                if normalized and normalized not in identifiers:
                    identifiers.append(normalized)

        aggregate_score = best.score + max(0, (len(providers) - 1) * 25)
        aggregate_reason = best.reason if len(providers) == 1 else f"{best.reason}+consensus"
        aggregated.append(
            RankedOnlineMatch(
                providers=providers,
                sources=sources,
                title=best.title,
                authors=best.authors,
                identifiers=identifiers,
                score=aggregate_score,
                reason=aggregate_reason,
            )
        )

    aggregated.sort(key=lambda item: item.score, reverse=True)
    return aggregated


def pick_best_online_match(meta: EpubMetadata, candidates: Iterable[OnlineCandidate]) -> RankedOnlineMatch | None:
    aggregated = aggregate_online_candidates(candidates)
    if not aggregated:
        return None
    best = aggregated[0]
    if best.score < 140 and not best.title:
        return None
    if len(aggregated) > 1 and best.score < 420:
        second = aggregated[1]
        if best.score - second.score < ONLINE_AMBIGUITY_MARGIN:
            best = RankedOnlineMatch(
                providers=best.providers,
                sources=best.sources,
                title=best.title,
                authors=best.authors,
                identifiers=best.identifiers,
                score=max(0, best.score - 10),
                reason=f"{best.reason}+ambiguous",
            )
    if len(best.providers) == 1 and best.providers[0] == "lubimyczytac" and best.score < 420:
        best = RankedOnlineMatch(
            providers=best.providers,
            sources=best.sources,
            title=best.title,
            authors=best.authors,
            identifiers=best.identifiers,
            score=max(0, best.score - 15),
            reason=f"{best.reason}+best-effort",
        )
    return best


def build_online_record(meta: EpubMetadata, best: RankedOnlineMatch) -> BookRecord:
    provider_text = ",".join(best.providers)
    source_text = ",".join(best.sources)
    review_reasons: list[str] = []
    if "ambiguous" in best.reason:
        review_reasons.append("online-niejednoznaczne")
    if "best-effort" in best.reason:
        review_reasons.append("online-best-effort")
    return BookRecord(
        path=meta.path,
        author=extract_authors(best.authors, ""),
        series="",
        volume=None,
        title=best.title,
        source=source_text or "online-aggregate",
        identifiers=best.identifiers or extract_isbns(meta.identifiers),
        notes=[
            "online:title-author-only",
            f"online-rank:{best.reason}",
            f"online-score:{best.score}",
            f"online-providers:{provider_text}",
        ],
        confidence=online_confidence(best.score),
        review_reasons=review_reasons,
        decision_reasons=[f"online-candidate:{provider_text}", f"match:{best.reason}"],
        online_checked=True,
    )



def parse_existing_filename(stem: str) -> tuple[str, str, tuple[int, str] | None, str] | None:
    prefix, sep, title = stem.rpartition(" - ")
    if not sep or not title:
        return None

    left, sep, volume_text = prefix.rpartition(" - ")
    if not sep or not volume_text.startswith("Tom "):
        return None

    author, sep, series = left.rpartition(" - ")
    if not sep or not author or not series:
        return None

    volume = parse_volume_parts(volume_text)
    if volume is None:
        return None

    return clean(author), clean_series(series), volume, clean(title)



def add_candidate(
    candidates: list[Candidate],
    series: str,
    volume: tuple[int, str] | None,
    score: int,
    source: str,
    title_override: str | None = None,
) -> None:
    cleaned = clean_series(series)
    if not cleaned:
        return
    candidates.append(Candidate(score, cleaned, volume, clean(title_override), source))


def series_candidate_priority(candidate: Candidate) -> tuple[int, int, int]:
    return (
        SERIES_SOURCE_PRIORITIES.get(candidate.source, candidate.score),
        candidate.score,
        1 if candidate.title_override else 0,
    )


def choose_series_candidate(candidates: list[Candidate]) -> Candidate | None:
    if not candidates:
        return None
    return max(candidates, key=series_candidate_priority)


def choose_title_candidate(candidates: list[Candidate]) -> Candidate | None:
    title_candidates = [candidate for candidate in candidates if candidate.title_override]
    if not title_candidates:
        return None
    return max(title_candidates, key=lambda candidate: (candidate.score, len(candidate.title_override or "")))


def source_needs_online_verification(source: str) -> bool:
    return source.startswith("core:") or source.startswith("segment:")


def collect_title_candidates(title: str, candidates: list[Candidate]) -> None:
    title = clean(title)
    if not title:
        return

    match = TITLE_WITH_SERIES_RE.match(title)
    if match:
        add_candidate(
            candidates,
            match.group(2),
            parse_volume_parts(match.group(3)),
            93,
            "title:series-book",
            match.group(1),
        )

    match = PAREN_SERIES_RE.match(title)
    if match:
        add_candidate(
            candidates,
            match.group(2),
            parse_volume_parts(match.group(3)),
            92,
            "title:paren-series",
            match.group(1),
        )

    match = TITLE_COLON_SERIES_INDEX_RE.match(title)
    if match:
        add_candidate(
            candidates,
            match.group(2),
            parse_volume_parts(match.group(3)),
            94,
            "title:colon-series-index",
            match.group(1),
        )

    match = INDEXED_TITLE_RE.match(title)
    if match and not re.match(r"^\d", clean(match.group(3))):
        add_candidate(
            candidates,
            match.group(1),
            parse_volume_parts(match.group(2)),
            86,
            "title:indexed",
            match.group(3),
        )

    match = INDEX_ONLY_RE.match(title)
    if match:
        add_candidate(
            candidates,
            match.group(1),
            parse_volume_parts(match.group(2)),
            78,
            "title:index-only",
        )



def collect_core_candidates(core: str, candidates: list[Candidate]) -> None:
    core = clean(core)
    if not core:
        return

    match = PAREN_SERIES_RE.match(core)
    if match:
        add_candidate(
            candidates,
            match.group(2),
            parse_volume_parts(match.group(3)),
            90,
            "core:paren-series",
            match.group(1),
        )

    match = CORE_COMMA_RE.match(core)
    if match:
        add_candidate(
            candidates,
            match.group(1),
            parse_volume_parts(match.group(2)),
            87,
            "core:comma",
            match.group(3),
        )

    match = CORE_JOINED_RE.match(core)
    if match and not re.match(r"^\d+\b", clean(match.group(3))):
        add_candidate(
            candidates,
            match.group(1),
            parse_volume_parts(match.group(2)),
            88,
            "core:joined",
            match.group(3),
        )

    match = CORE_SPACED_RE.match(core)
    if match and not re.match(r"^\d+\b", clean(match.group(3))):
        add_candidate(
            candidates,
            match.group(1),
            parse_volume_parts(match.group(2)),
            80,
            "core:spaced",
            match.group(3),
        )

    match = CORE_INDEX_ONLY_RE.match(core)
    if match:
        add_candidate(
            candidates,
            match.group(1),
            parse_volume_parts(match.group(2)),
            76,
            "core:index-only",
        )



def collect_segment_candidates(segments: list[str], candidates: list[Candidate]) -> None:
    for segment in segments[2:6]:
        segment = clean(segment)
        if not segment:
            continue

        match = SEGMENT_HASH_RE.search(segment)
        if match:
            add_candidate(
                candidates,
                match.group(1),
                parse_volume_parts(match.group(2)),
                74,
                "segment:hash",
            )

        match = SEGMENT_COMMA_RE.match(segment)
        if match:
            add_candidate(
                candidates,
                match.group(1),
                parse_volume_parts(match.group(2)),
                72,
                "segment:comma",
            )

        match = SEGMENT_YEAR_RE.match(segment)
        if match:
            add_candidate(
                candidates,
                match.group(1),
                parse_volume_parts(match.group(2)),
                75,
                "segment:year",
            )



def sanitize_title(title: str, series: str, volume: tuple[int, str] | None) -> str:
    title = clean(title)
    if not title:
        return ""
    title = TRAILING_SERIES_SUFFIX_RE.sub("", title)
    if series and volume is not None:
        prefix = rf"^{re.escape(series)}\s+{volume_match_pattern(volume)}\s*[:\-]\s*"
        title = re.sub(prefix, "", title, flags=re.IGNORECASE)
    if is_series_volume_only_title(title, series, volume):
        return ""
    return clean(title)


def is_series_volume_only_title(title: str, series: str, volume: tuple[int, str] | None) -> bool:
    title = clean(title)
    series = clean_series(series)
    if not title or not series or volume is None:
        return False

    volume_pattern = rf"(?:{volume_match_pattern(volume)})"
    series_pattern = re.escape(series)
    patterns = [
        rf"^{series_pattern}\s*(?:[-_:,]\s*)?(?:{SERIES_WORDS}\s*)?{volume_pattern}$",
        rf"^{series_pattern}\s*(?:[-_:,]\s*)?{volume_pattern}$",
        rf"^(?:{SERIES_WORDS}\s*)?{volume_pattern}\s*(?:[-_:,]\s*)?{series_pattern}$",
    ]
    return any(re.match(pattern, title, flags=re.IGNORECASE) for pattern in patterns)



def strip_author_from_title(title: str, author: str) -> str:
    title = clean(title)
    if not title or not author:
        return title
    for token in [part.strip() for part in author.split("&") if part.strip()]:
        names = [name for name in clean(token).split() if len(name) > 1]
        if len(names) < 2:
            continue
        title = re.sub(rf"\b{re.escape(names[0])}\b", "", title, flags=re.IGNORECASE)
        title = re.sub(rf"\b{re.escape(names[-1])}\b", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s{2,}", " ", title)
    title = re.sub(r"\s*[-,:]\s*$", "", title)
    return clean(title)



def read_book_metadata(path: Path) -> EpubMetadata:
    stem = path.stem
    segments = [clean(part) for part in re.split(r"\s*--\s*", stem) if clean(part)]
    core = segments[0] if segments else stem
    meta = EpubMetadata(path=path, stem=stem, segments=segments, core=core)

    if path.suffix.lower() != ".epub":
        return meta

    try:
        book = epub.read_epub(str(path), options={"ignore_ncx": True})
        meta.title = clean(book.get_metadata("DC", "title")[0][0]) if book.get_metadata("DC", "title") else ""
        meta.creators = [clean(item[0]) for item in book.get_metadata("DC", "creator") if clean(item[0])]
        meta.identifiers = [clean(item[0]) for item in book.get_metadata("DC", "identifier") if clean(item[0])]

        for namespace, key in (
            ("OPF", "calibre:series"),
            ("OPF", "series"),
            ("OPF", "belongs-to-collection"),
        ):
            values = book.get_metadata(namespace, key)
            if values and not meta.meta_series:
                meta.meta_series = clean_series(values[0][0])

        for namespace, key in (
            ("OPF", "calibre:series_index"),
            ("OPF", "series_index"),
            ("OPF", "group-position"),
        ):
            values = book.get_metadata(namespace, key)
            if values and meta.meta_volume is None:
                meta.meta_volume = parse_volume_parts(values[0][0])
    except Exception as exc:
        meta.errors.append(f"epub-read: {exc}")

    return meta



def extract_authors(creators: list[str], segment_author: str) -> str:
    raw: list[str] = []
    for creator in creators:
        raw.extend(split_authors(creator))
    if segment_author and re.search(r"[A-Za-z]", segment_author) and len(creators) <= 1:
        raw.extend(split_authors(segment_author))
    result = canonicalize_authors(raw)
    return " & ".join(result) if result else "Nieznany Autor"



def online_query(url: str, timeout: float) -> dict | None:
    payload = online_fetch(url, timeout, kind="json")
    return payload if isinstance(payload, dict) else None


def online_text_query(url: str, timeout: float) -> str | None:
    payload = online_fetch(url, timeout, kind="text")
    return payload if isinstance(payload, str) else None



def google_books_candidates(meta: EpubMetadata, timeout: float) -> list[OnlineCandidate]:
    isbns = extract_isbns(meta.identifiers)
    query = ""
    if isbns:
        query = f"isbn:{isbns[0]}"
    else:
        title = clean(meta.title or meta.core)
        author = clean(meta.creators[0] if meta.creators else "")
        if not title:
            return []
        parts = [f'intitle:"{title}"']
        if author:
            parts.append(f'inauthor:"{author}"')
        query = " ".join(parts)

    params = urllib.parse.urlencode(
        {
            "q": query,
            "maxResults": 3,
            "printType": "books",
            "projection": "lite",
        }
    )
    payload = online_query(f"https://www.googleapis.com/books/v1/volumes?{params}", timeout)
    if not payload or not payload.get("items"):
        return []

    candidates: list[tuple[str, list[str], list[str]]] = []
    for entry in payload.get("items", [])[:10]:
        item = entry.get("volumeInfo", {})
        title = clean(item.get("title"))
        subtitle = clean(item.get("subtitle"))
        if title and subtitle:
            title = f"{title}: {subtitle}"
        authors = [clean(author) for author in item.get("authors") or [] if clean(author)]
        identifiers = [
            clean(identifier.get("identifier"))
            for identifier in item.get("industryIdentifiers", [])
            if clean(identifier.get("identifier"))
        ]
        candidates.append((title, authors, identifiers))

    return build_online_candidates(meta, "google-books", "google-books", candidates)



def open_library_candidates(meta: EpubMetadata, timeout: float) -> list[OnlineCandidate]:
    isbns = extract_isbns(meta.identifiers)
    found: list[OnlineCandidate] = []
    if isbns:
        params = urllib.parse.urlencode({"bibkeys": f"ISBN:{isbns[0]}", "format": "json", "jscmd": "data"})
        payload = online_query(f"https://openlibrary.org/api/books?{params}", timeout)
        if payload:
            data = payload.get(f"ISBN:{isbns[0]}")
            if data:
                authors = [clean(author.get("name")) for author in data.get("authors", []) if clean(author.get("name"))]
                found.extend(
                    build_online_candidates(
                        meta,
                        "open-library:isbn",
                        "open-library",
                        [(clean(data.get("title")), authors, list(isbns))],
                    )
                )

    title = clean(meta.title or meta.core)
    if not title:
        return found
    params = urllib.parse.urlencode({"title": title, "limit": 10})
    payload = online_query(f"https://openlibrary.org/search.json?{params}", timeout)
    docs = (payload or {}).get("docs") or []
    if not docs:
        return found

    candidates: list[tuple[str, list[str], list[str]]] = []
    for doc in docs[:10]:
        authors = [clean(name) for name in doc.get("author_name", []) if clean(name)]
        identifiers = [clean(identifier) for identifier in doc.get("isbn", [])[:5] if clean(identifier)]
        candidates.append((clean(doc.get("title")), authors, identifiers))

    found.extend(build_online_candidates(meta, "open-library:search", "open-library", candidates))
    return found


def crossref_candidates(meta: EpubMetadata, timeout: float) -> list[OnlineCandidate]:
    isbns = extract_isbns(meta.identifiers)
    if not isbns:
        return []

    params = {
        "rows": 10,
        "select": "title,subtitle,author,ISBN,type",
        "query.bibliographic": isbns[0],
    }

    payload = online_query(f"https://api.crossref.org/works?{urllib.parse.urlencode(params)}", timeout)
    items = ((payload or {}).get("message") or {}).get("items") or []
    if not items:
        return []

    candidates: list[tuple[str, list[str], list[str]]] = []
    for item in items[:10]:
        if clean(item.get("type")).lower() not in {"book", "book-chapter", "monograph"}:
            continue
        title_text = clean((item.get("title") or [""])[0])
        subtitle = clean((item.get("subtitle") or [""])[0])
        if title_text and subtitle:
            title_text = f"{title_text}: {subtitle}"
        authors = []
        for author in item.get("author") or []:
            given = clean(author.get("given"))
            family = clean(author.get("family"))
            full = clean(f"{given} {family}")
            if full:
                authors.append(full)
        identifiers = [clean(identifier) for identifier in item.get("ISBN") or [] if clean(identifier)]
        candidates.append((title_text, authors, identifiers))

    return build_online_candidates(meta, "crossref", "crossref", candidates)


def hathitrust_candidates(meta: EpubMetadata, timeout: float) -> list[OnlineCandidate]:
    isbns = extract_isbns(meta.identifiers)
    if not isbns:
        return []

    payload = online_query(f"https://catalog.hathitrust.org/api/volumes/brief/isbn/{isbns[0]}.json", timeout)
    items = (payload or {}).get("items") or []
    records = (payload or {}).get("records") or {}
    if not items and not records:
        return []

    candidates: list[tuple[str, list[str], list[str]]] = []
    for item in items:
        from_record = records.get(item.get("fromRecord"), {})
        title = clean(from_record.get("title"))
        authors = [clean(author) for author in from_record.get("authors") or [] if clean(author)]
        identifiers = [isbns[0]]
        candidates.append((title, authors, identifiers))

    if not candidates:
        for record in records.values():
            title = clean(record.get("title"))
            authors = [clean(author) for author in record.get("authors") or [] if clean(author)]
            candidates.append((title, authors, list(isbns)))

    return build_online_candidates(meta, "hathitrust", "hathitrust", candidates)


def lubimyczytac_candidates(meta: EpubMetadata, timeout: float) -> list[OnlineCandidate]:
    terms: list[str] = []
    isbns = extract_isbns(meta.identifiers)
    if isbns:
        terms.append(isbns[0])
    title = clean(meta.title or meta.core)
    if title:
        terms.append(title)

    found: list[OnlineCandidate] = []
    seen_terms: set[str] = set()
    for term in terms:
        normalized_term = normalize_match_text(term)
        if not normalized_term or normalized_term in seen_terms:
            continue
        seen_terms.add(normalized_term)

        params = urllib.parse.urlencode({"phrase": term})
        url = f"https://lubimyczytac.pl/szukaj/ksiazki?{params}"
        page = online_text_query(url, timeout)
        if not page:
            continue

        candidates: list[tuple[str, list[str], list[str]]] = []
        parser = LubimyczytacSearchParser()
        parser.feed(page)
        parser.close()
        for title_text, authors in parser.results[:12]:
            if not authors:
                continue
            candidates.append((title_text, authors, list(isbns)))

        found.extend(build_online_candidates(meta, "lubimyczytac", "lubimyczytac", candidates))

    return found



def enrich_from_online(meta: EpubMetadata, providers: list[str], timeout: float) -> BookRecord | None:
    all_candidates: list[OnlineCandidate] = []
    provider_functions = {
        "google": google_books_candidates,
        "openlibrary": open_library_candidates,
        "crossref": crossref_candidates,
        "hathitrust": hathitrust_candidates,
        "lubimyczytac": lubimyczytac_candidates,
    }
    with ONLINE_ENRICH_SEMAPHORE:
        for provider in providers:
            func = provider_functions.get(provider)
            if func is None:
                continue
            try:
                candidates = func(meta, timeout)
            except Exception as exc:
                meta.errors.append(f"{provider}: {exc}")
                continue
            all_candidates.extend(candidates)
            if any(candidate.reason == "isbn-exact" and candidate.score >= 420 for candidate in candidates):
                break

    best = pick_best_online_match(meta, all_candidates)
    if best is None:
        return None
    return build_online_record(meta, best)



def finalize_record_quality(record: BookRecord, meta: EpubMetadata, base_confidence: int, title_from_core: bool) -> BookRecord:
    confidence = base_confidence
    review_reasons = list(record.review_reasons)

    if record.author == "Nieznany Autor":
        confidence -= 25
        review_reasons.append("nieznany-autor")

    if not record.title or record.title == "Bez tytulu":
        confidence -= 30
        review_reasons.append("brak-tytulu")

    if title_from_core:
        confidence -= 5
        review_reasons.append("tytul-z-nazwy-pliku")

    if record.source == "fallback":
        confidence -= 15
        review_reasons.append("fallback")

    if meta.errors:
        confidence -= 10
        review_reasons.append("blad-odczytu-metadanych")

    if HEX_NOISE_RE.search(record.title):
        confidence -= 20
        review_reasons.append("szum-w-tytule")

    if ANNA_ARCHIVE_RE.search(record.title):
        confidence -= 20
        review_reasons.append("artefakt-zrodla")

    deduped_reasons: list[str] = []
    seen: set[str] = set()
    for reason in review_reasons:
        if reason not in seen:
            seen.add(reason)
            deduped_reasons.append(reason)

    record.confidence = max(0, min(100, confidence))
    record.review_reasons = deduped_reasons
    return record



def infer_record(meta: EpubMetadata, use_online: bool, providers: list[str], timeout: float) -> BookRecord:
    existing = parse_existing_filename(meta.stem)
    if existing is not None:
        author, series, volume, title = existing
        record = BookRecord(
            path=meta.path,
            author=author,
            series=series or "Standalone",
            volume=volume,
            title=title or "Bez tytulu",
            source="existing-format",
            identifiers=extract_isbns(meta.identifiers),
            notes=list(meta.errors),
            confidence=100,
            review_reasons=[],
            decision_reasons=["existing-format"],
        )
        return finalize_record_quality(record, meta, 100, title_from_core=False)

    segment_author = ""
    if len(meta.segments) > 1:
        second = meta.segments[1]
        if re.search(r"[A-Za-z]", second) and not re.match(r"^\d{4}$", second):
            segment_author = second

    author = extract_authors(meta.creators, segment_author)
    candidates: list[Candidate] = []

    if meta.meta_series:
        add_candidate(candidates, meta.meta_series, meta.meta_volume, 100, "opf")
    collect_title_candidates(meta.title, candidates)
    collect_core_candidates(meta.core, candidates)
    collect_segment_candidates(meta.segments, candidates)

    if candidates:
        best_series = choose_series_candidate(candidates)
        best_title = choose_title_candidate(candidates)
        assert best_series is not None
        series = best_series.series
        volume = best_series.volume
        title_override = best_title.title_override if best_title else best_series.title_override
        source_parts = [best_series.source]
        if best_title and best_title.source not in source_parts:
            source_parts.append(best_title.source)
        source = "+".join(source_parts)
        base_confidence = max(
            best_series.score,
            best_title.score if best_title else 0,
        )
    else:
        series = "Standalone"
        volume = None
        title_override = None
        source = "fallback"
        base_confidence = 45

    local_title = sanitize_title(meta.title, series, volume)
    title = title_override or local_title or clean(meta.core)
    title_from_core = not bool(title_override or local_title)
    if local_title and title_override:
        if len(title_override) > len(local_title) + 12 or HEX_NOISE_RE.search(title_override) or ANNA_ARCHIVE_RE.search(title_override):
            title = local_title
            title_from_core = False

    title = strip_author_from_title(title, author)
    title = clean(ANNA_ARCHIVE_RE.sub("", title))
    title = sanitize_title(title, series, volume)
    notes = list(meta.errors)

    record = BookRecord(
        path=meta.path,
        author=author,
        series=clean_series(series) or "Standalone",
        volume=volume,
        title=title,
        source=source,
        identifiers=extract_isbns(meta.identifiers),
        notes=notes,
        confidence=base_confidence,
        review_reasons=[],
        decision_reasons=[f"inference:{source}"],
    )

    if use_online and (
        record.series == "Standalone"
        or record.volume is None
        or record.author == "Nieznany Autor"
        or source_needs_online_verification(record.source)
    ):
        online = enrich_from_online(meta, providers, timeout)
        if online:
            record.online_checked = True
            record.notes.append(f"online-checked:{online.source}")
            record.decision_reasons.append(f"online-checked:{online.source}")
            if online.review_reasons:
                record.review_reasons.extend(online.review_reasons)
            blocked_online = any(
                reason in {"online-niejednoznaczne", "online-best-effort"}
                for reason in online.review_reasons
            )
            if not blocked_online:
                online_applied = False
                if record.author == "Nieznany Autor" and online.author != "Nieznany Autor":
                    record.author = online.author
                    online_applied = True
                if (not record.title or HEX_NOISE_RE.search(record.title) or ANNA_ARCHIVE_RE.search(record.title)) and online.title:
                    record.title = online.title
                    title_from_core = False
                    online_applied = True
                if online.title:
                    online_candidates: list[Candidate] = []
                    collect_title_candidates(online.title, online_candidates)
                    if online_candidates:
                        best_online_series = choose_series_candidate(online_candidates)
                        best_online_title = choose_title_candidate(online_candidates)
                        if best_online_title and (
                            not record.title
                            or HEX_NOISE_RE.search(record.title)
                            or ANNA_ARCHIVE_RE.search(record.title)
                        ):
                            record.title = best_online_title.title_override or record.title
                            online_applied = True
                        if best_online_series and (
                            record.series == "Standalone"
                            or record.volume is None
                            or record.source.startswith("core:spaced")
                            or record.source.startswith("core:index-only")
                        ):
                            new_series = clean_series(best_online_series.series) or record.series
                            if new_series != record.series:
                                record.series = new_series
                                online_applied = True
                        if best_online_series and (
                            record.volume is None
                            or record.source.startswith("core:spaced")
                            or record.source.startswith("core:index-only")
                        ):
                            if best_online_series.volume != record.volume:
                                record.volume = best_online_series.volume
                                online_applied = True
                        record.title = sanitize_title(record.title, record.series, record.volume) or record.title
                if online_applied:
                    record.online_applied = True
                    record.notes.append(f"online-applied:{online.source}")
                    record.decision_reasons.extend(online.decision_reasons)
                    base_confidence = max(base_confidence, online.confidence)

    if not record.title:
        fallback_title = sanitize_title(meta.core, record.series, record.volume)
        if fallback_title:
            record.title = fallback_title
            title_from_core = True
        else:
            record.title = "Bez tytulu"
    if not record.series:
        record.series = "Standalone"

    return finalize_record_quality(record, meta, base_confidence, title_from_core)



def make_record_clone(
    record: BookRecord,
    *,
    title: str | None = None,
    notes: list[str] | None = None,
    confidence: int | None = None,
    review_reasons: list[str] | None = None,
    decision_reasons: list[str] | None = None,
    filename_suffix: str | None = None,
) -> BookRecord:
    return BookRecord(
        path=record.path,
        author=record.author,
        series=record.series,
        volume=record.volume,
        title=title if title is not None else record.title,
        source=record.source,
        identifiers=list(record.identifiers),
        notes=list(notes) if notes is not None else list(record.notes),
        confidence=record.confidence if confidence is None else confidence,
        review_reasons=list(review_reasons) if review_reasons is not None else list(record.review_reasons),
        decision_reasons=list(decision_reasons) if decision_reasons is not None else list(record.decision_reasons),
        filename_suffix=record.filename_suffix if filename_suffix is None else filename_suffix,
        output_folder=record.output_folder,
        online_checked=record.online_checked,
        online_applied=record.online_applied,
    )


def set_output_folder(records: list[BookRecord], folder: Path) -> list[BookRecord]:
    for record in records:
        record.output_folder = folder
    return records



def dedupe_destinations(records: list[BookRecord], folder: Path) -> list[BookRecord]:
    source_paths = {record.path.resolve() for record in records}
    reserved_names: set[str] = set()
    if folder.exists():
        existing_items = folder.iterdir()
    else:
        existing_items = []
    for existing in existing_items:
        if not is_supported_book_file(existing):
            continue
        if existing.resolve() not in source_paths:
            reserved_names.add(existing.name.lower())

    used_names = set(reserved_names)
    final: list[BookRecord] = []

    for record in records:
        final_record = record
        suffix_no = 0
        while final_record.filename.lower() in used_names:
            suffix_no += 1
            final_record = make_record_clone(
                record,
                notes=record.notes + ["dedupe-suffix", "existing-file-conflict"],
                confidence=max(0, record.confidence - 5),
                review_reasons=record.review_reasons + ["kolizja-nazwy"],
                decision_reasons=record.decision_reasons + [f"dedupe:filename-suffix-{suffix_no}"],
                filename_suffix=f"({suffix_no})",
            )

        used_names.add(final_record.filename.lower())
        final.append(final_record)

    return final



def build_moves(records: list[BookRecord], source_folder: Path, target_folder: Path, stamp: str) -> list[RenameMove]:
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
        if move.operation == "delete":
            continue
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
        deleted: list[Path] = []
        try:
            for move in moves:
                if move.source.exists():
                    move.source.unlink()
                    deleted.append(move.source)
        except Exception as exc:
            return [f"delete:{move.source.name}: {exc}"]
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



def write_report(
    path: Path,
    rows: list[BookRecord],
    dry_run: bool,
    source_folder: Path,
    target_folder: Path,
    operation: str,
    execution_status: dict[Path, str] | None = None,
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
                "mode",
            ]
        )
        for row in rows:
            target_name = row.filename
            status = ""
            if execution_status is not None:
                status = execution_status.get(row.path.resolve(), "")
            if not status:
                if row.needs_review:
                    status = "review-required"
                elif source_folder.resolve() == target_folder.resolve() and row.path.name == target_name:
                    status = "unchanged"
                else:
                    status = "planned"
            writer.writerow(
                [
                    row.path.name,
                    target_name,
                    str(source_folder),
                    str(target_folder),
                    row.path.name,
                    target_name,
                    row.author,
                    row.series,
                    format_volume(row.volume),
                    row.title,
                    row.source,
                    ",".join(row.identifiers),
                    " | ".join(row.notes),
                    row.confidence,
                    "CHECK" if row.needs_review else "OK",
                    " | ".join(row.review_reasons),
                    " | ".join(row.decision_reasons),
                    "yes" if row.online_checked else "no",
                    "yes" if row.online_applied else "no",
                    operation if row.path.name != target_name or source_folder.resolve() != target_folder.resolve() else "unchanged",
                    status,
                    operation,
                    "dry-run" if dry_run else "apply",
                ]
            )



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
                moves.append(RenameMove(current, None, destination, None, "delete"))
            else:
                temp = target_folder / f"__tmp_undo_{stamp}_{index:04d}{suffix}"
                moves.append(RenameMove(current, temp, destination, None, "rename"))

    return UndoPlan(folder=folder, moves=moves, total_rows=total_rows)



def execute_undo(report_path: Path, folder_hint: Path | None = None) -> int:
    try:
        plan = build_undo_plan(report_path, folder_hint)
    except Exception as exc:
        LOGGER.error(f"Nie mozna odczytac logu undo: {exc}")
        return 2

    if not plan.moves:
        log_lines(
            [
                f"UNDO_REPORT={report_path}",
                f"UNDO_FOLDER={plan.folder}",
                "UNDO_MOVES=0",
                "UNDO_ERRORS=0",
            ]
        )
        return 0

    missing = [move.source.name for move in plan.moves if not move.source.exists()]
    if missing:
        log_lines(
            [
                f"UNDO_REPORT={report_path}",
                f"UNDO_FOLDER={plan.folder}",
                f"UNDO_MOVES={len(plan.moves)}",
                f"UNDO_ERRORS={len(missing)}",
                "---ERRORS---",
            ]
        )
        log_lines([f"missing-current-file:{name}" for name in missing[:20]], level=logging.ERROR)
        return 1

    errors = execute_moves(plan.moves)
    completed = len(plan.moves) if not errors else 0
    deleted = sum(1 for move in plan.moves if move.operation == "delete") if not errors else 0
    renamed = sum(1 for move in plan.moves if move.operation == "rename") if not errors else 0
    log_lines(
        [
            f"UNDO_REPORT={report_path}",
            f"UNDO_FOLDER={plan.folder}",
            f"UNDO_TOTAL_ROWS={plan.total_rows}",
            f"UNDO_MOVES={len(plan.moves)}",
            f"UNDO_DONE={completed}",
            f"UNDO_RENAMED={renamed}",
            f"UNDO_DELETED={deleted}",
            f"UNDO_ERRORS={len(errors)}",
        ]
    )
    if errors:
        log_lines(["---ERRORS---"])
        log_lines(errors[:20], level=logging.ERROR)
        return 1
    return 0


def run_job(
    folder: Path,
    *,
    destination_folder: Path | None = None,
    apply_changes: bool,
    use_online: bool,
    providers: list[str],
    timeout: float,
    limit: int,
    online_workers: int = DEFAULT_INFER_WORKERS,
) -> tuple[int, list[str]]:
    started_at = time.perf_counter()
    if not folder.exists():
        return 2, [f"Folder nie istnieje: {folder}"]
    target_folder = destination_folder if destination_folder is not None else folder
    operation = "rename" if target_folder.resolve() == folder.resolve() else "copy"
    if apply_changes and operation == "copy":
        target_folder.mkdir(parents=True, exist_ok=True)

    files = sorted(path for path in folder.iterdir() if is_supported_book_file(path))
    if limit > 0:
        files = files[:limit]

    if not files:
        return 0, ["Brak obslugiwanych plikow ebook."]

    read_started_at = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max(1, min(8, len(files)))) as executor:
        metas = list(executor.map(read_book_metadata, files))
    read_ms = int((time.perf_counter() - read_started_at) * 1000)
    infer_started_at = time.perf_counter()
    infer_cap = max(1, online_workers) if use_online else 8
    infer_workers = max(1, min(8, infer_cap, len(metas)))
    with ThreadPoolExecutor(max_workers=infer_workers) as executor:
        records = list(
            executor.map(
                lambda meta: infer_record(meta, use_online=use_online, providers=providers, timeout=timeout),
                metas,
            )
        )
    infer_ms = int((time.perf_counter() - infer_started_at) * 1000)

    records = set_output_folder(records, target_folder)
    records = dedupe_destinations(records, target_folder)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dry_run = not apply_changes
    report_name = "rename_books_preview" if dry_run else "rename_books_log"
    report_path = folder / f"{report_name}_{stamp}.csv"
    execution_status: dict[Path, str] = {}
    if dry_run:
        for record in records:
            if operation == "rename" and record.path.name == record.filename:
                execution_status[record.path.resolve()] = "unchanged"
            else:
                execution_status[record.path.resolve()] = "planned"
    else:
        for record in records:
            if operation == "rename" and record.path.name == record.filename:
                execution_status[record.path.resolve()] = "unchanged"
            else:
                execution_status[record.path.resolve()] = "pending"

    lines: list[str] = []
    if dry_run:
        write_report(
            report_path,
            records,
            dry_run=True,
            source_folder=folder,
            target_folder=target_folder,
            operation=operation,
            execution_status=execution_status,
        )
        review_count = sum(1 for record in records if record.needs_review)
        lines.append(
            f"TOTAL={len(records)} | REVIEW={review_count} | TO_WRITE={len(build_moves(records, folder, target_folder, stamp))}"
        )
        lines.extend(
            [
                "MODE=DRY-RUN",
                f"OPERATION={operation.upper()}",
                f"SOURCE={folder}",
                f"DESTINATION={target_folder}",
                f"INFER_WORKERS={infer_workers}",
                f"ONLINE_HTTP_SLOTS={ONLINE_HTTP_SLOTS}",
                f"REVIEW={review_count}",
                f"REPORT={report_path}",
            ]
        )
        lines.extend(
            [
                f"PROFILE_READ_MS={read_ms}",
                f"PROFILE_INFER_MS={infer_ms}",
                f"PROFILE_TOTAL_MS={int((time.perf_counter() - started_at) * 1000)}",
            ]
        )
        for record in records[:10]:
            flag = " [CHECK]" if record.needs_review else ""
            lines.append(
                f"{record.path.name} -> {target_folder / record.filename} (confidence={record.confidence}){flag}"
            )
        flush_online_cache_if_needed(force=True)
        return 0, lines

    moves = build_moves(records, folder, target_folder, stamp)
    execute_started_at = time.perf_counter()
    errors = execute_moves(moves)
    execute_ms = int((time.perf_counter() - execute_started_at) * 1000)
    if errors:
        for move in moves:
            execution_status[move.source.resolve()] = "failed"
    else:
        for move in moves:
            execution_status[move.source.resolve()] = "renamed" if move.operation == "rename" else "copied"
    write_report(
        report_path,
        records,
        dry_run=False,
        source_folder=folder,
        target_folder=target_folder,
        operation=operation,
        execution_status=execution_status,
    )
    review_total = sum(1 for record in records if record.needs_review)
    written_total = len(moves) if not errors else 0
    lines.append(
        f"TOTAL={len(records)} | TO_WRITE={len(moves)} | WRITTEN={written_total} | REVIEW={review_total} | ERRORS={len(errors)}"
    )
    lines.extend(
        [
            f"OPERATION={operation.upper()}",
            f"SOURCE={folder}",
            f"DESTINATION={target_folder}",
            f"INFER_WORKERS={infer_workers}",
            f"ONLINE_HTTP_SLOTS={ONLINE_HTTP_SLOTS}",
            f"TO_WRITE={len(moves)}",
            f"WRITTEN={written_total}",
            f"REVIEW={review_total}",
            f"ERRORS={len(errors)}",
            f"REPORT={report_path}",
        ]
    )
    lines.extend(
        [
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
    return 0, lines


def launch_gui(
    default_folder: str,
    default_destination: str,
    default_providers: str,
    default_timeout: float,
    default_limit: int,
    default_online: bool,
    default_online_workers: int,
) -> int:
    root = tk.Tk()
    root.title(APP_NAME)
    root.geometry("760x520")
    result_queue: queue.Queue[tuple[int, list[str]] | tuple[str, str]] = queue.Queue()
    worker_state = {"running": False}

    folder_var = tk.StringVar(value=default_folder)
    destination_var = tk.StringVar(value=default_destination)
    providers_var = tk.StringVar(value=default_providers)
    timeout_var = tk.StringVar(value=str(default_timeout))
    limit_var = tk.StringVar(value=str(default_limit))
    online_workers_var = tk.StringVar(value=str(default_online_workers))
    online_var = tk.BooleanVar(value=default_online)
    apply_var = tk.BooleanVar(value=False)
    status_var = tk.StringVar(value="Gotowe.")

    def choose_folder() -> None:
        selected = filedialog.askdirectory(initialdir=folder_var.get() or default_folder)
        if selected:
            folder_var.set(selected)

    def choose_destination() -> None:
        selected = filedialog.askdirectory(initialdir=destination_var.get() or folder_var.get() or default_folder)
        if selected:
            destination_var.set(selected)

    def poll_result_queue() -> None:
        try:
            item = result_queue.get_nowait()
        except queue.Empty:
            if worker_state["running"]:
                root.after(150, poll_result_queue)
            return

        worker_state["running"] = False
        run_button.config(state=tk.NORMAL)

        if item and item[0] == "error":
            _, message = item
            status_var.set("Blad.")
            messagebox.showerror("Blad", message)
            return

        code, lines = item  # type: ignore[assignment]
        output.delete("1.0", tk.END)
        output.insert("1.0", "\n".join(lines))
        status_var.set(lines[0] if lines else "Gotowe.")
        if code == 0:
            messagebox.showinfo("Zakonczono", lines[0] if lines else "Gotowe.")
        else:
            messagebox.showwarning("Problem", lines[0] if lines else "Wystapil problem.")
        root.after(150, poll_result_queue)

    def run_from_gui() -> None:
        if worker_state["running"]:
            return
        folder = Path(folder_var.get().strip())
        destination_text = destination_var.get().strip()
        destination = Path(destination_text) if destination_text else None
        providers = [item.strip().lower() for item in providers_var.get().split(",") if item.strip()]
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
            online_workers = int(online_workers_var.get().strip() or str(DEFAULT_INFER_WORKERS))
        except ValueError:
            messagebox.showerror("Blad", "Infer workers musi byc liczba calkowita.")
            return
        apply_changes = apply_var.get()
        use_online = online_var.get()

        worker_state["running"] = True
        run_button.config(state=tk.DISABLED)
        status_var.set("Przetwarzanie...")
        output.delete("1.0", tk.END)
        output.insert("1.0", "Przetwarzanie...")

        def worker() -> None:
            try:
                result_queue.put(
                    run_job(
                        folder,
                        destination_folder=destination,
                        apply_changes=apply_changes,
                        use_online=use_online,
                        providers=providers,
                        timeout=timeout,
                        limit=limit,
                        online_workers=online_workers,
                    )
                )
            except Exception as exc:
                result_queue.put(("error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()
        root.after(150, poll_result_queue)

    frame = ttk.Frame(root, padding=12)
    frame.pack(fill=tk.BOTH, expand=True)

    ttk.Label(frame, text="Folder").grid(row=0, column=0, sticky="w")
    ttk.Entry(frame, textvariable=folder_var, width=70).grid(row=1, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(frame, text="Wybierz", command=choose_folder).grid(row=1, column=1, sticky="ew")

    ttk.Label(frame, text="Destination").grid(row=2, column=0, sticky="w", pady=(10, 0))
    ttk.Entry(frame, textvariable=destination_var, width=70).grid(row=3, column=0, sticky="ew", padx=(0, 8))
    ttk.Button(frame, text="Wybierz", command=choose_destination).grid(row=3, column=1, sticky="ew")

    ttk.Label(frame, text="Providers").grid(row=4, column=0, sticky="w", pady=(10, 0))
    ttk.Entry(frame, textvariable=providers_var, width=70).grid(row=5, column=0, columnspan=2, sticky="ew")

    options = ttk.Frame(frame)
    options.grid(row=6, column=0, columnspan=2, sticky="ew", pady=(10, 0))
    ttk.Checkbutton(options, text="Online", variable=online_var).grid(row=0, column=0, sticky="w")
    ttk.Checkbutton(options, text="Apply", variable=apply_var).grid(row=0, column=1, sticky="w", padx=(12, 0))
    ttk.Label(options, text="Timeout").grid(row=0, column=2, sticky="w", padx=(20, 0))
    ttk.Entry(options, textvariable=timeout_var, width=8).grid(row=0, column=3, sticky="w")
    ttk.Label(options, text="Limit").grid(row=0, column=4, sticky="w", padx=(20, 0))
    ttk.Entry(options, textvariable=limit_var, width=8).grid(row=0, column=5, sticky="w")
    ttk.Label(options, text="Infer workers").grid(row=0, column=6, sticky="w", padx=(20, 0))
    ttk.Entry(options, textvariable=online_workers_var, width=8).grid(row=0, column=7, sticky="w")

    run_button = ttk.Button(frame, text="Uruchom", command=run_from_gui)
    run_button.grid(row=7, column=0, columnspan=2, sticky="ew", pady=(12, 0))

    ttk.Label(frame, textvariable=status_var).grid(row=8, column=0, columnspan=2, sticky="w", pady=(8, 0))

    output = tk.Text(frame, wrap="word", height=20)
    output.grid(row=9, column=0, columnspan=2, sticky="nsew", pady=(12, 0))

    ttk.Label(frame, text=GUI_FOOTER_TEXT, font=("Segoe UI", 8)).grid(
        row=10, column=0, columnspan=2, sticky="e", pady=(6, 0)
    )

    frame.columnconfigure(0, weight=1)
    frame.rowconfigure(9, weight=1)
    root.mainloop()
    return 0



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"{APP_NAME}: rename/copy ebook files with optional online enrichment.")
    parser.add_argument("--folder", default=DEFAULT_SOURCE_FOLDER, help="Folder z plikami ebookow.")
    parser.add_argument("--destination", default="", help="Opcjonalny folder docelowy. Jesli podany i rozny od source, pliki beda kopiowane z nowa nazwa.")
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
        default=DEFAULT_PROVIDERS,
        help="Lista providerow online rozdzielona przecinkami: google, openlibrary, crossref, hathitrust, lubimyczytac.",
    )
    parser.add_argument("--timeout", type=float, default=DEFAULT_HTTP_TIMEOUT, help="Timeout dla zapytan HTTP.")
    parser.add_argument("--limit", type=int, default=0, help="Przetworz tylko pierwsze N plikow.")
    parser.add_argument(
        "--online-workers",
        type=int,
        default=DEFAULT_INFER_WORKERS,
        help="Maksymalna liczba rownoleglych rekordow inferencji; rownolegle lookupy HTTP nadal ogranicza semaphore.",
    )
    parser.add_argument("--undo", default="", help="Sciezka do logu CSV z wykonanego rename, aby cofnac zmiany.")
    return parser.parse_args()



def main() -> int:
    configure_logging()
    args = parse_args()

    if args.undo:
        report_path = Path(args.undo)
        if not report_path.exists():
            LOGGER.error(f"Log undo nie istnieje: {report_path}")
            return 2
        folder_hint = Path(args.folder) if args.folder else None
        if folder_hint is not None and not folder_hint.exists():
            folder_hint = None
        return execute_undo(report_path, folder_hint)

    if not args.cli:
        return launch_gui(args.folder, args.destination, args.providers, args.timeout, args.limit, args.online, args.online_workers)

    folder = Path(args.folder)
    destination = Path(args.destination) if args.destination else None
    providers = [item.strip().lower() for item in args.providers.split(",") if item.strip()]
    code, lines = run_job(
        folder,
        destination_folder=destination,
        apply_changes=args.apply,
        use_online=args.online,
        providers=providers,
        timeout=args.timeout,
        limit=args.limit,
        online_workers=args.online_workers,
    )
    log_lines(lines, level=logging.ERROR if code == 2 else logging.INFO)
    return code


load_online_cache()
atexit.register(lambda: flush_online_cache_if_needed(force=True))


if __name__ == "__main__":
    raise SystemExit(main())
