from __future__ import annotations

import re
from pathlib import Path


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
SERIES_WORDS = r"(?:Book|Tom|Volume|Vol\.?|#|Part|Cykl|Czesc|Cz\u0119\u015b\u0107|Ksiega|Ksi\u0119ga)"
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
TITLE_DOUBLE_COLON_BOOK_RE = re.compile(
    rf"^(.*?)\s*:\s*(.+?)\s*:\s*(?:Book|Tom|Volume|Vol\.?)\s*({VOLUME_INDEX_PATTERN})(?:\s*\([^)]*\))?$",
    re.IGNORECASE,
)
TITLE_DOTTED_SERIES_BOOK_RE = re.compile(
    rf"^(.*?)\.\s*(?:Book|Tom|Volume|Vol\.?|Czesc|Cz\u0119\u015b\u0107|Ksiega|Ksi\u0119ga)\s*({VOLUME_INDEX_PATTERN})\.\s*(.+)$",
    re.IGNORECASE,
)
TRAILING_BOOK_INDEX_RE = re.compile(
    rf"^(.*?)\s*,?\s*(?:Book|Tom|Volume|Vol\.?)\s*({VOLUME_INDEX_PATTERN})(?:\s*\[[^\]]+\])?$",
    re.IGNORECASE,
)
SERIES_ONLY_PAREN_INDEX_RE = re.compile(
    rf"^(.+?)\s*\(\s*(?:Book|Tom|Volume|Vol\.?|Czesc|Cz\u0119\u015b\u0107|Ksiega|Ksi\u0119ga)\s*({VOLUME_INDEX_PATTERN})\s*\)\s*$",
    re.IGNORECASE,
)
BOX_SET_RE = re.compile(
    r"^(.*?)\s+(The Complete Series|Complete Series|Omnibus|Box Set)(?:\s*[-:]\s*(.*?))?(?:\s*\([^)]*\))?$",
    re.IGNORECASE,
)
INDEXED_TITLE_RE = re.compile(rf"^(.+?)\s+({VOLUME_INDEX_PATTERN})\s*[:\-]\s*(.+)$", re.IGNORECASE)
INDEX_ONLY_RE = re.compile(rf"^(.+?)\s*[-:]\s*({VOLUME_INDEX_PATTERN})$", re.IGNORECASE)
CORE_COMMA_RE = re.compile(rf"^(.+?)\s+({VOLUME_INDEX_PATTERN})\s*,\s*(.+)$", re.IGNORECASE)
CORE_JOINED_RE = re.compile(rf"^(.+?)\s+({VOLUME_INDEX_PATTERN})\s*[-_:]\s*(.+)$", re.IGNORECASE)
CORE_SPACED_RE = re.compile(rf"^(.+?)\s+({VOLUME_INDEX_PATTERN})\s+(.+)$", re.IGNORECASE)
CORE_INDEX_ONLY_RE = re.compile(rf"^(.+?)\s+({VOLUME_INDEX_PATTERN})$", re.IGNORECASE)
CORE_TITLE_AUTHOR_RE = re.compile(rf"^(.+?)\s+({VOLUME_INDEX_PATTERN})\s+(.+?)\s+-\s+(.+)$", re.IGNORECASE)
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
GENRE_TAIL_RE = re.compile(
    r"\s*(?:\[[^\]]+\]|\((?:isekai|litrpg|progression fantasy|cultivation|fantasy|sci-fi|scifi)[^)]*\))\s*$",
    re.IGNORECASE,
)
PUBLISHER_LIKE_RE = re.compile(r"\b(?:press|publishing|books|book group|media|house|studio|audio|audiobooks|editions)\b", re.IGNORECASE)
SOURCE_ARTIFACT_RE = re.compile(r"\b(?:Anna.?s Archive|libgen(?:\.li)?|z-?library|zlib)\b", re.IGNORECASE)
NULLISH_RE = re.compile(r"^(?:null|none|n/?a)(?:\s*,\s*(?:null|none|n/?a|\d{4}))*$", re.IGNORECASE)
QUERY_NOISE_PAREN_RE = re.compile(r"\((?:[^)]*\d{4}[^)]*|[^)]*(?:press|publishing|books)\b[^)]*)\)", re.IGNORECASE)
LEADING_INDEX_TITLE_RE = re.compile(
    rf"^(?:#\s*)?(?:\(\s*)?(?:{VOLUME_INDEX_PATTERN})(?:\s*\))?[.)\s:_-]+\s*(.+)$",
    re.IGNORECASE,
)
LEADING_INDEX_DOTTED_TITLE_RE = re.compile(rf"^({VOLUME_INDEX_PATTERN})[.)\s:_-]+\s*(.+)$", re.IGNORECASE)
GENRE_SUFFIX_RE = re.compile(r"^(.*?)\s*\[([^\[\]]+)\]\s*$")

ONLINE_AMBIGUITY_MARGIN = 25
ONLINE_HTTP_SLOTS = 4
APP_NAME = "ebookRen"
APP_VERSION = "15.0"
GUI_FOOTER_TEXT = "v15. 2026. Piotr Grechuta"
DEFAULT_SOURCE_FOLDER = str(Path.cwd())
DEFAULT_PROVIDERS = "google,openlibrary,crossref,hathitrust,lubimyczytac"
DEFAULT_ONLINE_MODE = "PL"
DEFAULT_AI_MODE = "OFF"
DEFAULT_HTTP_TIMEOUT = 8.0
DEFAULT_INFER_WORKERS = 2
AI_REQUEST_CONFIDENCE_THRESHOLD = 75
AI_AUTO_APPLY_CONFIDENCE = 88
AI_CLI_TIMEOUT_SECONDS = 120
AI_SANDBOX_MODE = "read-only"
AI_ENABLE_WEB_RESEARCH = True
AI_RESEARCH_SOURCES = (
    "strony autorow i wydawcow",
    "OpenLibrary",
    "WorldCat",
    "Fantastic Fiction",
    "Goodreads",
    "Wikipedia",
    "katalogi bibliotek",
    "ksiegarnie i strony ksiazek",
)
ONLINE_ERROR_CACHE_TTL = 60.0
LUBIMYCZYTAC_HOST = "lubimyczytac.pl"
LUBIMYCZYTAC_SEARCH_DELAY_RANGE = (2.4, 5.6)

BLOCKING_REVIEW_REASONS = {
    "online-niejednoznaczne",
    "online-best-effort",
    "nieznany-autor",
    "brak-tytulu",
    "fallback",
    "seria-bez-tomu",
    "szum-w-tytule",
    "artefakt-zrodla",
}

SERIES_SOURCE_PRIORITIES = {
    "opf": 140,
    "title:series-book": 136,
    "title:dotted-series-book": 135,
    "title:paren-series": 132,
    "title:colon-series-index": 130,
    "title:series-index-only": 128,
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
