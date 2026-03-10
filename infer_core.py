from __future__ import annotations

import difflib
import re
import unicodedata
from pathlib import Path
from typing import Iterable


GENRE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "litrpg": ("litrpg", "game lit", "gamelit"),
    "fantasy": ("fantasy", "dark fantasy", "epic fantasy", "heroic fantasy", "urban fantasy", "myth"),
    "sci-fi": ("science fiction", "sci fi", "sci-fi", "sf", "space opera", "cyberpunk", "dystopia", "post apocalyptic"),
    "kryminal": ("crime", "mystery", "detective", "kryminal", "kryminalna", "noir", "police procedural"),
    "thriller": ("thriller", "suspense", "sensacja"),
    "horror": ("horror", "ghost", "supernatural"),
    "romans": ("romance", "romans", "love story", "love stories", "erotica", "new adult"),
    "historyczna": ("historical fiction", "historical", "powiesc historyczna", "fiction historical"),
    "mlodziezowa": ("young adult", "juvenile fiction", "teen", "ya fiction"),
    "biografia": ("biography", "autobiography", "memoir", "biogra", "autobiogra"),
    "literatura-faktu": ("nonfiction", "non-fiction", "reportage", "essay", "history", "self help", "psychology"),
}


def clean(text: str | None) -> str:
    if not text:
        return ""
    text = re.sub(r"([^\W\d_])[-_](\d{1,3})\.(?=[^\W\d_])", r"\1 \2 ", text, flags=re.UNICODE)
    text = re.sub(r"([^\W\d_])[-_](\d{1,3})(?=[-_:])", r"\1 \2 ", text, flags=re.UNICODE)
    text = re.sub(r"(\d{1,3})\.(?=[^\W\d_])", r"\1 ", text, flags=re.UNICODE)
    text = text.replace("_", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text.strip(" .-")


def clean_series(text: str | None) -> str:
    return clean(text).strip(" ,")


def fold_text(text: str | None) -> str:
    normalized = unicodedata.normalize("NFKD", clean(text))
    folded = "".join(char for char in normalized if not unicodedata.combining(char))
    return (
        folded.replace("\u0141", "L")
        .replace("\u0142", "l")
        .replace("\u00d8", "O")
        .replace("\u00f8", "o")
        .replace("\u0110", "D")
        .replace("\u0111", "d")
    )


def normalize_match_text(text: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", fold_text(text).lower()).strip()


def infer_book_genre(labels: Iterable[str]) -> str:
    scores: dict[str, tuple[int, int]] = {}
    for label in labels:
        normalized = normalize_match_text(label)
        if not normalized:
            continue
        for genre, keywords in GENRE_KEYWORDS.items():
            for keyword in keywords:
                keyword_norm = normalize_match_text(keyword)
                if not keyword_norm:
                    continue
                position = normalized.find(keyword_norm)
                if position == -1:
                    continue
                current_score, current_position = scores.get(genre, (0, 10**9))
                scores[genre] = (
                    current_score + max(1, len(keyword_norm.split())),
                    min(current_position, position),
                )
    if not scores:
        return ""
    if "fantasy" in scores and "sci-fi" in scores and scores["fantasy"][1] <= scores["sci-fi"][1]:
        return "fantasy"
    return max(
        scores.items(),
        key=lambda item: (
            item[1][0],
            -item[1][1],
            item[0] != "mlodziezowa",
        ),
    )[0]


def split_title_genre_suffix(title: str, *, genre_suffix_re) -> tuple[str, str]:
    cleaned = clean(title)
    match = genre_suffix_re.match(cleaned)
    if not match:
        return cleaned, ""
    normalized_genre = infer_book_genre([match.group(2)]) or clean(match.group(2))
    if not normalized_genre:
        return cleaned, ""
    return clean(match.group(1)), normalized_genre


def format_title_with_genre(title: str, genre: str, *, genre_suffix_re) -> str:
    title = clean(title)
    genre = clean(genre)
    if not title or not genre:
        return title
    base_title, _ = split_title_genre_suffix(title, genre_suffix_re=genre_suffix_re)
    return clean(f"{base_title} [{genre}]")


def similarity_score(left: str | None, right: str | None) -> float:
    left_norm = normalize_match_text(left)
    right_norm = normalize_match_text(right)
    if not left_norm or not right_norm:
        return 0.0
    return difflib.SequenceMatcher(None, left_norm, right_norm).ratio()


def author_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", fold_text(text).lower())


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
    if total <= 0 or total > 3999:
        return None
    return total


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
    if re.fullmatch(r"[IVXLCDM]", token, flags=re.IGNORECASE):
        return None
    roman_value = roman_to_int(token)
    if roman_value is not None:
        return roman_value, "00"
    return None


def format_volume(volume: tuple[int, str] | None) -> str:
    if volume is None:
        return "Tom 00.00"
    major, minor = volume
    if major == 0 and int(str(minor).zfill(2)) == 0:
        return "Tom 00.00"
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


def sanitize_component(text: str, *, device_names: set[str]) -> str:
    text = clean(text)
    text = re.sub(r'[<>:"/\\|?*]', "-", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    if not text:
        text = "Brak"
    if text.upper() in device_names:
        text = f"_{text}"
    return text


def build_filename_stem(author: str, series: str, volume: str, title: str) -> str:
    parts = [clean(author)]
    series = clean_series(series)
    volume = clean(volume)
    title = clean(title)
    if series:
        parts.append(series)
    if volume:
        parts.append(volume)
    if title:
        parts.append(title)
    return " - ".join(part for part in parts if part)


def trim_title_for_path(folder: Path, author: str, series: str, volume: str, title: str) -> str:
    filename = build_filename_stem(author, series, volume, title)
    budget = 230 - len(str(folder))
    if budget < 80:
        budget = 80
    if len(filename) <= budget:
        return title
    overflow = len(filename) - budget
    trimmed = title[:-overflow].rstrip(" .-")
    return trimmed or "Bez tytulu"


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


def split_authors(text: str, *, clean_author_segment) -> list[str]:
    text = clean_author_segment(text)
    if not text:
        return []
    text = re.sub(r"\[.*?\]", "", text)
    text = text.replace(";", " & ")
    text = re.sub(r"\s+(?:and|i)\s+", " & ", text, flags=re.IGNORECASE)
    if "&" not in text:
        comma_parts = [clean(part) for part in re.split(r"\s*,\s*", text) if clean(part)]
        if len(comma_parts) >= 2 and all(len(part.split()) >= 2 for part in comma_parts):
            return comma_parts
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


def extract_isbns(values: Iterable[str], *, isbn_re) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for value in values:
        for match in isbn_re.findall(value or ""):
            normalized = match.upper()
            if normalized not in seen:
                seen.add(normalized)
                found.append(normalized)
    return found
