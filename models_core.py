from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


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
    subjects: list[str] = field(default_factory=list)
    meta_series: str = ""
    meta_volume: tuple[int, str] | None = None
    errors: list[str] = field(default_factory=list)


@dataclass
class RenameMove:
    source: Path
    temp: Path | None
    destination: Path
    record: object | None = None
    operation: str = "rename"


@dataclass
class UndoPlan:
    folder: Path
    moves: list[RenameMove]
    total_rows: int


@dataclass
class InferenceResult:
    record: object
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
    series: str = ""
    volume: tuple[int, str] | None = None
    genre: str = ""


@dataclass(frozen=True)
class OnlineCandidate:
    provider: str
    source: str
    title: str
    authors: list[str]
    identifiers: list[str]
    score: int
    reason: str
    series: str = ""
    volume: tuple[int, str] | None = None
    genre: str = ""


@dataclass(frozen=True)
class LubimyczytacResult:
    title: str
    authors: list[str]
    series: str = ""
    volume: tuple[int, str] | None = None
    url: str = ""
    genres: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class OnlineVerification:
    checked: bool
    author_confirmed: bool
    title_confirmed: bool
    series_confirmed: bool
    volume_confirmed: bool
    providers: list[str]


@dataclass(frozen=True)
class OnlineRoleEvidence:
    authors: dict[str, str]
    titles: dict[str, str]
    series: dict[str, str]
    volumes: set[tuple[int, str]]


@dataclass(frozen=True)
class HybridLocalParse:
    title_hint: str = ""
    author_hint: str = ""
    volume_hint: tuple[int, str] | None = None
    source: str = ""
    confidence: int = 0


@dataclass(frozen=True)
class LocalPrototype:
    path: Path
    author: str
    series: str
    volume: tuple[int, str] | None
    title: str
    genre: str
    source: str
    confidence: int
    title_from_core: bool = False
    author_from_trailing_core: bool = False
