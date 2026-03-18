from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from dataclasses import asdict, dataclass
from glob import glob
from pathlib import Path
from typing import Any, Callable

import infer_core


AI_MODE_OFF = "OFF"
AI_MODE_REVIEW = "REVIEW"
AI_MODE_ASSIST = "ASSIST"
AI_MODE_AUTO = "AUTO"
AI_MODES = {AI_MODE_OFF, AI_MODE_REVIEW, AI_MODE_ASSIST, AI_MODE_AUTO}

STRUCTURAL_REVIEW_REASONS = {
    "online-niejednoznaczne",
    "online-best-effort",
    "nieznany-autor",
    "brak-tytulu",
    "fallback",
    "seria-bez-tomu",
    "szum-w-tytule",
    "artefakt-zrodla",
    "online-brak-potwierdzenia-autora",
    "online-brak-potwierdzenia-serii",
    "online-brak-potwierdzenia-tomu",
    "online-brak-potwierdzenia-tytulu",
}
STRUCTURAL_SIGNAL_NOTES = {
    "existing-format:author-recovered-from-title",
    "existing-format:trailing-author-reinterpreted",
}


@dataclass(frozen=True)
class AiResolutionRequest:
    path: str
    stem: str
    core: str
    segments: list[str]
    meta_title: str
    meta_creators: list[str]
    meta_series: str
    meta_volume: tuple[int, str] | None
    current_author: str
    current_series: str
    current_volume: tuple[int, str] | None
    current_title: str
    current_source: str
    current_confidence: int
    current_review_reasons: list[str]
    current_decision_reasons: list[str]
    current_notes: list[str]
    signals: list[str]


@dataclass(frozen=True)
class AiResolutionResponse:
    author: str
    series: str
    volume: tuple[int, str] | None
    title: str
    confidence: int
    decision_reasons: list[str]


def normalize_ai_mode(value: str | None) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if normalized in AI_MODES else AI_MODE_OFF


def collect_ai_review_signals(record, meta, *, confidence_threshold: int) -> list[str]:
    signals: list[str] = []
    if getattr(record, "confidence", 0) < confidence_threshold:
        signals.append(f"low-confidence:{getattr(record, 'confidence', 0)}")
    if getattr(record, "needs_review", False):
        signals.append("needs-review")

    for reason in getattr(record, "review_reasons", []):
        if reason in STRUCTURAL_REVIEW_REASONS:
            signals.append(f"review:{reason}")
    for note in getattr(record, "notes", []):
        if note in STRUCTURAL_SIGNAL_NOTES:
            signals.append(f"note:{note}")
    for reason in getattr(record, "decision_reasons", []):
        if reason.startswith("online-verify-") and reason.endswith(":no"):
            signals.append(reason)

    if getattr(record, "author", "") == "Nieznany Autor":
        signals.append("unknown-author")
    if not getattr(record, "title", "") or getattr(record, "title", "") == "Bez tytulu":
        signals.append("missing-title")
    if str(getattr(record, "source", "")).startswith("fallback"):
        signals.append("fallback-source")
    if infer_core.clean(getattr(meta, "title", "")) and infer_core.clean(getattr(record, "title", "")):
        if infer_core.normalize_match_text(meta.title) != infer_core.normalize_match_text(record.title):
            signals.append("meta-title-differs")

    deduped: list[str] = []
    seen: set[str] = set()
    for signal in signals:
        if signal and signal not in seen:
            seen.add(signal)
            deduped.append(signal)
    return deduped


def build_ai_resolution_request(record, meta, signals: list[str]) -> AiResolutionRequest:
    return AiResolutionRequest(
        path=str(getattr(record, "path", "")),
        stem=getattr(meta, "stem", ""),
        core=getattr(meta, "core", ""),
        segments=list(getattr(meta, "segments", [])),
        meta_title=getattr(meta, "title", ""),
        meta_creators=list(getattr(meta, "creators", [])),
        meta_series=getattr(meta, "meta_series", ""),
        meta_volume=getattr(meta, "meta_volume", None),
        current_author=getattr(record, "author", ""),
        current_series=getattr(record, "series", ""),
        current_volume=getattr(record, "volume", None),
        current_title=getattr(record, "title", ""),
        current_source=getattr(record, "source", ""),
        current_confidence=int(getattr(record, "confidence", 0)),
        current_review_reasons=list(getattr(record, "review_reasons", [])),
        current_decision_reasons=list(getattr(record, "decision_reasons", [])),
        current_notes=list(getattr(record, "notes", [])),
        signals=list(signals),
    )


def request_to_payload(request: AiResolutionRequest) -> dict[str, Any]:
    return asdict(request)


def build_ai_resolution_prompt(
    request: AiResolutionRequest,
    *,
    allow_web_research: bool,
    allowed_sources: list[str] | tuple[str, ...],
) -> str:
    payload = json.dumps(request_to_payload(request), ensure_ascii=False, indent=2)
    research_lines = ""
    if allow_web_research:
        preferred_sources = ", ".join(source for source in allowed_sources if infer_core.clean(source))
        research_lines = (
            "- mozesz wykonac dodatkowy research w sieci, gdy lokalne dane nie wystarczaja\n"
            "- nie ograniczaj sie do LubimyCzytac; preferuj: "
            f"{preferred_sources}\n"
            "- przy sprzecznych zrodlach wybierz ostrozny wynik i obniz confidence\n"
            "- jesli uzyjesz researchu webowego, dodaj do decision_reasons tag ai-research:web lub ai-research:<zrodlo>\n"
        )
    return (
        "Rozstrzygasz niejednoznaczne przypadki zmiany nazw ebookow.\n"
        "Masz poprawic tylko pola author, series, volume, title.\n"
        "Zwracaj WYŁĄCZNIE czysty JSON bez markdown.\n"
        "Dozwolony format odpowiedzi:\n"
        '{"author":"...", "series":"...", "volume":[1,"00"] lub null, "title":"...", '
        '"confidence":0-100, "decision_reasons":["..."]}\n'
        "Zasady:\n"
        "- jesli nie ma wiarygodnej serii, ustaw series na \"Standalone\"\n"
        "- nie zgaduj; gdy niepewne, trzymaj sie jak najblizej obecnego wyniku i obniz confidence\n"
        "- volume ma byc null albo [liczba_calkowita, dwucyfrowa_czesc_dziesietna]\n"
        "- author i title nie moga byc puste\n"
        "- decision_reasons maja byc krotkie i techniczne\n"
        f"{research_lines}"
        "Dane wejsciowe JSON:\n"
        f"{payload}"
    )


def _extract_last_agent_message(stdout: str) -> str:
    last_message = ""
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") == "item.completed":
            item = event.get("item") or {}
            if item.get("type") == "agent_message":
                text = (item.get("text") or "").strip()
                if text:
                    last_message = text
    return last_message


def _find_codex_executable() -> str | None:
    manual = os.environ.get("CODEX_EXE")
    if manual and os.path.isfile(manual):
        return manual

    candidates = ("codex.exe", "codex.com", "codex.cmd", "codex.bat") if os.name == "nt" else ("codex",)
    for name in candidates:
        resolved = shutil.which(name)
        if resolved and os.path.isfile(resolved):
            return resolved

    if os.name == "nt":
        root = os.environ.get("USERPROFILE")
        if root:
            patterns = [
                os.path.join(root, ".vscode", "extensions", "openai.chatgpt-*", "bin", "windows-x86_64", "codex.exe"),
                os.path.join(root, ".vscode-insiders", "extensions", "openai.chatgpt-*", "bin", "windows-x86_64", "codex.exe"),
            ]
            matches: list[str] = []
            for pattern in patterns:
                matches.extend(glob(pattern))
            matches = [match for match in matches if os.path.isfile(match)]
            if matches:
                matches.sort(key=os.path.getmtime, reverse=True)
                return matches[0]
    return None


def run_local_codex(
    prompt: str,
    *,
    timeout_seconds: int,
    sandbox_mode: str,
    workdir: Path | None,
) -> str:
    codex_exe = _find_codex_executable()
    if not codex_exe:
        raise RuntimeError("Nie znaleziono lokalnego Codex CLI. Ustaw CODEX_EXE albo dodaj codex do PATH.")

    cmd = [
        codex_exe,
        "--ask-for-approval",
        "never",
        "--sandbox",
        sandbox_mode,
        "exec",
        "--skip-git-repo-check",
        "--json",
        "--",
        prompt,
    ]
    try:
        if os.name == "nt" and os.path.splitext(codex_exe)[1].lower() in {".cmd", ".bat"}:
            cmdline = subprocess.list2cmdline(cmd)
            result = subprocess.run(
                ["cmd.exe", "/d", "/s", "/c", cmdline],
                cwd=str(workdir) if workdir is not None else None,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout_seconds,
            )
        else:
            result = subprocess.run(
                cmd,
                cwd=str(workdir) if workdir is not None else None,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout_seconds,
            )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Lokalny Codex przekroczyl timeout po {timeout_seconds}s.") from exc

    if result.returncode != 0:
        raise RuntimeError(
            f"Codex zakonczyl sie bledem (exit={result.returncode}). "
            f"STDERR={(result.stderr or '').strip() or '(empty)'}"
        )

    answer = _extract_last_agent_message(result.stdout or "")
    return answer or (result.stdout or "").strip() or "(No response)"


def _extract_json_blob(text: str) -> dict[str, Any]:
    cleaned = (text or "").strip()
    if not cleaned:
        raise ValueError("Pusta odpowiedz AI.")
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.IGNORECASE | re.DOTALL).strip()
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("Odpowiedz AI nie zawiera obiektu JSON.")
    parsed = json.loads(cleaned[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("Odpowiedz AI nie jest obiektem JSON.")
    return parsed


def _normalize_volume(value: Any) -> tuple[int, str] | None:
    if value is None or value == "" or value == []:
        return None
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        whole = int(value[0])
        fraction = str(value[1] if len(value) > 1 else "00").strip() or "00"
        if not fraction.isdigit():
            raise ValueError("Niepoprawny format volume[1].")
        return whole, fraction.zfill(2)[:2]
    parsed = infer_core.parse_volume_parts(str(value))
    if parsed is None:
        raise ValueError("Niepoprawny format volume.")
    return parsed


def parse_ai_resolution_response(text: str) -> AiResolutionResponse:
    payload = _extract_json_blob(text)
    author = infer_core.clean(payload.get("author"))
    series = infer_core.clean_series(payload.get("series")) or "Standalone"
    volume = _normalize_volume(payload.get("volume"))
    title = infer_core.clean(payload.get("title"))
    try:
        confidence = int(payload.get("confidence", 0))
    except (TypeError, ValueError) as exc:
        raise ValueError("Niepoprawne pole confidence.") from exc
    confidence = max(0, min(100, confidence))
    raw_reasons = payload.get("decision_reasons") or []
    if not isinstance(raw_reasons, list):
        raise ValueError("decision_reasons musi byc lista.")
    decision_reasons = [infer_core.clean(reason) for reason in raw_reasons if infer_core.clean(reason)]
    return AiResolutionResponse(
        author=author,
        series=series,
        volume=volume,
        title=title,
        confidence=confidence,
        decision_reasons=decision_reasons,
    )


def validate_ai_resolution(response: AiResolutionResponse) -> None:
    if not response.author or not any(character.isalpha() for character in response.author):
        raise ValueError("AI zwrocilo niepoprawnego autora.")
    if not response.title or not any(character.isalpha() for character in response.title):
        raise ValueError("AI zwrocilo niepoprawny tytul.")
    if response.series != "Standalone" and not any(character.isalpha() for character in response.series):
        raise ValueError("AI zwrocilo niepoprawna serie.")
    if infer_core.author_key(response.author) == infer_core.author_key(response.title):
        raise ValueError("AI zwrocilo tytul identyczny z autorem.")


def apply_ai_resolution(record, response: AiResolutionResponse, *, make_record_clone) -> object:
    review_reasons = list(getattr(record, "review_reasons", []))
    if response.author != "Nieznany Autor":
        review_reasons = [reason for reason in review_reasons if reason != "nieznany-autor"]
    if response.title and response.title != "Bez tytulu":
        review_reasons = [reason for reason in review_reasons if reason != "brak-tytulu"]
    if str(getattr(record, "source", "")).startswith("fallback"):
        review_reasons = [reason for reason in review_reasons if reason != "fallback"]

    notes = list(getattr(record, "notes", [])) + ["ai-local:applied"]
    decision_reasons = list(getattr(record, "decision_reasons", []))
    decision_reasons.extend(reason for reason in response.decision_reasons if reason not in decision_reasons)
    if "ai-local:auto-applied" not in decision_reasons:
        decision_reasons.append("ai-local:auto-applied")

    return make_record_clone(
        record,
        author=response.author,
        series=response.series,
        volume=response.volume,
        title=response.title,
        source=f"{getattr(record, 'source', '')}+ai-local",
        notes=notes,
        confidence=response.confidence,
        review_reasons=review_reasons,
        decision_reasons=decision_reasons,
    )


def resolve_record_with_ai(
    record,
    meta,
    *,
    mode: str,
    make_record_clone,
    request_confidence_threshold: int,
    auto_apply_confidence: int,
    timeout_seconds: int,
    sandbox_mode: str,
    allow_web_research: bool,
    allowed_sources: list[str] | tuple[str, ...],
    workdir: Path | None,
    run_prompt_fn: Callable[..., str] = run_local_codex,
) -> tuple[object, dict[str, Any] | None]:
    normalized_mode = normalize_ai_mode(mode)
    signals = collect_ai_review_signals(record, meta, confidence_threshold=request_confidence_threshold)
    if normalized_mode == AI_MODE_OFF or not signals:
        return record, None

    request = build_ai_resolution_request(record, meta, signals)
    log_entry: dict[str, Any] = {
        "path": str(getattr(record, "path", "")),
        "mode": normalized_mode,
        "status": "queued",
        "signals": signals,
        "request": request_to_payload(request),
    }
    if normalized_mode == AI_MODE_REVIEW:
        return record, log_entry

    prompt = build_ai_resolution_prompt(
        request,
        allow_web_research=allow_web_research,
        allowed_sources=allowed_sources,
    )
    try:
        raw_response = run_prompt_fn(
            prompt,
            timeout_seconds=timeout_seconds,
            sandbox_mode=sandbox_mode,
            workdir=workdir,
        )
        log_entry["response_text"] = raw_response
        response = parse_ai_resolution_response(raw_response)
        validate_ai_resolution(response)
    except Exception as exc:
        log_entry["status"] = "error"
        log_entry["error"] = str(exc)
        return record, log_entry

    log_entry["resolution"] = asdict(response)
    if normalized_mode == AI_MODE_ASSIST:
        log_entry["status"] = "suggested"
        return record, log_entry
    if response.confidence < auto_apply_confidence:
        log_entry["status"] = "below-threshold"
        return record, log_entry

    applied_record = apply_ai_resolution(record, response, make_record_clone=make_record_clone)
    log_entry["status"] = "applied"
    return applied_record, log_entry
