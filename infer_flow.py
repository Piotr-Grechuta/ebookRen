from __future__ import annotations

import re
from typing import Any, Callable, Iterable, TypeAlias

from models_core import Candidate, EpubMetadata, OnlineCandidate, OnlineRoleEvidence, OnlineVerification

RecordLike: TypeAlias = Any
SplitAuthors: TypeAlias = Callable[[str], list[str]]
AuthorMatchKeys: TypeAlias = Callable[[list[str]], set[str]]
SimilarityScore: TypeAlias = Callable[[str | None, str | None], float]


def is_strong_online_candidate(candidate: object, *, is_online_candidate, clean_series: Callable[[str | None], str]) -> bool:
    if not is_online_candidate(candidate):
        return False
    if candidate.score < 150 or "ambiguous" in candidate.reason:
        return False
    if candidate.provider != "lubimyczytac":
        return "approx" not in candidate.reason
    return bool(
        candidate.score >= 210
        and (
            clean_series(candidate.series)
            or candidate.volume is not None
            or candidate.reason in {"isbn-exact", "title-author-exact", "title-exact", "title-author-approx"}
        )
    )


def register_online_role_text(
    bucket: dict[str, str],
    text: str | None,
    *,
    clean: Callable[[str | None], str],
    clean_author_segment: Callable[[str | None], str],
    author_key: Callable[[str], str],
    normalize_match_text: Callable[[str | None], str],
    author_role: bool = False,
) -> None:
    label = clean_author_segment(text) if author_role else clean(text)
    if not label:
        return
    key = author_key(label) if author_role else normalize_match_text(label)
    if key and key not in bucket:
        bucket[key] = label


def collect_online_candidate_candidates(
    candidate: OnlineCandidate,
    *,
    add_candidate,
    collect_title_candidates,
    collect_core_candidates,
) -> list[Candidate]:
    parsed_candidates: list[Candidate] = []
    if candidate.series:
        add_candidate(
            parsed_candidates,
            candidate.series,
            candidate.volume,
            max(96, candidate.score),
            f"online:{candidate.provider}",
            candidate.title or None,
        )
    collect_title_candidates(candidate.title, parsed_candidates)
    collect_core_candidates(candidate.title, parsed_candidates)
    return parsed_candidates


def collect_online_role_evidence(
    candidates: list[OnlineCandidate],
    *,
    is_strong_online_candidate,
    canonicalize_authors,
    register_online_role_text_fn,
    collect_online_candidate_candidates,
    choose_series_candidate,
    choose_title_candidate,
) -> OnlineRoleEvidence:
    authors: dict[str, str] = {}
    titles: dict[str, str] = {}
    series: dict[str, str] = {}
    volumes: set[tuple[int, str]] = set()

    for candidate in candidates:
        if not is_strong_online_candidate(candidate):
            continue
        for author in canonicalize_authors(candidate.authors):
            register_online_role_text_fn(authors, author, author_role=True)
        register_online_role_text_fn(titles, candidate.title)

        parsed_candidates = collect_online_candidate_candidates(candidate)
        best_series = choose_series_candidate(parsed_candidates)
        best_title = choose_title_candidate(parsed_candidates)
        if best_series is not None:
            register_online_role_text_fn(series, best_series.series)
            if best_series.volume is not None:
                volumes.add(best_series.volume)
        if best_title is not None and best_title.title_override:
            register_online_role_text_fn(titles, best_title.title_override)

    return OnlineRoleEvidence(authors, titles, series, volumes)


def best_matching_online_text(
    values: Iterable[str],
    evidence: dict[str, str],
    *,
    clean: Callable[[str | None], str],
    clean_author_segment: Callable[[str | None], str],
    author_match_keys: AuthorMatchKeys,
    normalize_match_text,
    similarity_score: SimilarityScore,
    author_role: bool = False,
    threshold: float = 0.92,
) -> str | None:
    best_label = ""
    best_score = 0.0
    for value in values:
        label = clean_author_segment(value) if author_role else clean(value)
        if not label:
            continue
        if author_role:
            value_keys = author_match_keys([label])
            for evidence_label in evidence.values():
                if value_keys.intersection(author_match_keys([evidence_label])):
                    return evidence_label
        else:
            key = normalize_match_text(label)
            if key and key in evidence:
                return evidence[key]
        for evidence_label in evidence.values():
            score = similarity_score(label, evidence_label)
            if score >= threshold and score > best_score:
                best_label = evidence_label
                best_score = score
    return best_label or None


def expected_author_match_keys(
    record: RecordLike,
    meta: EpubMetadata,
    *,
    split_authors: SplitAuthors,
    author_match_keys: AuthorMatchKeys,
    extract_trailing_author_from_core: Callable[[str], str],
) -> set[str]:
    fragments: list[str] = []
    if record.author and record.author != "Nieznany Autor":
        fragments.extend(split_authors(record.author))
    for creator in meta.creators:
        fragments.extend(split_authors(creator))
    trailing = extract_trailing_author_from_core(meta.core)
    if trailing:
        fragments.extend(split_authors(trailing))
    return author_match_keys(fragments)


def online_candidate_matches_expected_author(
    record,
    meta,
    candidate,
    *,
    split_authors: SplitAuthors,
    author_match_keys: AuthorMatchKeys,
    similarity_score: SimilarityScore,
    extract_trailing_author_from_core: Callable[[str], str],
) -> bool:
    expected_keys = expected_author_match_keys(
        record,
        meta,
        split_authors=split_authors,
        author_match_keys=author_match_keys,
        extract_trailing_author_from_core=extract_trailing_author_from_core,
    )
    candidate_keys = author_match_keys(candidate.authors)
    if not expected_keys:
        return False
    if expected_keys.intersection(candidate_keys):
        return True
    for expected_key in expected_keys:
        for candidate_key in candidate_keys:
            if similarity_score(expected_key, candidate_key) >= 0.92:
                return True
    return False


def online_candidate_matches_expected_title(
    record,
    meta,
    candidate,
    *,
    strip_leading_title_index: Callable[[str], str],
    sanitize_title,
    clean: Callable[[str | None], str],
    collect_online_candidate_candidates,
    normalize_match_text,
    similarity_score: SimilarityScore,
) -> bool:
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


def online_candidate_supports_record_context(
    record,
    meta,
    candidate,
    *,
    expected_author_match_keys_fn,
    online_candidate_matches_expected_author_fn,
    online_candidate_matches_expected_title_fn,
) -> bool:
    if expected_author_match_keys_fn(record, meta):
        return online_candidate_matches_expected_author_fn(record, meta, candidate)
    return online_candidate_matches_expected_title_fn(record, meta, candidate)


def verify_record_against_online(
    record,
    meta,
    candidates,
    *,
    is_online_candidate,
    author_match_keys,
    split_authors,
    normalize_match_text,
    similarity_score,
    collect_online_candidate_candidates,
    online_candidate_supports_record_context_fn,
    verification_type,
) -> OnlineVerification:
    if not candidates:
        return verification_type(False, False, False, False, False, [])

    providers: list[str] = []
    author_confirmed = False
    title_confirmed = False
    series_confirmed = False
    volume_confirmed = False
    record_author_keys = author_match_keys(split_authors(record.author))
    record_title_key = normalize_match_text(record.title)
    record_series_key = normalize_match_text(record.series) if record.series and record.series != "Standalone" else ""
    record_volume_known = record.volume is not None and record.volume != (0, "00")

    for online_candidate in candidates:
        if not is_online_candidate(online_candidate):
            continue
        if online_candidate.provider not in providers:
            providers.append(online_candidate.provider)
        candidate_supports_context = online_candidate_supports_record_context_fn(record, meta, online_candidate)
        if record_author_keys and record_author_keys.intersection(author_match_keys(online_candidate.authors)):
            author_confirmed = True

        candidate_title_key = normalize_match_text(online_candidate.title)
        if record_title_key and (
            candidate_title_key == record_title_key or similarity_score(record.title, online_candidate.title) >= 0.9
        ):
            title_confirmed = True

        parsed_candidates = collect_online_candidate_candidates(online_candidate)
        for parsed in parsed_candidates:
            if not title_confirmed and parsed.title_override:
                parsed_title_key = normalize_match_text(parsed.title_override)
                if parsed_title_key == record_title_key or similarity_score(record.title, parsed.title_override) >= 0.9:
                    title_confirmed = True
            if candidate_supports_context and not series_confirmed and record_series_key and normalize_match_text(parsed.series) == record_series_key:
                series_confirmed = True
            if candidate_supports_context and not volume_confirmed and record_volume_known and parsed.volume == record.volume:
                volume_confirmed = True

        if candidate_supports_context and not series_confirmed and record_series_key and normalize_match_text(online_candidate.series) == record_series_key:
            series_confirmed = True
        if candidate_supports_context and not volume_confirmed and record_volume_known and online_candidate.volume == record.volume:
            volume_confirmed = True
        if author_confirmed and title_confirmed and series_confirmed and volume_confirmed:
            break

    return verification_type(True, author_confirmed, title_confirmed, series_confirmed, volume_confirmed, providers)


def clear_strong_lubimyczytac_review(record, verification) -> None:
    if not {"online-best-effort", "online-niejednoznaczne"}.intersection(record.review_reasons):
        return
    if "lubimyczytac" not in verification.providers:
        return
    if not all(
        (
            verification.author_confirmed,
            verification.title_confirmed,
            verification.series_confirmed,
            verification.volume_confirmed,
        )
    ):
        return
    record.review_reasons = [
        reason
        for reason in record.review_reasons
        if reason not in {"online-best-effort", "online-niejednoznaczne"}
    ]


def validate_record_components_with_online(
    record,
    meta,
    local_candidates,
    online_candidates,
    verification,
    *,
    collect_online_role_evidence,
    best_matching_online_text,
    is_online_candidate,
    online_candidate_supports_record_context_fn,
    series_candidate_priority,
    clean_series,
    is_strong_online_candidate,
    strip_leading_title_index,
    sanitize_title,
    clean,
    verification_type,
    extract_trailing_author_from_core,
) -> OnlineVerification:
    initial_evidence = collect_online_role_evidence(online_candidates)
    if not any([initial_evidence.authors, initial_evidence.titles, initial_evidence.series, initial_evidence.volumes]):
        return verification

    author_confirmed = verification.author_confirmed
    title_confirmed = verification.title_confirmed
    series_confirmed = verification.series_confirmed
    volume_confirmed = verification.volume_confirmed

    author_fragments: list[str] = [record.author]
    author_fragments.extend(meta.creators)
    author_fragments.append(extract_trailing_author_from_core(meta.core))
    author_fragments.extend(part for part in re.split(r"\s+-\s+", meta.core) if part)

    matched_author = None if author_confirmed else best_matching_online_text(author_fragments, initial_evidence.authors, author_role=True, threshold=0.9)
    if matched_author:
        canonical_author = matched_author
        if canonical_author and canonical_author != record.author:
            record.author = canonical_author
            record.notes.append("online-role-author:applied")
            record.decision_reasons.append("online-role-author:yes")
            record.online_applied = True
        author_confirmed = True

    supporting_online_candidates = [
        candidate
        for candidate in online_candidates
        if is_online_candidate(candidate) and online_candidate_supports_record_context_fn(record, meta, candidate)
    ]
    evidence = collect_online_role_evidence(supporting_online_candidates)
    if not any([evidence.authors, evidence.titles, evidence.series, evidence.volumes]):
        return verification_type(
            verification.checked,
            author_confirmed,
            title_confirmed,
            series_confirmed,
            volume_confirmed,
            verification.providers,
        )

    if (record.series == "Standalone" or not series_confirmed) and evidence.series:
        series_applied = False
        for candidate in sorted(local_candidates, key=series_candidate_priority, reverse=True):
            matched_series = best_matching_online_text([candidate.series], evidence.series, threshold=0.9)
            if not matched_series:
                continue
            cleaned_series = clean_series(matched_series)
            if cleaned_series and cleaned_series != record.series:
                record.series = cleaned_series
                record.notes.append("online-role-series:applied")
                record.decision_reasons.append("online-role-series:yes")
                record.online_applied = True
            series_confirmed = True
            if not volume_confirmed and candidate.volume in evidence.volumes:
                record.volume = candidate.volume
                volume_confirmed = True
            series_applied = True
            break
        if not series_applied:
            best_series_candidate = next(
                (
                    candidate
                    for candidate in sorted(supporting_online_candidates, key=lambda item: item.score, reverse=True)
                    if is_strong_online_candidate(candidate) and clean_series(candidate.series)
                ),
                None,
            )
            if best_series_candidate is not None:
                cleaned_series = clean_series(best_series_candidate.series)
                if cleaned_series and cleaned_series != record.series:
                    record.series = cleaned_series
                    record.notes.append("online-role-series:applied")
                    record.decision_reasons.append("online-role-series:yes")
                    record.online_applied = True
                series_confirmed = True
                if not volume_confirmed and best_series_candidate.volume is not None:
                    record.volume = best_series_candidate.volume
                    volume_confirmed = True
                    record.notes.append("online-role-volume:applied")
                    record.decision_reasons.append("online-role-volume:yes")
                    record.online_applied = True

    title_needs_cleanup = strip_leading_title_index(record.title) != record.title
    if (not title_confirmed or title_needs_cleanup) and evidence.titles:
        preferred_lubimyczytac_title = next(
            (
                strip_leading_title_index(
                    sanitize_title(candidate.title, record.series, record.volume) or clean(candidate.title)
                )
                for candidate in sorted(supporting_online_candidates, key=lambda item: item.score, reverse=True)
                if candidate.provider == "lubimyczytac" and clean(candidate.title)
            ),
            "",
        )
        if preferred_lubimyczytac_title:
            if preferred_lubimyczytac_title != record.title:
                record.title = preferred_lubimyczytac_title
                record.notes.append("online-role-title:applied")
                record.decision_reasons.append("online-role-title:yes")
                record.online_applied = True
            title_confirmed = True

    if (not title_confirmed or title_needs_cleanup) and evidence.titles:
        local_title_candidates = [candidate.title_override for candidate in local_candidates if candidate.title_override]
        local_title_candidates.extend(
            [
                record.title,
                strip_leading_title_index(record.title),
                meta.title,
                strip_leading_title_index(meta.title),
                sanitize_title(meta.core, record.series, record.volume),
                strip_leading_title_index(sanitize_title(meta.core, record.series, record.volume)),
            ]
        )
        matched_title = best_matching_online_text(local_title_candidates, evidence.titles, threshold=0.88)
        if matched_title:
            cleaned_title = strip_leading_title_index(
                sanitize_title(matched_title, record.series, record.volume) or clean(matched_title)
            )
            if cleaned_title and cleaned_title != record.title:
                record.title = cleaned_title
                record.notes.append("online-role-title:applied")
                record.decision_reasons.append("online-role-title:yes")
                record.online_applied = True
            title_confirmed = True

    if not volume_confirmed and evidence.volumes:
        if record.volume in evidence.volumes:
            volume_confirmed = True
        else:
            for candidate in sorted(local_candidates, key=series_candidate_priority, reverse=True):
                if candidate.volume in evidence.volumes:
                    record.volume = candidate.volume
                    volume_confirmed = True
                    record.notes.append("online-role-volume:applied")
                    record.decision_reasons.append("online-role-volume:yes")
                    record.online_applied = True
                    break
        if not volume_confirmed:
            best_volume_candidate = next(
                (
                    candidate
                    for candidate in sorted(supporting_online_candidates, key=lambda item: item.score, reverse=True)
                    if candidate.volume in evidence.volumes
                    and (not clean_series(candidate.series) or clean_series(candidate.series) == clean_series(record.series))
                ),
                None,
            )
            if best_volume_candidate is not None:
                record.volume = best_volume_candidate.volume
                volume_confirmed = True
                record.notes.append("online-role-volume:applied")
                record.decision_reasons.append("online-role-volume:yes")
                record.online_applied = True

    if not record.genre:
        best_genre = next(
            (
                clean(candidate.genre)
                for candidate in sorted(supporting_online_candidates, key=lambda item: item.score, reverse=True)
                if clean(candidate.genre)
            ),
            "",
        )
        if best_genre:
            record.genre = best_genre
            record.notes.append("online-role-genre:applied")
            record.decision_reasons.append("online-role-genre:yes")
            record.online_applied = True

    return verification_type(
        verification.checked,
        author_confirmed,
        title_confirmed,
        series_confirmed,
        volume_confirmed,
        verification.providers,
    )


def infer_record(
    meta,
    use_online,
    providers,
    timeout,
    *,
    parse_existing_filename,
    book_record_type,
    extract_isbns,
    infer_book_genre,
    existing_format_needs_online_verification,
    finalize_record_quality,
    add_candidate,
    clean_author_segment,
    looks_like_author_segment,
    extract_trailing_author_from_core,
    extract_authors,
    collect_title_candidates,
    collect_core_candidates,
    collect_segment_candidates,
    choose_series_candidate,
    choose_title_candidate,
    sanitize_title,
    strip_leading_title_index,
    strip_author_from_title,
    clean,
    hex_noise_re,
    anna_archive_re,
    clean_series,
    fetch_online_candidates,
    build_online_query_variants,
    pick_best_online_match,
    build_online_record,
    split_authors,
    online_candidate_supports_record_context_fn,
    collect_online_candidate_candidates,
    source_needs_online_verification,
    verify_record_against_online_fn,
    validate_record_components_with_online_fn,
    clear_strong_lubimyczytac_review_fn,
    online_candidate_type,
    emit_stage=None,
    emit_trace=None,
) -> object:
    def trace_state(label: str, current_record=None, extra: list[str] | None = None) -> None:
        if emit_trace is None:
            return
        lines = [f"{meta.path.name}", f"  etap: {label}"]
        lines.append(f"  wejscie: stem={meta.stem}")
        if meta.title:
            lines.append(f"  meta.title: {meta.title}")
        if meta.creators:
            lines.append(f"  meta.creators: {' | '.join(meta.creators)}")
        if meta.meta_series or meta.meta_volume is not None:
            lines.append(f"  meta.series: {meta.meta_series or '(brak)'}")
            lines.append(f"  meta.volume: {meta.meta_volume}")
        if current_record is not None:
            lines.append(f"  author: {current_record.author}")
            lines.append(f"  series: {current_record.series}")
            lines.append(f"  volume: {current_record.volume}")
            lines.append(f"  title: {current_record.title}")
            lines.append(f"  source: {current_record.source}")
        if extra:
            lines.extend(f"  {item}" for item in extra if item)
        emit_trace("\n".join(lines))

    def fetch_online_candidates_with_progress(target_meta, label: str):
        if emit_stage is not None:
            emit_stage("sprawdzenie-online", label)
        try:
            return fetch_online_candidates(
                target_meta,
                providers,
                timeout,
                emit_stage=emit_stage,
                query_label=label,
            )
        except TypeError as exc:
            if "emit_stage" not in str(exc) and "query_label" not in str(exc):
                raise
            return fetch_online_candidates(target_meta, providers, timeout)

    existing = parse_existing_filename(meta.stem)
    candidates: list[Candidate] = []
    author_from_trailing_core = False
    title_from_core = False
    if existing is not None:
        author, series, volume, title, genre = existing
        record = book_record_type(
            path=meta.path,
            author=author,
            series=series or "Standalone",
            volume=volume,
            title=title or "Bez tytulu",
            source="existing-format",
            identifiers=extract_isbns(meta.identifiers),
            notes=list(meta.errors),
            genre=genre or infer_book_genre(meta.subjects),
            confidence=100,
            review_reasons=[],
            decision_reasons=["existing-format"],
        )
        if emit_stage is not None:
            emit_stage("lokalne-dopasowanie", record.source)
        trace_state("lokalne-dopasowanie", record)
        if not (use_online and existing_format_needs_online_verification(record)):
            final_record = finalize_record_quality(record, meta, 100, title_from_core=False)
            if emit_stage is not None:
                emit_stage("nazwa-koncowa", final_record.filename)
            trace_state("nazwa-koncowa", final_record, [f"filename: {final_record.filename}"])
            return final_record
        if record.series and record.series != "Standalone":
            add_candidate(candidates, record.series, record.volume, 100, "existing-format", record.title)
        base_confidence = 100
    else:
        segment_author = ""
        if len(meta.segments) > 1:
            second = clean_author_segment(meta.segments[1])
            if looks_like_author_segment(second):
                segment_author = second
        if not segment_author and not meta.creators:
            segment_author = extract_trailing_author_from_core(meta.core)
            author_from_trailing_core = bool(segment_author)

        author = extract_authors(meta.creators, segment_author)

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
            base_confidence = max(best_series.score, best_title.score if best_title else 0)
        else:
            series = "Standalone"
            volume = None
            title_override = None
            source = "fallback"
            base_confidence = 45

        local_title = sanitize_title(meta.title, series, volume)
        fallback_title = ""
        if not title_override and not local_title:
            fallback_title = strip_leading_title_index(meta.core)
            if segment_author and " - " in fallback_title:
                head, _, tail = fallback_title.rpartition(" - ")
                if clean(tail) == clean(segment_author):
                    fallback_title = clean(head)
        title = title_override or local_title or fallback_title or clean(meta.core)
        title_from_core = not bool(title_override or local_title)
        if local_title and title_override:
            if len(title_override) > len(local_title) + 12 or hex_noise_re.search(title_override) or anna_archive_re.search(title_override):
                title = local_title
                title_from_core = False

        title = strip_author_from_title(title, author)
        title = clean(anna_archive_re.sub("", title))
        title = sanitize_title(title, series, volume)
        notes = list(meta.errors)

        record = book_record_type(
            path=meta.path,
            author=author,
            series=clean_series(series) or "Standalone",
            volume=volume,
            title=title,
            source=source,
            identifiers=extract_isbns(meta.identifiers),
            notes=notes,
            genre=infer_book_genre(meta.subjects),
            confidence=base_confidence,
            review_reasons=[],
            decision_reasons=[f"inference:{source}"],
        )
        if emit_stage is not None:
            emit_stage("lokalne-dopasowanie", record.source)
        trace_state("lokalne-dopasowanie", record)
    online_candidates: list[OnlineCandidate] = []
    if use_online and (
        record.source == "existing-format"
        or record.source == "online-aggregate"
        or record.series == "Standalone"
        or record.volume is None
        or record.volume == (0, "00")
        or record.author == "Nieznany Autor"
        or source_needs_online_verification(record.source)
    ):
        online_candidates = fetch_online_candidates_with_progress(meta, "wariant=oryginal")
        variants = build_online_query_variants(meta, record)
        for variant_index, variant in enumerate(variants, start=1):
            variant_label = variant.title or variant.core or f"wariant-{variant_index}"
            online_candidates.extend(
                fetch_online_candidates_with_progress(
                    variant,
                    f"wariant={variant_index}/{len(variants)}:{variant_label}",
                )
            )
        best_online = pick_best_online_match(meta, online_candidates)
        if emit_stage is not None:
            if best_online is None:
                emit_stage("sprawdzenie-online", "brak wiarygodnego wyniku")
            else:
                emit_stage(
                    "sprawdzenie-online",
                    f"wybrano: {best_online.title or '(brak tytulu)'} | score={best_online.score}",
                )
        online = build_online_record(meta, best_online) if best_online is not None else None
        if online:
            record.online_checked = True
            record.notes.append(f"online-checked:{online.source}")
            record.decision_reasons.append(f"online-checked:{online.source}")
            if online.review_reasons:
                record.review_reasons.extend(online.review_reasons)
            online_context_candidate = online_candidate_type(
                provider="aggregate",
                source=online.source,
                title=online.title,
                authors=split_authors(online.author),
                identifiers=list(online.identifiers),
                score=max(online.confidence, 0),
                reason="aggregate",
                series=online.series,
                volume=online.volume,
                genre=online.genre,
            )
            trace_state(
                "online-kandydat",
                record,
                [
                    f"online.title: {online.title}",
                    f"online.author: {online.author}",
                    f"online.series: {online.series}",
                    f"online.volume: {online.volume}",
                    f"online.genre: {online.genre}",
                ],
            )
            blocked_online = any(reason in {"online-niejednoznaczne", "online-best-effort"} for reason in online.review_reasons)
            if not blocked_online and online_candidate_supports_record_context_fn(record, meta, online_context_candidate):
                online_applied = False
                if record.author == "Nieznany Autor" and online.author != "Nieznany Autor":
                    record.author = online.author
                    online_applied = True
                if not record.genre and online.genre:
                    record.genre = clean(online.genre)
                    online_applied = True
                if (not record.title or hex_noise_re.search(record.title) or anna_archive_re.search(record.title)) and online.title:
                    record.title = online.title
                    title_from_core = False
                    online_applied = True
                if online.title or online.series:
                    online_title_candidates = collect_online_candidate_candidates(
                        online_candidate_type(
                            provider="aggregate",
                            source=online.source,
                            title=online.title,
                            authors=split_authors(online.author),
                            identifiers=list(online.identifiers),
                            score=max(96, online.confidence),
                            reason="aggregate",
                            series=online.series,
                            volume=online.volume,
                            genre=online.genre,
                        )
                    )
                    if online_title_candidates:
                        best_online_series = choose_series_candidate(online_title_candidates)
                        best_online_title = choose_title_candidate(online_title_candidates)
                        if best_online_title and (
                            not record.title
                            or hex_noise_re.search(record.title)
                            or anna_archive_re.search(record.title)
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
                            or record.volume == (0, "00")
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
                    trace_state("online-zastosowany", record)

    if use_online:
        if emit_stage is not None:
            emit_stage("walidacja", record.source)
        verification = verify_record_against_online_fn(record, meta, online_candidates)
        verification = validate_record_components_with_online_fn(record, meta, candidates, online_candidates, verification)
        trace_state(
            "walidacja",
            record,
            [
                f"author_confirmed: {verification.author_confirmed}",
                f"series_confirmed: {verification.series_confirmed}",
                f"volume_confirmed: {verification.volume_confirmed}",
                f"title_confirmed: {verification.title_confirmed}",
            ],
        )
        if verification.checked:
            provider_text = ",".join(verification.providers)
            record.notes.append(f"online-verify:{provider_text}")
            record.decision_reasons.extend(
                [
                    f"online-verify-author:{'yes' if verification.author_confirmed else 'no'}",
                    f"online-verify-series:{'yes' if verification.series_confirmed else 'no'}",
                    f"online-verify-volume:{'yes' if verification.volume_confirmed else 'no'}",
                    f"online-verify-title:{'yes' if verification.title_confirmed else 'no'}",
                ]
            )
            if author_from_trailing_core and not verification.author_confirmed:
                record.author = "Nieznany Autor"
                record.review_reasons.append("online-brak-potwierdzenia-autora")
                base_confidence = min(base_confidence, 52)
            if source_needs_online_verification(record.source):
                if record.series != "Standalone" and not verification.series_confirmed:
                    record.review_reasons.append("online-brak-potwierdzenia-serii")
                    base_confidence = min(base_confidence, 60)
                if record.volume is not None and not verification.volume_confirmed:
                    record.review_reasons.append("online-brak-potwierdzenia-tomu")
                    base_confidence = min(base_confidence, 60)
                if title_from_core and not verification.title_confirmed:
                    record.review_reasons.append("online-brak-potwierdzenia-tytulu")
                    base_confidence = min(base_confidence, 58)
            clear_strong_lubimyczytac_review_fn(record, verification)

    if not record.title:
        fallback_title = sanitize_title(meta.core, record.series, record.volume)
        if fallback_title:
            record.title = fallback_title
            title_from_core = True
        else:
            record.title = "Bez tytulu"
    if not record.series:
        record.series = "Standalone"

    final_record = finalize_record_quality(record, meta, base_confidence, title_from_core)
    if emit_stage is not None:
        emit_stage("nazwa-koncowa", final_record.filename)
    trace_state("nazwa-koncowa", final_record, [f"filename: {final_record.filename}"])
    return final_record
