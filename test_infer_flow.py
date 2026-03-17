from pathlib import Path
import unittest

import infer_flow
from domain_naming import BookRecord
from models_core import Candidate, EpubMetadata, OnlineCandidate, OnlineVerification


class InferFlowTests(unittest.TestCase):
    def test_clear_strong_lubimyczytac_review_removes_advisory_flags(self) -> None:
        record = BookRecord(
            path=Path("book.epub"),
            author="Victoria Aveyard",
            series="Czerwona Królowa",
            volume=(1, "00"),
            title="Czerwona Królowa",
            source="existing-format",
            identifiers=[],
            notes=[],
            review_reasons=["online-best-effort", "online-niejednoznaczne", "kolizja-nazwy"],
        )
        verification = OnlineVerification(True, True, True, True, True, ["lubimyczytac"])

        infer_flow.clear_strong_lubimyczytac_review(record, verification)

        self.assertEqual(record.review_reasons, ["kolizja-nazwy"])

    def test_expected_author_match_keys_uses_trailing_author_from_core(self) -> None:
        record = BookRecord(
            path=Path("book.epub"),
            author="Nieznany Autor",
            series="Standalone",
            volume=None,
            title="Title",
            source="fallback",
            identifiers=[],
            notes=[],
        )
        meta = EpubMetadata(
            path=Path("book.epub"),
            stem="Title - Victoria Aveyard",
            segments=["Title - Victoria Aveyard"],
            core="Title - Victoria Aveyard",
            creators=[],
        )

        keys = infer_flow.expected_author_match_keys(
            record,
            meta,
            split_authors=lambda text: [item.strip() for item in text.split("&") if item.strip()],
            author_match_keys=lambda values: {"".join(value.lower().split()) for value in values if value},
            extract_trailing_author_from_core=lambda core: core.rsplit(" - ", 1)[-1],
        )

        self.assertIn("victoriaaveyard", keys)

    def test_validate_record_components_preserves_full_multi_author_against_initials(self) -> None:
        record = BookRecord(
            path=Path("book.epub"),
            author="Ludlum Robert & van Lustbader Eric",
            series="Standalone",
            volume=None,
            title="Imperatyw Bourne'a",
            source="hybrid:compact-title-author",
            identifiers=[],
            notes=[],
        )
        meta = EpubMetadata(
            path=Path("book.epub"),
            stem="Imperatyw Bourne'a",
            segments=["Imperatyw Bourne'a"],
            core="Imperatyw Bourne'a",
            creators=[],
        )
        candidate = OnlineCandidate(
            provider="google",
            source="google",
            title="Imperatyw Bourne'a",
            authors=["Bourne'a", "Ludlum R.", "Van Lustbader E."],
            identifiers=[],
            score=220,
            reason="title-author-exact",
        )
        verification = OnlineVerification(True, False, True, False, False, ["google"])

        updated = infer_flow.validate_record_components_with_online(
            record,
            meta,
            [],
            [candidate],
            verification,
            collect_online_role_evidence=lambda candidates: infer_flow.collect_online_role_evidence(
                candidates,
                is_strong_online_candidate=lambda candidate: True,
                canonicalize_authors=lambda authors: authors,
                register_online_role_text_fn=lambda bucket, text, author_role=False: bucket.setdefault((text or "").lower(), text or ""),
                collect_online_candidate_candidates=lambda candidate: [],
                choose_series_candidate=lambda candidates: None,
                choose_title_candidate=lambda candidates: None,
            ),
            best_matching_online_text=lambda fragments, bucket, author_role=False, threshold=0.9: "Bourne'a & Ludlum R. & Van Lustbader E.",
            is_online_candidate=lambda candidate: True,
            online_candidate_supports_record_context_fn=lambda record, meta, candidate: True,
            series_candidate_priority=lambda candidate: (0, 0, 0),
            clean_series=lambda text: text or "",
            is_strong_online_candidate=lambda candidate: True,
            strip_leading_title_index=lambda text: text or "",
            sanitize_title=lambda title, series, volume: title,
            clean=lambda text: text or "",
            clean_author_segment=lambda text: text or "",
            split_authors=lambda text: [item.strip() for item in text.split("&") if item.strip()],
            similarity_score=lambda left, right: 1.0 if left == right else 0.0,
            normalize_match_text=lambda text: (text or "").lower(),
            verification_type=OnlineVerification,
            extract_trailing_author_from_core=lambda core: "",
        )

        self.assertEqual(record.author, "Ludlum Robert & van Lustbader Eric")
        self.assertTrue(updated.author_confirmed)

    def test_validate_record_components_applies_genre_only_from_matching_candidate(self) -> None:
        record = BookRecord(
            path=Path("book.epub"),
            author="Ludlum Robert & van Lustbader Eric",
            series="Jason Bourne",
            volume=(9, "00"),
            title="Świat Bourne'a",
            source="online-aggregate",
            identifiers=[],
            notes=[],
        )
        meta = EpubMetadata(
            path=Path("book.epub"),
            stem="Świat Bourne'a",
            segments=["Świat Bourne'a"],
            core="Świat Bourne'a",
            creators=[],
        )
        good = OnlineCandidate(
            provider="lubimyczytac",
            source="lubimyczytac",
            title="Świat Bourne'a",
            authors=["Eric van Lustbader", "Robert Ludlum"],
            identifiers=[],
            score=300,
            reason="title-author-exact",
            genre="thriller",
        )
        bad = OnlineCandidate(
            provider="google",
            source="google",
            title="Świat magii",
            authors=["Eric van Lustbader", "Robert Ludlum"],
            identifiers=[],
            score=500,
            reason="title-author-exact",
            genre="fantasy",
        )
        verification = OnlineVerification(True, True, True, True, True, ["lubimyczytac", "google"])

        infer_flow.validate_record_components_with_online(
            record,
            meta,
            [],
            [bad, good],
            verification,
            collect_online_role_evidence=lambda candidates: infer_flow.collect_online_role_evidence(
                candidates,
                is_strong_online_candidate=lambda candidate: True,
                canonicalize_authors=lambda authors: authors,
                register_online_role_text_fn=lambda bucket, text, author_role=False: bucket.setdefault((text or "").lower(), text or ""),
                collect_online_candidate_candidates=lambda candidate: [],
                choose_series_candidate=lambda candidates: None,
                choose_title_candidate=lambda candidates: None,
            ),
            best_matching_online_text=lambda fragments, bucket, author_role=False, threshold=0.9: None,
            is_online_candidate=lambda candidate: True,
            online_candidate_supports_record_context_fn=lambda record, meta, candidate: True,
            series_candidate_priority=lambda candidate: (0, 0, 0),
            clean_series=lambda text: text or "",
            is_strong_online_candidate=lambda candidate: True,
            strip_leading_title_index=lambda text: text or "",
            sanitize_title=lambda title, series, volume: title,
            clean=lambda text: text or "",
            clean_author_segment=lambda text: text or "",
            split_authors=lambda text: [item.strip() for item in text.split("&") if item.strip()],
            similarity_score=lambda left, right: 1.0 if left == right else 0.0,
            normalize_match_text=lambda text: (text or "").lower(),
            verification_type=OnlineVerification,
            extract_trailing_author_from_core=lambda core: "",
        )

        self.assertEqual(record.genre, "thriller")

    def test_collect_online_candidate_candidates_ignores_search_only_cycle_hint(self) -> None:
        candidate = OnlineCandidate(
            provider="lubimyczytac",
            source="lubimyczytac",
            title="Reporter",
            authors=["A. J. Quinnell"],
            identifiers=[],
            score=320,
            reason="title-author-exact",
            series="Czarna",
            volume=(8, "00"),
            cycle_source="search",
        )

        parsed = infer_flow.collect_online_candidate_candidates(
            candidate,
            add_candidate=lambda bucket, series, volume, score, source, title_override=None: bucket.append(
                Candidate(score, series, volume, title_override, source)
            ),
            collect_title_candidates=lambda title, bucket: None,
            collect_core_candidates=lambda title, bucket: None,
        )

        self.assertEqual(parsed, [])


if __name__ == "__main__":
    unittest.main()
