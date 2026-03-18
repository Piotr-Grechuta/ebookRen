from __future__ import annotations

from dataclasses import MISSING, dataclass, field, replace
from pathlib import Path
import unittest
from unittest import mock

import ai_resolver
from models_core import EpubMetadata


@dataclass
class DummyRecord:
    path: Path
    author: str
    series: str
    volume: tuple[int, str] | None
    title: str
    source: str
    identifiers: list[str]
    notes: list[str]
    genre: str = ""
    confidence: int = 0
    review_reasons: list[str] = field(default_factory=list)
    decision_reasons: list[str] = field(default_factory=list)

    @property
    def needs_review(self) -> bool:
        return self.confidence < 65 or bool(self.review_reasons)


def clone_record(
    record: DummyRecord,
    *,
    author: str | None = None,
    series: str | None = None,
    volume: tuple[int, str] | None | object = MISSING,
    title: str | None = None,
    source: str | None = None,
    genre: str | None = None,
    notes: list[str] | None = None,
    confidence: int | None = None,
    review_reasons: list[str] | None = None,
    decision_reasons: list[str] | None = None,
):
    return replace(
        record,
        author=record.author if author is None else author,
        series=record.series if series is None else series,
        volume=record.volume if volume is MISSING else volume,
        title=record.title if title is None else title,
        source=record.source if source is None else source,
        genre=record.genre if genre is None else genre,
        notes=list(record.notes if notes is None else notes),
        confidence=record.confidence if confidence is None else confidence,
        review_reasons=list(record.review_reasons if review_reasons is None else review_reasons),
        decision_reasons=list(record.decision_reasons if decision_reasons is None else decision_reasons),
    )


class AiResolverTests(unittest.TestCase):
    def make_meta(self, path: str = "source.epub") -> EpubMetadata:
        return EpubMetadata(
            path=Path(path),
            stem=Path(path).stem,
            segments=["AN 03", "Nie ma takiego miasta", "Tom 00.00", "Konatkowski Tomasz"],
            core="Nie ma takiego miasta - Tom 00.00 - Konatkowski Tomasz",
            title="Nie ma takiego miasta",
            creators=["Tomasz Konatkowski"],
        )

    def test_normalize_ai_mode_falls_back_to_off(self) -> None:
        self.assertEqual(ai_resolver.normalize_ai_mode("assist"), "ASSIST")
        self.assertEqual(ai_resolver.normalize_ai_mode("  "), "OFF")
        self.assertEqual(ai_resolver.normalize_ai_mode("weird"), "OFF")

    def test_collect_ai_review_signals_aggregates_structural_red_flags(self) -> None:
        record = DummyRecord(
            path=Path("source.epub"),
            author="Nieznany Autor",
            series="Standalone",
            volume=None,
            title="Konatkowski Tomasz",
            source="fallback:existing-format",
            identifiers=[],
            notes=["existing-format:trailing-author-reinterpreted"],
            confidence=52,
            review_reasons=["fallback", "nieznany-autor"],
            decision_reasons=["online-verify-title:no"],
        )

        signals = ai_resolver.collect_ai_review_signals(record, self.make_meta(), confidence_threshold=75)

        self.assertIn("low-confidence:52", signals)
        self.assertIn("needs-review", signals)
        self.assertIn("review:fallback", signals)
        self.assertIn("review:nieznany-autor", signals)
        self.assertIn("note:existing-format:trailing-author-reinterpreted", signals)
        self.assertIn("online-verify-title:no", signals)
        self.assertIn("unknown-author", signals)
        self.assertIn("fallback-source", signals)

    def test_parse_ai_resolution_response_normalizes_volume_and_series(self) -> None:
        response = ai_resolver.parse_ai_resolution_response(
            '{"author":"Tomasz Konatkowski","series":"","volume":[15,"5"],'
            '"title":"Gryf w chwale","confidence":93,"decision_reasons":["ai:test"]}'
        )

        self.assertEqual(response.author, "Tomasz Konatkowski")
        self.assertEqual(response.series, "Standalone")
        self.assertEqual(response.volume, (15, "05"))
        self.assertEqual(response.title, "Gryf w chwale")
        self.assertEqual(response.confidence, 93)
        self.assertEqual(response.decision_reasons, ["ai:test"])

    def test_build_ai_resolution_prompt_allows_web_research_with_preferred_sources(self) -> None:
        request = ai_resolver.build_ai_resolution_request(
            DummyRecord(
                path=Path("source.epub"),
                author="Norton Andre",
                series="Świat Czarownic",
                volume=None,
                title="Mądrość Świata Czarownic",
                source="lubimyczytac",
                identifiers=[],
                notes=[],
                confidence=60,
                review_reasons=["seria-bez-tomu"],
            ),
            self.make_meta(),
            ["review:seria-bez-tomu"],
        )

        prompt = ai_resolver.build_ai_resolution_prompt(
            request,
            allow_web_research=True,
            allowed_sources=("OpenLibrary", "WorldCat", "Wikipedia"),
        )

        self.assertIn("dodatkowy research w sieci", prompt)
        self.assertIn("nie ograniczaj sie do LubimyCzytac", prompt)
        self.assertIn("OpenLibrary, WorldCat, Wikipedia", prompt)
        self.assertIn("ai-research:web", prompt)

    def test_collect_ai_review_signals_includes_series_without_volume(self) -> None:
        record = DummyRecord(
            path=Path("source.epub"),
            author="Norton Andre",
            series="Świat Czarownic",
            volume=None,
            title="Mądrość Świata Czarownic",
            source="lubimyczytac",
            identifiers=[],
            notes=[],
            confidence=60,
            review_reasons=["seria-bez-tomu"],
        )

        signals = ai_resolver.collect_ai_review_signals(record, self.make_meta(), confidence_threshold=75)

        self.assertIn("review:seria-bez-tomu", signals)

    def test_resolve_record_with_ai_review_mode_only_queues_case(self) -> None:
        record = DummyRecord(
            path=Path("source.epub"),
            author="Nieznany Autor",
            series="Standalone",
            volume=None,
            title="Bez tytulu",
            source="fallback",
            identifiers=[],
            notes=[],
            confidence=40,
            review_reasons=["fallback", "nieznany-autor", "brak-tytulu"],
        )

        run_prompt = mock.Mock(side_effect=AssertionError("prompt should not run in REVIEW mode"))

        resolved, log_entry = ai_resolver.resolve_record_with_ai(
            record,
            self.make_meta(),
            mode="REVIEW",
            make_record_clone=clone_record,
            request_confidence_threshold=75,
            auto_apply_confidence=88,
            timeout_seconds=1,
            sandbox_mode="read-only",
            allow_web_research=True,
            allowed_sources=("OpenLibrary", "WorldCat"),
            workdir=None,
            run_prompt_fn=run_prompt,
        )

        self.assertIs(resolved, record)
        self.assertIsNotNone(log_entry)
        assert log_entry is not None
        self.assertEqual(log_entry["status"], "queued")
        self.assertEqual(log_entry["mode"], "REVIEW")
        run_prompt.assert_not_called()

    def test_resolve_record_with_ai_assist_mode_returns_suggestion_without_mutating_record(self) -> None:
        record = DummyRecord(
            path=Path("source.epub"),
            author="Nieznany Autor",
            series="Standalone",
            volume=None,
            title="Nie ma takiego miasta",
            source="fallback",
            identifiers=[],
            notes=[],
            confidence=50,
            review_reasons=["fallback"],
        )

        resolved, log_entry = ai_resolver.resolve_record_with_ai(
            record,
            self.make_meta(),
            mode="ASSIST",
            make_record_clone=clone_record,
            request_confidence_threshold=75,
            auto_apply_confidence=88,
            timeout_seconds=1,
            sandbox_mode="read-only",
            allow_web_research=True,
            allowed_sources=("OpenLibrary", "WorldCat"),
            workdir=None,
            run_prompt_fn=lambda _prompt, **kwargs: (
                '{"author":"Tomasz Konatkowski","series":"Standalone","volume":null,'
                '"title":"Nie ma takiego miasta","confidence":94,"decision_reasons":["ai:suggested"]}'
            ),
        )

        self.assertIs(resolved, record)
        self.assertIsNotNone(log_entry)
        assert log_entry is not None
        self.assertEqual(log_entry["status"], "suggested")
        self.assertEqual(log_entry["resolution"]["author"], "Tomasz Konatkowski")
        self.assertEqual(record.author, "Nieznany Autor")

    def test_resolve_record_with_ai_auto_mode_applies_high_confidence_result(self) -> None:
        record = DummyRecord(
            path=Path("source.epub"),
            author="Nieznany Autor",
            series="Standalone",
            volume=None,
            title="Bez tytulu",
            source="fallback",
            identifiers=[],
            notes=[],
            confidence=42,
            review_reasons=["fallback", "nieznany-autor", "brak-tytulu"],
        )

        resolved, log_entry = ai_resolver.resolve_record_with_ai(
            record,
            self.make_meta(),
            mode="AUTO",
            make_record_clone=clone_record,
            request_confidence_threshold=75,
            auto_apply_confidence=88,
            timeout_seconds=1,
            sandbox_mode="read-only",
            allow_web_research=True,
            allowed_sources=("OpenLibrary", "WorldCat"),
            workdir=None,
            run_prompt_fn=lambda _prompt, **kwargs: (
                '{"author":"Tomasz Konatkowski","series":"Standalone","volume":null,'
                '"title":"Nie ma takiego miasta","confidence":95,"decision_reasons":["ai:known-author-tail","ai-research:web"]}'
            ),
        )

        self.assertIsNot(resolved, record)
        self.assertIsNotNone(log_entry)
        assert log_entry is not None
        self.assertEqual(log_entry["status"], "applied")
        self.assertEqual(resolved.author, "Tomasz Konatkowski")
        self.assertEqual(resolved.title, "Nie ma takiego miasta")
        self.assertEqual(resolved.source, "fallback+ai-local")
        self.assertEqual(resolved.confidence, 95)
        self.assertIn("ai-local:applied", resolved.notes)
        self.assertIn("ai-local:auto-applied", resolved.decision_reasons)
        self.assertIn("ai-research:web", resolved.decision_reasons)
        self.assertNotIn("nieznany-autor", resolved.review_reasons)
        self.assertNotIn("brak-tytulu", resolved.review_reasons)
        self.assertNotIn("fallback", resolved.review_reasons)

    def test_resolve_record_with_ai_auto_mode_keeps_record_when_confidence_is_too_low(self) -> None:
        record = DummyRecord(
            path=Path("source.epub"),
            author="Nieznany Autor",
            series="Standalone",
            volume=None,
            title="Nie ma takiego miasta",
            source="fallback",
            identifiers=[],
            notes=[],
            confidence=50,
            review_reasons=["fallback"],
        )

        resolved, log_entry = ai_resolver.resolve_record_with_ai(
            record,
            self.make_meta(),
            mode="AUTO",
            make_record_clone=clone_record,
            request_confidence_threshold=75,
            auto_apply_confidence=88,
            timeout_seconds=1,
            sandbox_mode="read-only",
            allow_web_research=True,
            allowed_sources=("OpenLibrary", "WorldCat"),
            workdir=None,
            run_prompt_fn=lambda _prompt, **kwargs: (
                '{"author":"Tomasz Konatkowski","series":"Standalone","volume":null,'
                '"title":"Nie ma takiego miasta","confidence":70,"decision_reasons":["ai:uncertain"]}'
            ),
        )

        self.assertIs(resolved, record)
        self.assertIsNotNone(log_entry)
        assert log_entry is not None
        self.assertEqual(log_entry["status"], "below-threshold")


if __name__ == "__main__":
    unittest.main()
