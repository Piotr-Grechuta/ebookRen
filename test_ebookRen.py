import csv
import json
import importlib.util
import os
import re
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("app_runtime.py")
SPEC = importlib.util.spec_from_file_location("app_runtime", MODULE_PATH)
kod_v3 = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(kod_v3)

def make_meta(
    stem: str,
    *,
    title: str = "",
    creators: list[str] | None = None,
    identifiers: list[str] | None = None,
    subjects: list[str] | None = None,
):
    segments = [kod_v3.clean(part) for part in re.split(r"\s*--\s*", stem) if kod_v3.clean(part)]
    meta = kod_v3.EpubMetadata(
        path=Path(stem + ".epub"),
        stem=stem,
        segments=segments,
        core=segments[0] if segments else stem,
    )
    meta.title = title
    meta.creators = list(creators or [])
    meta.identifiers = list(identifiers or [])
    meta.subjects = list(subjects or [])
    return meta


class KodV3Tests(unittest.TestCase):
    def test_local_inference_prefers_explicit_series_marker_over_weak_core_series(self) -> None:
        stem = (
            "Completionist Chronicles 01_ Bibliomancer -- Hunter, James & Krout, Dakota -- "
            "Wolfman Warlock #1, 2019 -- Mountaindale Press -- 68b670c535b8be0930e8d69b0f45fe89 -- Anna’s Archive"
        )
        meta = make_meta(stem)
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.series, "Wolfman Warlock")
        self.assertEqual(record.title, "Bibliomancer")
        self.assertEqual(record.filename, "Hunter James & Krout Dakota - Wolfman Warlock - Tom 01.00 - Bibliomancer.epub")

    def test_collect_title_candidates_parses_colon_series_book_format(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.collect_title_candidates("Bibliomancer: Wolfman Warlock, Book 1", candidates)
        self.assertTrue(candidates)
        best = kod_v3.choose_series_candidate(candidates)
        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best.series, "Wolfman Warlock")
        self.assertEqual(best.volume, (1, "00"))

    def test_collect_title_candidates_parses_roman_volume(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.collect_title_candidates("Chronicle: Cykl, Tom IV", candidates)
        best = kod_v3.choose_series_candidate(candidates)
        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best.volume, (4, "00"))

    def test_collect_title_candidates_parses_trailing_series_book_format(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.collect_title_candidates("Past's Price The Resonance Cycle, Book 3 [Isekai, LitRPG]", candidates)
        best_series = kod_v3.choose_series_candidate(candidates)
        best_title = kod_v3.choose_title_candidate(candidates)
        self.assertIsNotNone(best_series)
        self.assertIsNotNone(best_title)
        assert best_series is not None
        assert best_title is not None
        self.assertEqual(best_series.series, "The Resonance Cycle")
        self.assertEqual(best_series.volume, (3, "00"))
        self.assertEqual(best_title.title_override, "Past's Price")

    def test_collect_title_candidates_parses_double_colon_book_format(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.collect_title_candidates("The Eldritch Artisan: Father of Constructs: Book 3 (LitRPG)", candidates)
        best_series = kod_v3.choose_series_candidate(candidates)
        best_title = kod_v3.choose_title_candidate(candidates)
        self.assertIsNotNone(best_series)
        self.assertIsNotNone(best_title)
        assert best_series is not None
        assert best_title is not None
        self.assertEqual(best_series.series, "Father of Constructs")
        self.assertEqual(best_series.volume, (3, "00"))
        self.assertEqual(best_title.title_override, "The Eldritch Artisan")

    def test_collect_title_candidates_parses_polish_dotted_series_book_format(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.collect_title_candidates("Czarny mag. Tom 2. Adeptka", candidates)
        best_series = kod_v3.choose_series_candidate(candidates)
        best_title = kod_v3.choose_title_candidate(candidates)
        self.assertIsNotNone(best_series)
        self.assertIsNotNone(best_title)
        assert best_series is not None
        assert best_title is not None
        self.assertEqual(best_series.series, "Czarny mag")
        self.assertEqual(best_series.volume, (2, "00"))

    def test_collect_title_candidates_parses_leading_index_dotted_series_title(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.collect_title_candidates("01. Legenda. Rebeliant", candidates)
        best_series = kod_v3.choose_series_candidate(candidates)
        best_title = kod_v3.choose_title_candidate(candidates)
        self.assertIsNotNone(best_series)
        self.assertIsNotNone(best_title)
        assert best_series is not None
        assert best_title is not None
        self.assertEqual(best_series.series, "Legenda")
        self.assertEqual(best_series.volume, (1, "00"))
        self.assertEqual(best_series.title_override, "Rebeliant")
        self.assertEqual(best_title.title_override, "Rebeliant")

    def test_collect_title_candidates_parses_series_with_parenthetical_volume_only(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.collect_title_candidates("Mroczne umysły (tom 1)", candidates)
        best_series = kod_v3.choose_series_candidate(candidates)
        self.assertIsNotNone(best_series)
        assert best_series is not None
        self.assertEqual(best_series.series, "Mroczne umysły")
        self.assertEqual(best_series.volume, (1, "00"))

    def test_collect_title_candidates_supports_unicode_czesc_marker(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.collect_title_candidates("Saga. Część 2. Finał", candidates)
        best_series = kod_v3.choose_series_candidate(candidates)
        best_title = kod_v3.choose_title_candidate(candidates)
        self.assertIsNotNone(best_series)
        self.assertIsNotNone(best_title)
        assert best_series is not None
        assert best_title is not None
        self.assertEqual(best_series.series, "Saga")
        self.assertEqual(best_series.volume, (2, "00"))
        self.assertEqual(best_title.title_override, "Finał")

    def test_collect_title_candidates_supports_unicode_ksiega_marker(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.collect_title_candidates("Opowieść (Księga 3)", candidates)
        best_series = kod_v3.choose_series_candidate(candidates)
        self.assertIsNotNone(best_series)
        assert best_series is not None
        self.assertEqual(best_series.series, "Opowieść")
        self.assertEqual(best_series.volume, (3, "00"))

    def test_collect_title_candidates_parses_square_bracket_series_prefix(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.collect_title_candidates("[Cykl-Ture Sventon (1)] Latajacy detektyw", candidates)
        best_series = kod_v3.choose_series_candidate(candidates)
        best_title = kod_v3.choose_title_candidate(candidates)
        self.assertIsNotNone(best_series)
        self.assertIsNotNone(best_title)
        assert best_series is not None
        assert best_title is not None
        self.assertEqual(best_series.series, "Ture Sventon")
        self.assertEqual(best_series.volume, (1, "00"))
        self.assertEqual(best_title.title_override, "Latajacy detektyw")

    def test_sanitize_title_strips_trailing_roman_volume_suffix(self) -> None:
        self.assertEqual(
            kod_v3.sanitize_title("Chronicle (Tom IV)", "Cykl", (4, "00")),
            "Chronicle",
        )

    def test_sanitize_title_strips_trailing_cycle_suffix(self) -> None:
        self.assertEqual(
            kod_v3.sanitize_title("Chronicle (Cykl II)", "Cykl", (2, "00")),
            "Chronicle",
        )

    def test_sanitize_title_strips_genre_tail_and_series_book_suffix(self) -> None:
        self.assertEqual(
            kod_v3.sanitize_title("Past's Price The Resonance Cycle, Book 3 [Isekai, LitRPG]", "The Resonance Cycle", (3, "00")),
            "Past's Price",
        )

    def test_unicode_normalization_keeps_polish_matching_signal(self) -> None:
        self.assertEqual(kod_v3.normalize_match_text("Żmijewski"), "zmijewski")
        self.assertEqual(kod_v3.author_key("Łukasz Żmijewski"), "lukaszzmijewski")

    def test_infer_book_genre_prefers_fantasy_over_young_adult(self) -> None:
        self.assertEqual(
            kod_v3.infer_book_genre(["Young Adult Fiction / Fantasy / Dark Fantasy"]),
            "fantasy",
        )

    def test_clean_normalizes_compact_series_volume_title_pattern(self) -> None:
        self.assertEqual(
            kod_v3.clean("Księżycowe Miasto-03.Dom płomienia i cienia"),
            "Księżycowe Miasto 03 Dom płomienia i cienia",
        )

    def test_parse_volume_parts_does_not_treat_single_letter_i_as_roman_volume(self) -> None:
        self.assertIsNone(kod_v3.parse_volume_parts("i"))

    def test_infer_record_handles_compact_series_volume_title_pattern(self) -> None:
        stem = "Księżycowe Miasto-03.Dom płomienia i cienia -- Maas, Sarah J"
        meta = make_meta(stem, creators=["Maas, Sarah J"])
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.author, "Maas Sarah J")
        self.assertEqual(record.series, "Księżycowe Miasto")
        self.assertEqual(record.volume, (3, "00"))
        self.assertEqual(record.title, "Dom płomienia i cienia")

    def test_infer_record_handles_compact_series_volume_title_with_trailing_author(self) -> None:
        stem = "Ksiezycowe Miasto-01.Dom Ziemi - Sarah J. Maas"
        meta = make_meta(stem)
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.author, "Maas Sarah J.")
        self.assertEqual(record.series, "Ksiezycowe Miasto")
        self.assertEqual(record.volume, (1, "00"))
        self.assertEqual(record.title, "Dom Ziemi")

    def test_filename_appends_genre_suffix(self) -> None:
        record = kod_v3.BookRecord(
            path=Path("x.epub"),
            author="Author",
            series="Series",
            volume=(1, "00"),
            title="Title",
            source="test",
            identifiers=[],
            notes=[],
            genre="fantasy",
        )
        self.assertEqual(record.filename, "Author - Series - Tom 01.00 - Title [fantasy].epub")

    def test_google_books_candidates_returns_empty_list_when_no_query_can_be_built(self) -> None:
        meta = make_meta("", title="", creators=[], identifiers=[])
        self.assertEqual(kod_v3.google_books_candidates(meta, 2.0), [])

    def test_infer_record_uses_local_subjects_for_genre(self) -> None:
        meta = make_meta(
            "Some Series 01 Some Title -- Author Name",
            subjects=["Young Adult Fiction / Fantasy / Dark Fantasy"],
        )
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.genre, "fantasy")
        self.assertTrue(record.filename.endswith("Some Title [fantasy].epub"))

    def test_opf_candidate_skips_publisher_like_series(self) -> None:
        candidates: list[kod_v3.Candidate] = []
        kod_v3.add_candidate(candidates, "Pivot Press Publishing", None, 100, "opf")
        self.assertEqual(candidates, [])

    def test_extract_authors_strips_trailing_numeric_noise(self) -> None:
        self.assertEqual(kod_v3.extract_authors([], "Dakota Krout -, 1, 2018"), "Krout Dakota")

    def test_extract_authors_expands_shared_plural_polish_family_name(self) -> None:
        self.assertEqual(
            kod_v3.extract_authors([], "Centkiewiczowie Alina i Czesław"),
            "Centkiewicz Alina & Centkiewicz Czesław",
        )

    def test_extract_authors_uses_catalog_for_multi_author_segment_with_noise(self) -> None:
        self.assertEqual(
            kod_v3.extract_authors([], "Bourne'a Ludlum Robert Van Lustbader Eric"),
            "Ludlum Robert & van Lustbader Eric",
        )

    def test_extract_authors_preserves_catalog_multi_author_order(self) -> None:
        self.assertEqual(
            kod_v3.extract_authors([], "Robert Ludlum & Eric van Lustbader"),
            "Ludlum Robert & van Lustbader Eric",
        )

    def test_build_online_record_filters_non_author_noise_from_author_list(self) -> None:
        meta = make_meta("Świat Bourne'a")
        best = kod_v3.RankedOnlineMatch(
            providers=["google"],
            sources=["google"],
            title="Świat Bourne'a",
            authors=["Bourne'a", "Robert Ludlum", "Eric van Lustbader"],
            identifiers=[],
            score=320,
            reason="title-author-exact",
            series="Jason Bourne",
            volume=(9, "00"),
            genre="thriller",
        )

        record = kod_v3.build_online_record(meta, best)

        self.assertEqual(record.author, "Ludlum Robert & van Lustbader Eric")

    def test_extract_authors_prefers_single_catalog_author_over_mixed_noise_segment(self) -> None:
        self.assertEqual(
            kod_v3.extract_authors([], "Cel Nagi & Snerg Adam Wisniewski"),
            "Snerg Adam Wisniewski",
        )

    def test_infer_record_uses_known_author_from_swapped_metadata_title_in_local_prototype(self) -> None:
        stem = "1 Cel Nagi & Snerg Adam Wisniewski - Standalone - Tom 00.00 - Oro. Otomi znaczy wyslaniec [fantasy]"
        meta = make_meta(
            stem,
            title="Adam Wisniewski-Snerg",
            creators=["Nagi Cel"],
            subjects=["fantasy"],
        )
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.author, "Snerg Adam Wisniewski")
        self.assertEqual(record.title, "Oro. Otomi znaczy wyslaniec")

    def test_infer_record_keeps_leading_title_when_middle_segment_is_only_standalone_volume(self) -> None:
        stem = "1 Nagi Cel - Standalone - Tom 00.00 - Adam Wisniewski-Snerg"
        meta = make_meta(
            stem,
            title="Adam Wisniewski-Snerg",
            creators=["Nagi Cel"],
        )
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.author, "Snerg Adam Wisniewski")
        self.assertEqual(record.title, "Nagi Cel")

    def test_spite_the_dark_source_artifact_is_not_used_as_author(self) -> None:
        stem = "Spite the Dark 01_Assassin Summoner_ Aaron Renfroe -- Anna’s Archive"
        meta = make_meta(stem)
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.author, "Renfroe Aaron")
        self.assertEqual(record.series, "Spite the Dark")
        self.assertEqual(record.volume, (1, "00"))
        self.assertEqual(record.title, "Assassin Summoner")

    def test_unconfirmed_trailing_author_is_downgraded_when_online_enabled(self) -> None:
        stem = "Spite the Dark 01_Assassin Summoner_ Aaron Renfroe -- Anna’s Archive"
        meta = make_meta(stem)
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=[]):
            record = kod_v3.infer_record(meta, use_online=True, providers=["google"], timeout=1.0)
        self.assertEqual(record.author, "Renfroe Aaron")

    def test_unconfirmed_trailing_author_is_removed_when_online_conflicts(self) -> None:
        stem = "Spite the Dark 01_Assassin Summoner_ Aaron Renfroe -- Anna’s Archive"
        meta = make_meta(stem)
        fake_candidates = [
            kod_v3.OnlineCandidate("lubimyczytac", "lubimyczytac", "Some Other Book", ["Jane Smith"], [], 180, "approx")
        ]
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=fake_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["google"], timeout=1.0)
        self.assertEqual(record.author, "Nieznany Autor")
        self.assertIn("online-brak-potwierdzenia-autora", record.review_reasons)

    def test_sanitize_title_for_online_query_strips_author_prefix_and_noise(self) -> None:
        value = kod_v3.sanitize_title_for_online_query(
            "Cale Plamann - A Dream of Wings & Flame A LitRPG Adventure (2024, Aethon Books)",
            "Nieznany Autor",
            "A Dream of Wings & Flame",
            (1, "00"),
        )
        self.assertEqual(value, "A Dream of Wings & Flame A LitRPG Adventure")

    def test_sanitize_title_for_online_query_strips_leading_volume_marker(self) -> None:
        value = kod_v3.sanitize_title_for_online_query(
            "01. Mroczne umysły",
            "Alexandra Bracken",
            "Standalone",
            (0, "00"),
        )
        self.assertEqual(value, "Mroczne umysły")

    def test_sanitize_title_for_online_query_strips_hash_prefixed_volume_marker(self) -> None:
        value = kod_v3.sanitize_title_for_online_query(
            "#2 Pigulki namietnosci",
            "Cyril M. Kornbluth",
            "Dwa Swiaty",
            (2, "00"),
        )
        self.assertEqual(value, "Pigulki namietnosci")

    def test_build_online_query_variants_uses_author_embedded_in_title(self) -> None:
        meta = make_meta("x")
        record = kod_v3.BookRecord(
            path=Path("x.mobi"),
            author="Nieznany Autor",
            series="A Dream of Wings & Flame",
            volume=(1, "00"),
            title="Cale Plamann - A Dream of Wings & Flame A LitRPG Adventure (2024, Aethon Books)",
            source="core:joined",
            identifiers=[],
            notes=[],
            confidence=50,
            review_reasons=[],
            decision_reasons=[],
        )
        variants = kod_v3.build_online_query_variants(meta, record)
        self.assertTrue(any("Plamann Cale" in " | ".join(variant.creators) or "Cale Plamann" in " | ".join(variant.creators) for variant in variants))
        self.assertTrue(any(variant.title == "A Dream of Wings & Flame A LitRPG Adventure" for variant in variants))

    def test_build_online_query_variants_normalizes_existing_format_author_for_search(self) -> None:
        meta = make_meta("x")
        record = kod_v3.BookRecord(
            path=Path("x.epub"),
            author="Aveyard Victoria",
            series="Czerwona Królowa",
            volume=(3, "00"),
            title="Królewska klatka",
            source="existing-format",
            identifiers=[],
            notes=[],
            confidence=90,
            review_reasons=[],
            decision_reasons=[],
        )
        variants = kod_v3.build_online_query_variants(meta, record)
        self.assertTrue(any("Victoria Aveyard" in variant.creators for variant in variants))

    def test_build_online_query_variants_accepts_local_prototype(self) -> None:
        meta = make_meta("x")
        prototype = kod_v3.LocalPrototype(
            path=Path("x.epub"),
            author="Aveyard Victoria",
            series="Czerwona Krolowa",
            volume=(3, "00"),
            title="Krolewska klatka",
            genre="fantasy",
            source="prototype:test",
            confidence=88,
        )

        variants = kod_v3.build_online_query_variants(meta, prototype)

        self.assertTrue(any(variant.title == "Krolewska klatka" for variant in variants))
        self.assertTrue(any("Victoria Aveyard" in variant.creators for variant in variants))

    def test_infer_record_parses_ampersand_separated_title_and_author(self) -> None:
        meta = make_meta("Piekni & przekleci & Scott Fitzgerald Francis")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:ampersand-title-author")
        self.assertEqual(record.title, "Piekni & przekleci")
        self.assertEqual(record.author, "Francis Scott Fitzgerald")

    def test_infer_record_parses_bracketed_title_and_trailing_author(self) -> None:
        meta = make_meta("[bubble] - Anders de la Motte")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:bracketed-title-author")
        self.assertEqual(record.title, "bubble")
        self.assertEqual(record.author, "de la Motte Anders")
        self.assertEqual(
            record.filename,
            "de la Motte Anders - Standalone - Tom 00.00 - bubble.epub",
        )

    def test_infer_record_treats_polish_i_title_as_title_not_coauthors(self) -> None:
        meta = make_meta("Wiezienie i pokoj - Aksjonow Wasilij [Moskiewska saga 3]")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "title:square-bracket-series-book")
        self.assertEqual(record.author, "Wasilij Aksjonow")
        self.assertEqual(record.series, "Moskiewska saga")
        self.assertEqual(record.volume, (3, "00"))
        self.assertEqual(record.title, "Wiezienie i pokoj")
        self.assertEqual(
            record.filename,
            "Wasilij Aksjonow - Moskiewska saga - Tom 03.00 - Wiezienie i pokoj.epub",
        )

    def test_infer_record_extracts_square_bracket_series_from_title_author_pattern(self) -> None:
        meta = make_meta("Latajacy detektyw - Ake Holmberg [Cykl-Ture Sventon (1)]")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.author, "Holmberg Ake")
        self.assertEqual(record.series, "Ture Sventon")
        self.assertEqual(record.volume, (1, "00"))
        self.assertEqual(record.title, "Latajacy detektyw")
        self.assertEqual(
            record.filename,
            "Holmberg Ake - Ture Sventon - Tom 01.00 - Latajacy detektyw.epub",
        )

    def test_infer_record_keeps_title_with_tom_suffix_as_title_not_series(self) -> None:
        meta = make_meta("Wiezien ukladu, tom 2 - Alan Akab")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:delimited-title-author")
        self.assertEqual(record.author, "Akab Alan")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(record.title, "Wiezien ukladu, tom 2")
        self.assertEqual(
            record.filename,
            "Akab Alan - Standalone - Tom 00.00 - Wiezien ukladu, tom 2.epub",
        )

    def test_infer_record_recognizes_plain_author_title_pattern(self) -> None:
        meta = make_meta("Agatha Christie - Poirota Wczesne sprawy")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:delimited-author-title")
        self.assertEqual(record.author, "Christie Agatha")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(record.title, "Poirota Wczesne sprawy")
        self.assertEqual(
            record.filename,
            "Christie Agatha - Standalone - Tom 00.00 - Poirota Wczesne sprawy.epub",
        )

    def test_infer_record_recognizes_plain_author_title_pattern_with_capitalized_polish_title(self) -> None:
        meta = make_meta("Agatha Christie - Morderstwo Uspione")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:delimited-author-title")
        self.assertEqual(record.author, "Christie Agatha")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(record.title, "Morderstwo Uspione")
        self.assertEqual(
            record.filename,
            "Christie Agatha - Standalone - Tom 00.00 - Morderstwo Uspione.epub",
        )

    def test_infer_record_recognizes_plain_author_title_pattern_with_number_word_title(self) -> None:
        meta = make_meta("Agatha Christie - Trzynascie zagadek")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:delimited-author-title")
        self.assertEqual(record.author, "Christie Agatha")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(record.title, "Trzynascie zagadek")
        self.assertEqual(
            record.filename,
            "Christie Agatha - Standalone - Tom 00.00 - Trzynascie zagadek.epub",
        )

    def test_infer_record_recognizes_plain_author_title_pattern_with_polish_preposition_title(self) -> None:
        meta = make_meta("Agatha Christie - Podroz w nieznane")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:delimited-author-title")
        self.assertEqual(record.author, "Christie Agatha")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(record.title, "Podroz w nieznane")
        self.assertEqual(
            record.filename,
            "Christie Agatha - Standalone - Tom 00.00 - Podroz w nieznane.epub",
        )

    def test_infer_record_recognizes_plain_author_title_pattern_with_three_part_author(self) -> None:
        meta = make_meta("Adrianna Ewa Stawska - Smierc w klasztorze")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:delimited-author-title")
        self.assertEqual(record.author, "Stawska Adrianna Ewa")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(record.title, "Smierc w klasztorze")
        self.assertEqual(
            record.filename,
            "Stawska Adrianna Ewa - Standalone - Tom 00.00 - Smierc w klasztorze.epub",
        )

    def test_infer_record_recognizes_plain_author_title_pattern_with_short_title(self) -> None:
        meta = make_meta("Adrian Lara - mm")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:delimited-author-title")
        self.assertEqual(record.author, "Lara Adrian")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(record.title, "mm")
        self.assertEqual(
            record.filename,
            "Lara Adrian - Standalone - Tom 00.00 - mm.epub",
        )

    def test_infer_record_recognizes_compact_author_title_pattern_without_separator(self) -> None:
        meta = make_meta("Aguirre Ann Enklawa")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:compact-author-title")
        self.assertEqual(record.author, "Aguirre Ann")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(record.title, "Enklawa")
        self.assertEqual(
            record.filename,
            "Aguirre Ann - Standalone - Tom 00.00 - Enklawa.epub",
        )

    def test_infer_record_recognizes_plain_title_author_pattern_with_short_title(self) -> None:
        meta = make_meta("mm - Adrian Lara")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:delimited-title-author")
        self.assertEqual(record.author, "Lara Adrian")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(record.title, "mm")
        self.assertEqual(
            record.filename,
            "Lara Adrian - Standalone - Tom 00.00 - mm.epub",
        )

    def test_infer_record_keeps_ampersand_inside_long_title_when_author_is_trailing(self) -> None:
        meta = make_meta(
            "Specyfikacja na przykladach. Poznaj zwinne metody & wlasciwie dostarczaj oprogramowanie - Adzic Gojko"
        )

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:delimited-title-author")
        self.assertEqual(record.author, "Gojko Adzic")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(
            record.title,
            "Specyfikacja na przykladach. Poznaj zwinne metody & wlasciwie dostarczaj oprogramowanie",
        )

    def test_infer_record_keeps_ampersand_inside_long_title_when_author_is_leading(self) -> None:
        meta = make_meta(
            "Adzic Gojko - Specyfikacja na przykladach. Poznaj zwinne metody & wlasciwie dostarczaj oprogramowanie"
        )

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:delimited-author-title")
        self.assertEqual(record.author, "Gojko Adzic")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(
            record.title,
            "Specyfikacja na przykladach. Poznaj zwinne metody & wlasciwie dostarczaj oprogramowanie",
        )

    def test_infer_record_keeps_ampersand_inside_short_title_when_author_is_leading(self) -> None:
        meta = make_meta("Agatha Christie - Przyjdz & zgin")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.source, "hybrid:delimited-author-title")
        self.assertEqual(record.author, "Christie Agatha")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(record.title, "Przyjdz & zgin")
        self.assertEqual(
            record.filename,
            "Christie Agatha - Standalone - Tom 00.00 - Przyjdz & zgin.epub",
        )

    def test_infer_record_extracts_square_bracket_series_from_prefixed_title_author_pattern(self) -> None:
        meta = make_meta("[Krolestwo Polksiezyca 1] Tron Polksiezyca - Ahmed Saladin")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.author, "Saladin Ahmed")
        self.assertEqual(record.series, "Krolestwo Polksiezyca")
        self.assertEqual(record.volume, (1, "00"))
        self.assertEqual(record.title, "Tron Polksiezyca")
        self.assertEqual(
            record.filename,
            "Saladin Ahmed - Krolestwo Polksiezyca - Tom 01.00 - Tron Polksiezyca.epub",
        )

    def test_infer_record_extracts_square_bracket_series_from_author_leading_pattern(self) -> None:
        meta = make_meta("Ahmed Saladin - [Krolestwo Polksiezyca 1] Tron Polksiezyca")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.author, "Saladin Ahmed")
        self.assertEqual(record.series, "Krolestwo Polksiezyca")
        self.assertEqual(record.volume, (1, "00"))
        self.assertEqual(record.title, "Tron Polksiezyca")
        self.assertEqual(
            record.filename,
            "Saladin Ahmed - Krolestwo Polksiezyca - Tom 01.00 - Tron Polksiezyca.epub",
        )

    def test_strip_source_artifacts_removes_numeric_suffix(self) -> None:
        self.assertEqual(kod_v3.strip_source_artifacts("Title - libgen.li (1)"), "Title")

    def test_strip_source_artifacts_removes_raw_domain_artifact(self) -> None:
        self.assertEqual(kod_v3.strip_source_artifacts("www.scan-dal.prv.pl"), "")

    def test_strip_source_artifacts_keeps_domain_inside_normal_title(self) -> None:
        self.assertEqual(
            kod_v3.strip_source_artifacts("Przewodnik www.example.com dla kazdego"),
            "Przewodnik www.example.com dla kazdego",
        )

    def test_infer_record_does_not_keep_plain_author_name_as_fallback_title(self) -> None:
        meta = make_meta("Alexandra Adornetto")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.author, "Adornetto Alexandra")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(record.title, "Bez tytulu")
        self.assertEqual(
            record.filename,
            "Adornetto Alexandra - Standalone - Tom 00.00 - Bez tytulu.epub",
        )

    def test_strip_author_from_title_removes_prefixed_author_without_known_author_field(self) -> None:
        self.assertEqual(
            kod_v3.strip_author_from_title(
                "Cale Plamann - A Dream of Wings & Flame A LitRPG Adventure",
                "",
            ),
            "A Dream of Wings & Flame A LitRPG Adventure",
        )

    def test_father_of_constructs_source_prefers_series_over_publisher(self) -> None:
        stem = (
            "Father of Constructs 03 The Eldritch Artisan_ Father of Constructs_ Book 3 (LitRPG) -- "
            "Renfroe, Aaron -- 2024 -- Pivot Press Publishing, LLC -- 3ab8cab58a126a4efe8075f1e9d4dd17 -- Anna’s Archive"
        )
        meta = make_meta(stem)
        meta.title = "The Eldritch Artisan: Father of Constructs: Book 3 (LitRPG)"
        meta.creators = ["Renfroe, Aaron"]
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.author, "Renfroe Aaron")
        self.assertEqual(record.series, "Father of Constructs")
        self.assertEqual(record.volume, (3, "00"))
        self.assertEqual(record.title, "The Eldritch Artisan")

    def test_resonance_war_source_prefers_cycle_over_publisher(self) -> None:
        stem = (
            "The Resonance Cycle_ 10 The Resonance War (Part 2)_ The Resonance Cycle Book 10 -- Aaron Renfroe -- "
            "2025 -- Pivot Press Publishing, LLC -- 123 -- Anna’s Archive"
        )
        meta = make_meta(stem)
        meta.creators = ["Aaron Renfroe"]
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.author, "Renfroe Aaron")
        self.assertEqual(record.series, "The Resonance Cycle")
        self.assertEqual(record.volume, (10, "00"))
        self.assertEqual(record.title, "The Resonance War (Part 2)")

    def test_complete_series_box_set_maps_to_series(self) -> None:
        stem = "Blessed Time_ The Complete Series_ (A LitRPG Adventure Box Set) -- Cale Plamann -- 2023 -- Aethon Books -- hash -- Anna’s Archive"
        meta = make_meta(stem, creators=["Cale Plamann"])
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.author, "Plamann Cale")
        self.assertEqual(record.series, "Blessed Time")
        self.assertEqual(record.title, "The Complete Series")

    def test_pick_best_online_match_marks_small_margin_as_ambiguous(self) -> None:
        meta = make_meta("Right Book", title="Right Book", creators=["Test Author"])
        candidates = [
            kod_v3.OnlineCandidate("google-books", "google-books", "Right Book", ["Test Author"], [], 300, "title-author-exact"),
            kod_v3.OnlineCandidate("open-library", "open-library:search", "Right Book Deluxe", ["Test Author"], [], 284, "title-author-exact"),
        ]
        best = kod_v3.pick_best_online_match(meta, candidates)
        self.assertIsNotNone(best)
        assert best is not None
        self.assertIn("ambiguous", best.reason)

    def test_pick_best_online_match_ignores_local_candidate_objects(self) -> None:
        meta = make_meta("Right Book", title="Right Book", creators=["Test Author"])
        candidates = [
            kod_v3.Candidate(90, "Series", (1, "00"), "Right Book", "core:joined"),
            kod_v3.OnlineCandidate("google-books", "google-books", "Right Book", ["Test Author"], [], 300, "title-author-exact"),
        ]
        best = kod_v3.pick_best_online_match(meta, candidates)
        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best.providers, ["google-books"])

    def test_ambiguous_online_is_checked_but_not_applied(self) -> None:
        meta = make_meta("Mystery Book")
        offline = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        online = kod_v3.BookRecord(
            path=meta.path,
            author="Test Author",
            series="",
            volume=None,
            title="Mystery Book",
            source="google-books",
            identifiers=[],
            notes=[],
            confidence=78,
            review_reasons=["online-niejednoznaczne"],
            decision_reasons=["online-candidate:google-books"],
            online_checked=True,
        )
        fake_candidate = kod_v3.OnlineCandidate(
            "google-books",
            "google-books",
            "Mystery Book",
            ["Test Author"],
            [],
            300,
            "title-author-exact",
        )
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=[fake_candidate]), \
             mock.patch.object(kod_v3, "pick_best_online_match", return_value=fake_candidate), \
             mock.patch.object(kod_v3, "build_online_record", return_value=online):
            record = kod_v3.infer_record(meta, use_online=True, providers=["google"], timeout=1.0)
        self.assertTrue(record.online_checked)
        self.assertFalse(record.online_applied)
        self.assertIn("online-niejednoznaczne", record.review_reasons)
        self.assertNotIn("uzupelnione-online", record.review_reasons)
        self.assertEqual(record.confidence, offline.confidence)

    def test_validate_record_components_with_online_reassigns_fields_by_role(self) -> None:
        meta = make_meta("Ksiezycowe Miasto-01.Dom Ziemi - Sarah J. Maas")
        local_candidates: list[kod_v3.Candidate] = []
        kod_v3.add_candidate(local_candidates, "Ksiezycowe Miasto", (1, "00"), 91, "core:title-author", "Dom Ziemi")
        record = kod_v3.BookRecord(
            path=meta.path,
            author="Nieznany Autor",
            series="Standalone",
            volume=None,
            title="Sarah J. Maas",
            source="core:spaced",
            identifiers=[],
            notes=[],
            confidence=50,
            review_reasons=[],
            decision_reasons=[],
        )
        online_candidates = [
            kod_v3.OnlineCandidate(
                "google-books",
                "google-books",
                "Dom Ziemi",
                ["Sarah J. Maas"],
                [],
                230,
                "title-author-exact",
            ),
            kod_v3.OnlineCandidate(
                "google-books",
                "google-books",
                "Dom Ziemi: Ksiezycowe Miasto, Book 1",
                ["Sarah J. Maas"],
                [],
                220,
                "title-author-exact",
            )
        ]
        verification = kod_v3.OnlineVerification(True, False, False, False, False, ["google-books"])
        verification = kod_v3.validate_record_components_with_online(
            record,
            meta,
            local_candidates,
            online_candidates,
            verification,
        )
        self.assertEqual(record.author, "Maas Sarah J")
        self.assertEqual(record.series, "Ksiezycowe Miasto")
        self.assertEqual(record.volume, (1, "00"))
        self.assertEqual(record.title, "Dom Ziemi")
        self.assertTrue(verification.author_confirmed)
        self.assertTrue(verification.series_confirmed)
        self.assertTrue(verification.volume_confirmed)
        self.assertTrue(verification.title_confirmed)

    def test_filename_preserves_unknown_series_and_volume_placeholders(self) -> None:
        record = kod_v3.BookRecord(
            path=Path("book.epub"),
            author="Bracken Alexandra",
            series="Standalone",
            volume=None,
            title="Po zmierzchu",
            source="fallback",
            identifiers=[],
            notes=[],
            genre="fantasy",
        )
        self.assertEqual(record.filename, "Bracken Alexandra - Standalone - Tom 00.00 - Po zmierzchu [fantasy].epub")

    def test_existing_format_with_standalone_and_zero_volume_still_allows_online_series_upgrade(self) -> None:
        meta = make_meta("Carter Rachel E - Standalone - Tom 00.00 - 01. Pierwszy rok")
        online_candidates = [
            kod_v3.OnlineCandidate(
                "google-books",
                "google-books",
                "First Year: The Black Mage, Book 1",
                ["Rachel E. Carter"],
                [],
                230,
                "title-author-exact",
            )
        ]
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["google"], timeout=1.0)
        self.assertEqual(record.author, "Carter Rachel E")
        self.assertEqual(record.series, "The Black Mage")
        self.assertEqual(record.volume, (1, "00"))

    def test_existing_format_preserves_genre_suffix_from_input_filename_until_verified(self) -> None:
        meta = make_meta("Wisniewski-Snerg Adam - Standalone - Tom 00.00 - Nagi cel [fantasy]")
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.genre, "fantasy")
        self.assertIn("[fantasy]", record.filename)

    def test_unknown_volume_from_fallback_is_filled_from_online_series_match(self) -> None:
        meta = make_meta(
            "01. Czerwona Kr?lowa - Victoria Aveyard",
            title="01. Czerwona Kr?lowa",
            creators=["Victoria Aveyard"],
        )
        online_candidates = [
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "Czerwona Kr?lowa",
                ["Victoria Aveyard"],
                [],
                320,
                "title-author-exact",
                series="Czerwona Kr?lowa",
                volume=(1, "00"),
                genre="fantasy",
            )
        ]
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["lubimyczytac"], timeout=1.0)
        self.assertEqual(record.series, "Czerwona Kr?lowa")
        self.assertEqual(record.volume, (1, "00"))
        self.assertEqual(record.filename, "Aveyard Victoria - Czerwona Kr-lowa - Tom 01.00 - Czerwona Kr-lowa [fantasy].epub")

    def test_existing_format_with_lubimyczytac_series_metadata_upgrades_polish_title(self) -> None:
        meta = make_meta(
            "Bracken Alexandra - Standalone - Tom 00.00 - 01. Mroczne umysły",
            creators=["Alexandra Bracken"],
        )
        online_candidates = [
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "Mroczne umysły",
                ["Alexandra Bracken"],
                [],
                252,
                "title-author-approx",
                series="Mroczne umysły",
                volume=(1, "00"),
            )
        ]
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["lubimyczytac"], timeout=1.0)
        self.assertEqual(record.author, "Bracken Alexandra")
        self.assertEqual(record.series, "Mroczne umysły")
        self.assertEqual(record.volume, (1, "00"))
        self.assertEqual(record.title, "Mroczne umysły")

    def test_lubimyczytac_title_overrides_hash_prefixed_local_title(self) -> None:
        meta = make_meta(
            "#2 Pigulki namietnosci - Cyril M. Kornbluth",
            title="#2 Pigulki namietnosci",
            creators=["Cyril M. Kornbluth"],
        )
        online_candidates = [
            kod_v3.OnlineCandidate(
                "google-books",
                "google-books",
                "Dwa Swiaty. #2 Pigulki namietnosci",
                ["Cyril M. Kornbluth"],
                [],
                260,
                "title-author-approx",
                series="Dwa Swiaty",
                volume=(2, "00"),
                genre="fantasy",
            ),
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "Pigulki namietnosci",
                ["Cyril M. Kornbluth"],
                [],
                252,
                "title-author-approx",
                series="Dwa Swiaty",
                volume=(2, "00"),
                genre="fantasy",
            ),
        ]
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["google", "lubimyczytac"], timeout=1.0)
        self.assertEqual(record.series, "Dwa Swiaty")
        self.assertEqual(record.volume, (2, "00"))
        self.assertEqual(record.title, "Pigulki namietnosci")
        self.assertNotIn("#2", record.filename)

    def test_online_context_filter_rejects_alien_series_for_known_book(self) -> None:
        meta = make_meta(
            "Aveyard Victoria - Standalone - Tom 00.00 - 04. Wojenna burza",
            creators=["Victoria Aveyard"],
        )
        online_candidates = [
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "2001: Odyseja kosmiczna",
                ["Arthur C. Clarke"],
                [],
                260,
                "title-author-approx",
                series="Odyseja Kosmiczna",
                volume=(2, "00"),
            )
        ]
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["lubimyczytac"], timeout=1.0)
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, (0, "00"))
        self.assertEqual(record.title, "04. Wojenna burza")

    def test_existing_format_with_wrong_volume_can_be_corrected_online(self) -> None:
        meta = make_meta(
            "Aveyard Victoria - Czerwona Królowa - Tom 02.00 - Wojenna burza",
            creators=["Victoria Aveyard"],
        )
        online_candidates = [
            kod_v3.OnlineCandidate(
                "open-library",
                "open-library:search",
                "Wojenna burza",
                ["Victoria Aveyard"],
                [],
                320,
                "title-author-exact",
                series="Czerwona Królowa",
                volume=(4, "00"),
            )
        ]
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["openlibrary"], timeout=1.0)
        self.assertEqual(record.series, "Czerwona Królowa")
        self.assertEqual(record.volume, (4, "00"))
        self.assertEqual(record.title, "Wojenna burza")

    def test_existing_format_with_alien_series_is_corrected_from_contextual_online_match(self) -> None:
        meta = make_meta(
            "Bracken Alexandra - Odyseja Kosmiczna - Tom 02.00 - Nigdy nie gasn? [fantasy]",
            creators=["Alexandra Bracken"],
        )
        online_candidates = [
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "Nigdy nie gasn?",
                ["Alexandra Bracken"],
                [],
                252,
                "title-author-approx",
                series="Mroczne umys?y",
                volume=(2, "00"),
                genre="fantasy",
            ),
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "Odyseja kosmiczna 2010",
                ["Arthur C. Clarke"],
                [],
                166,
                "approx",
                series="Odyseja Kosmiczna",
                volume=(2, "00"),
                genre="sci-fi",
            ),
        ]
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["lubimyczytac"], timeout=1.0)
        self.assertEqual(record.series, "Mroczne umys?y")
        self.assertEqual(record.volume, (2, "00"))
        self.assertEqual(record.title, "Nigdy nie gasn?")
        self.assertEqual(record.genre, "fantasy")

    def test_parse_existing_filename_preserves_trailing_genre_suffix(self) -> None:
        parsed = kod_v3.parse_existing_filename("Author - Series - Tom 01.00 - Title [fantasy]")
        self.assertEqual(parsed, ("Author", "Series", (1, "00"), "Title", "fantasy"))

    def test_parse_existing_filename_rejects_parenthesized_index_prefix_as_author(self) -> None:
        parsed = kod_v3.parse_existing_filename("(03) - Burzowe Kocie - Maja Lidia Kossakowska")
        self.assertIsNone(parsed)

    def test_parse_existing_filename_rejects_short_lowercase_author_prefix(self) -> None:
        parsed = kod_v3.parse_existing_filename("ja - Aksjonow Wasilij - [Moskiewska saga 3] Wiezienie i pokoj")
        self.assertIsNone(parsed)

    def test_infer_record_handles_parenthesized_index_title_author_pattern(self) -> None:
        meta = make_meta(
            "(03) - Burzowe Kocie - Maja Lidia Kossakowska",
            subjects=["fantasy"],
        )
        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)
        self.assertEqual(record.author, "Kossakowska Maja Lidia")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.title, "Burzowe Kocie")
        self.assertEqual(
            record.filename,
            "Kossakowska Maja Lidia - Standalone - Tom 00.00 - Burzowe Kocie [fantasy].epub",
        )

    def test_infer_record_does_not_treat_short_lowercase_prefix_as_existing_format(self) -> None:
        meta = make_meta("ja - Aksjonow Wasilij - [Moskiewska saga 3] Wiezienie i pokoj")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertNotEqual(record.source, "existing-format")
        self.assertEqual(record.source, "hybrid:prefixed-noise-title-author")
        self.assertEqual(record.author, "Wasilij Aksjonow")
        self.assertEqual(record.title, "Wiezienie i pokoj")

    def test_online_applied_does_not_force_review(self) -> None:
        meta = make_meta("Series 1: Title", creators=["Known Author"])
        record = kod_v3.BookRecord(
            path=meta.path,
            author="Known Author",
            series="Series",
            volume=(1, "00"),
            title="Title",
            source="title:series-book",
            identifiers=[],
            notes=[],
            confidence=78,
            review_reasons=[],
            decision_reasons=["online-applied:google-books"],
            online_checked=True,
            online_applied=True,
        )
        final = kod_v3.finalize_record_quality(record, meta, 78, title_from_core=False)
        self.assertTrue(final.online_checked)
        self.assertTrue(final.online_applied)
        self.assertNotIn("uzupelnione-online", final.review_reasons)
        self.assertFalse(final.needs_review)

    def test_advisory_review_reason_does_not_force_review(self) -> None:
        record = kod_v3.BookRecord(
            path=Path("x.epub"),
            author="Author",
            series="Series",
            volume=(1, "00"),
            title="Title",
            source="test",
            identifiers=[],
            notes=[],
            confidence=80,
            review_reasons=["blad-odczytu-metadanych"],
            decision_reasons=[],
        )
        self.assertFalse(record.needs_review)

    def test_crossref_requires_isbn(self) -> None:
        meta = make_meta("Only Title", title="Only Title", creators=["Author"], identifiers=[])
        with mock.patch.object(kod_v3, "online_query") as online_query:
            self.assertEqual(kod_v3.crossref_candidates(meta, 2.0), [])
        online_query.assert_not_called()

    def test_lubimyczytac_single_source_is_best_effort(self) -> None:
        meta = make_meta("Some Book", title="Some Book")
        best = kod_v3.pick_best_online_match(
            meta,
            [kod_v3.OnlineCandidate("lubimyczytac", "lubimyczytac", "Some Book", ["Author"], [], 320, "title-author-exact")],
        )
        self.assertIsNotNone(best)
        assert best is not None
        self.assertIn("best-effort", best.reason)

    def test_lubimyczytac_single_source_polish_title_is_not_best_effort(self) -> None:
        meta = make_meta("Tom 1 Czerwona Krolowa", title="Tom 1 Czerwona Krolowa")
        best = kod_v3.pick_best_online_match(
            meta,
            [kod_v3.OnlineCandidate("lubimyczytac", "lubimyczytac", "Czerwona Krolowa", ["Victoria Aveyard"], [], 320, "title-author-exact")],
        )
        self.assertIsNotNone(best)
        assert best is not None
        self.assertNotIn("best-effort", best.reason)

    def test_pick_best_online_match_prefers_lubimyczytac_for_polish_title(self) -> None:
        meta = make_meta("Tom 1 Czerwona Krolowa", title="Tom 1 Czerwona Krolowa", creators=["Victoria Aveyard"])
        best = kod_v3.pick_best_online_match(
            meta,
            [
                kod_v3.OnlineCandidate(
                    "google-books",
                    "google-books",
                    "Czerwona Krolowa Deluxe",
                    ["Victoria Aveyard"],
                    [],
                    300,
                    "title-author-exact",
                ),
                kod_v3.OnlineCandidate(
                    "lubimyczytac",
                    "lubimyczytac",
                    "Czerwona Krolowa",
                    ["Victoria Aveyard"],
                    [],
                    280,
                    "title-author-exact",
                ),
            ],
        )
        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best.providers, ["lubimyczytac"])

    def test_aggregate_online_candidates_keeps_descriptive_fields_from_best_candidate_only(self) -> None:
        aggregated = kod_v3.aggregate_online_candidates(
            [
                kod_v3.OnlineCandidate(
                    "google-books",
                    "google-books",
                    "Tylko jedno spojrzenie",
                    ["Harlan Coben"],
                    ["9781111111111"],
                    280,
                    "title-author-exact",
                    genre="sensacja",
                ),
                kod_v3.OnlineCandidate(
                    "lubimyczytac",
                    "lubimyczytac",
                    "Tylko jedno spojrzenie",
                    ["Harlan Coben"],
                    [],
                    320,
                    "title-author-exact",
                    genre="thriller",
                ),
            ]
        )

        self.assertEqual(len(aggregated), 2)
        best = max(aggregated, key=lambda item: item.score)
        self.assertEqual(best.providers, ["lubimyczytac"])
        self.assertEqual(best.title, "Tylko jedno spojrzenie")
        self.assertEqual(best.authors, ["Harlan Coben"])
        self.assertEqual(best.genre, "thriller")
        self.assertEqual(best.identifiers, [])

    def test_fetch_online_candidates_pl_mode_uses_only_lubimyczytac(self) -> None:
        meta = make_meta("Okrutny biegun", title="Okrutny biegun")
        calls: list[str] = []

        def make_provider(name: str):
            def provider(_meta, _timeout):
                calls.append(name)
                if name == "lubimyczytac":
                    return [kod_v3.OnlineCandidate(name, name, "Okrutny biegun", ["Alina Centkiewicz"], [], 320, "title-author-exact")]
                return [kod_v3.OnlineCandidate(name, name, "Other", ["Other"], [], 320, "title-author-exact")]

            return provider

        with mock.patch.object(kod_v3, "google_books_candidates", side_effect=make_provider("google")), \
             mock.patch.object(kod_v3, "open_library_candidates", side_effect=make_provider("openlibrary")), \
             mock.patch.object(kod_v3, "crossref_candidates", side_effect=make_provider("crossref")), \
             mock.patch.object(kod_v3, "hathitrust_candidates", side_effect=make_provider("hathitrust")), \
             mock.patch.object(kod_v3, "lubimyczytac_candidates", side_effect=make_provider("lubimyczytac")):
            candidates = kod_v3.fetch_online_candidates(
                meta,
                ["google", "openlibrary", "crossref", "hathitrust", "lubimyczytac"],
                1.0,
                online_mode="PL",
            )

        self.assertEqual(calls, ["lubimyczytac"])
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].provider, "lubimyczytac")

    def test_fetch_online_candidates_pl_plus_falls_back_after_weak_lubimyczytac(self) -> None:
        meta = make_meta("Okrutny biegun", title="Okrutny biegun")
        calls: list[str] = []

        def make_provider(name: str):
            def provider(_meta, _timeout):
                calls.append(name)
                if name == "lubimyczytac":
                    return [kod_v3.OnlineCandidate(name, name, "Okrutny", ["Unknown"], [], 120, "approx")]
                if name == "google":
                    return [kod_v3.OnlineCandidate(name, name, "Okrutny biegun", ["Alina Centkiewicz"], [], 320, "title-author-exact")]
                return []

            return provider

        with mock.patch.object(kod_v3, "google_books_candidates", side_effect=make_provider("google")), \
             mock.patch.object(kod_v3, "open_library_candidates", side_effect=make_provider("openlibrary")), \
             mock.patch.object(kod_v3, "crossref_candidates", side_effect=make_provider("crossref")), \
             mock.patch.object(kod_v3, "hathitrust_candidates", side_effect=make_provider("hathitrust")), \
             mock.patch.object(kod_v3, "lubimyczytac_candidates", side_effect=make_provider("lubimyczytac")):
            candidates = kod_v3.fetch_online_candidates(
                meta,
                ["google", "openlibrary", "crossref", "hathitrust", "lubimyczytac"],
                1.0,
                online_mode="PL+",
            )

        self.assertEqual(calls[:2], ["lubimyczytac", "google"])
        self.assertIn("google", calls)
        self.assertEqual(candidates[0].provider, "lubimyczytac")
        self.assertEqual(candidates[1].provider, "google")

    def test_pl_mode_stops_after_first_strong_lubimyczytac_query(self) -> None:
        meta = make_meta("Okrutny biegun", title="Okrutny biegun")
        fetch_calls: list[str] = []

        def fake_fetch(target_meta, providers, timeout, **kwargs):
            fetch_calls.append(target_meta.title or target_meta.core)
            return [
                kod_v3.OnlineCandidate(
                    "lubimyczytac",
                    "lubimyczytac",
                    "Okrutny biegun",
                    ["Alina Centkiewicz"],
                    [],
                    320,
                    "title-author-exact",
                    genre="biografia",
                )
            ]

        with mock.patch.object(kod_v3, "fetch_online_candidates", side_effect=fake_fetch), \
             mock.patch.object(
                 kod_v3,
                 "build_online_query_variants",
                 return_value=[
                     make_meta("wariant 1", title="wariant 1"),
                     make_meta("wariant 2", title="wariant 2"),
                 ],
             ):
            record = kod_v3.infer_record(meta, use_online=True, providers=["lubimyczytac"], timeout=1.0, online_mode="PL")

        self.assertEqual(fetch_calls, ["Okrutny biegun"])
        self.assertEqual(record.title, "Okrutny biegun")

    def test_strong_lubimyczytac_confirmation_clears_best_effort_review(self) -> None:
        meta = make_meta(
            "Aveyard Victoria - Czerwona Kr?lowa - Tom 00.00 - Czerwona Kr?lowa [fantasy]",
            title="01. Czerwona Kr?lowa",
            creators=["Victoria Aveyard"],
        )
        online_candidates = [
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "Czerwona Kr?lowa",
                ["Victoria Aveyard"],
                [],
                320,
                "title-author-exact",
                series="Czerwona Kr?lowa",
                volume=(1, "00"),
                genre="fantasy",
            )
        ]
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["lubimyczytac"], timeout=1.0)
        self.assertEqual(record.series, "Czerwona Kr?lowa")
        self.assertEqual(record.volume, (1, "00"))
        self.assertNotIn("online-best-effort", record.review_reasons)
        self.assertNotIn("online-niejednoznaczne", record.review_reasons)
        self.assertFalse(record.needs_review)
        self.assertIn("online-truth:lubimyczytac", record.decision_reasons)

    def test_pl_lubimyczytac_truth_overrides_bad_local_author_and_title(self) -> None:
        meta = make_meta(
            "[000] Doyle Arthur Conan - Tajemnica Doliny Boscombe",
            creators=["Arthur Conan Doyle"],
        )
        online_candidates = [
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "Tajemnica Doliny Boscombe",
                ["Arthur Conan Doyle"],
                [],
                320,
                "title-author-exact",
                genre="kryminał, sensacja, thriller",
            )
        ]
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["lubimyczytac"], timeout=1.0, online_mode="PL")
        self.assertEqual(record.author, "Doyle Arthur Conan")
        self.assertEqual(record.title, "Tajemnica Doliny Boscombe")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.volume, None)
        self.assertEqual(record.genre, "kryminał, sensacja, thriller")
        self.assertIn("online-truth:lubimyczytac", record.decision_reasons)
        self.assertNotIn("fallback", record.review_reasons)
        self.assertEqual(
            record.filename,
            "Doyle Arthur Conan - Standalone - Tom 00.00 - Tajemnica Doliny Boscombe [kryminał, sensacja, thriller].epub",
        )

    def test_existing_format_genre_suffix_is_replaced_when_online_verifies_different_label(self) -> None:
        meta = make_meta(
            "Wisniewski-Snerg Adam - Standalone - Tom 00.00 - Nagi cel [fantasy]",
            creators=["Adam WiĹ›niewski-Snerg"],
        )
        online_candidates = [
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "Nagi cel",
                ["Adam WiĹ›niewski-Snerg"],
                [],
                320,
                "title-author-exact",
                genre="kryminaĹ‚, sensacja, thriller",
            )
        ]
        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["lubimyczytac"], timeout=1.0, online_mode="PL")
        self.assertEqual(record.genre, "kryminaĹ‚, sensacja, thriller")
        self.assertNotIn("[fantasy]", record.filename)
        self.assertIn("[kryminaĹ‚, sensacja, thriller]", record.filename)

    def test_build_online_record_preserves_lubimyczytac_coauthor_order(self) -> None:
        meta = make_meta("Okrutny biegun")
        best = kod_v3.RankedOnlineMatch(
            ["lubimyczytac"],
            ["lubimyczytac"],
            "Okrutny biegun",
            ["Czesław Centkiewicz", "Alina Centkiewicz"],
            [],
            320,
            "title-author-exact",
            genre="biografia",
        )

        record = kod_v3.build_online_record(meta, best)

        self.assertEqual(record.author, "Centkiewicz Czesław & Centkiewicz Alina")

    def test_lubimyczytac_html_parser_extracts_title_and_author(self) -> None:
        parser = kod_v3.LubimyczytacSearchParser()
        parser.feed(
            '<a class="authorAllBooks__singleTextTitle" href="/x">Bibliomancer</a>'
            '<div class="authorAllBooks__singleTextAuthor"><a href="/a">James Hunter</a></div>'
            '<div class="listLibrary__info listLibrary__info--cycles"><a href="/c">Wolfman Warlock (tom 1)</a></div>'
        )
        parser.close()
        self.assertEqual(
            parser.results,
            [kod_v3.LubimyczytacResult("Bibliomancer", ["James Hunter"], "Wolfman Warlock", (1, "00"), "/x")],
        )

    def test_parse_lubimyczytac_detail_page_extracts_cycle_and_genre(self) -> None:
        page = (
            '<span class="d-none d-sm-block mt-1"> Cykl:'
            '<a href="/cykl/4826/czerwona-krolowa"> Czerwona Kr?lowa (tom 3) </a></span>'
            '<a class="book__category d-sm-block d-none" href="/kategoria/beletrystyka/fantasy-science-fiction">'
            ' fantasy, science fiction </a>'
        )
        series, volume, genres = kod_v3.parse_lubimyczytac_detail_page(page)
        self.assertEqual(series, "Czerwona Kr?lowa")
        self.assertEqual(volume, (3, "00"))
        self.assertEqual(genres, ["fantasy, science fiction"])

    def test_parse_lubimyczytac_detail_page_prefers_cycle_over_series_label(self) -> None:
        page = (
            '<span class="d-none d-sm-block mt-1"> Cykl:'
            '<a href="/cykl/1/cycle-name"> Cycle Name (tom 2) </a></span>'
            '<span class="d-none d-sm-block mt-1"> Seria:'
            '<a href="/seria/1/series-name"> Series Name </a></span>'
        )
        series, volume, genres = kod_v3.parse_lubimyczytac_detail_page(page)
        self.assertEqual(series, "Cycle Name")
        self.assertEqual(volume, (2, "00"))
        self.assertEqual(genres, [])

    def test_parse_lubimyczytac_detail_page_extracts_genre_from_generic_category_link(self) -> None:
        page = '<a href="/kategoria/literatura-piekna/literatura-wspolczesna"> literatura wspolczesna </a>'

        series, volume, genres = kod_v3.parse_lubimyczytac_detail_page(page)

        self.assertEqual(series, "")
        self.assertIsNone(volume)
        self.assertEqual(genres, ["literatura wspolczesna"])

    def test_lubimyczytac_candidates_enrich_from_detail_page(self) -> None:
        meta = make_meta("Kr?lewska klatka", creators=["Victoria Aveyard"])
        search_page = (
            '<a class="authorAllBooks__singleTextTitle" href="/ksiazka/4404787/krolewska-klatka"> Kr?lewska klatka </a>'
            '<div class="authorAllBooks__singleTextAuthor"><a href="/autor/72769/victoria-aveyard">Victoria Aveyard</a></div>'
        )
        detail_page = (
            '<span class="d-none d-sm-block mt-1"> Cykl:<a href="/cykl/4826/czerwona-krolowa"> Czerwona Kr?lowa (tom 3) </a></span>'
            '<a class="book__category d-sm-block d-none" href="/kategoria/beletrystyka/fantasy-science-fiction"> fantasy, science fiction </a>'
        )
        with mock.patch.object(
            kod_v3,
            "online_text_query",
            side_effect=lambda url, timeout: detail_page if "ksiazka/4404787" in url else search_page,
        ):
            candidates = kod_v3.lubimyczytac_candidates(meta, 2.0)
        self.assertTrue(candidates)
        best = max(candidates, key=lambda item: item.score)
        self.assertEqual(best.series, "Czerwona Kr?lowa")
        self.assertEqual(best.volume, (3, "00"))
        self.assertEqual(best.genre, "fantasy, science fiction")

    def test_lubimyczytac_candidates_fallback_to_raw_category_when_mapping_is_unknown(self) -> None:
        meta = make_meta("Unknown Book", creators=["Author Example"])
        search_page = (
            '<a class="authorAllBooks__singleTextTitle" href="/ksiazka/1/unknown-book"> Unknown Book </a>'
            '<div class="authorAllBooks__singleTextAuthor"><a href="/autor/1/author-example">Author Example</a></div>'
        )
        detail_page = '<a href="/kategoria/literatura-piekna/literatura-wspolczesna"> literatura wspolczesna </a>'

        with mock.patch.object(
            kod_v3,
            "online_text_query",
            side_effect=lambda url, timeout: detail_page if "/ksiazka/1/" in url else search_page,
        ):
            candidates = kod_v3.lubimyczytac_candidates(meta, 2.0)

        self.assertTrue(candidates)
        best = max(candidates, key=lambda item: item.score)
        self.assertEqual(best.genre, "literatura wspolczesna")

    def test_build_lubimyczytac_query_terms_uses_author_surname_and_title_fallbacks(self) -> None:
        meta = make_meta("01. Czerwona Krlowa", creators=["Victoria Aveyard"], title="01. Czerwona Krlowa")
        terms = kod_v3.build_lubimyczytac_query_terms(meta)
        self.assertEqual(terms[:2], ["Aveyard Czerwona Krlowa", "Victoria Czerwona Krlowa"])
        self.assertIn("Czerwona Krlowa", terms)

    def test_build_lubimyczytac_query_terms_do_not_cut_title_at_ampersand(self) -> None:
        meta = make_meta("Piekni & przekleci", title="Piekni & przekleci")
        terms = kod_v3.build_lubimyczytac_query_terms(meta)
        self.assertIn("Piekni przekleci", terms)

    def test_lubimyczytac_candidates_use_calibre_style_author_query_for_polish_titles(self) -> None:
        meta = make_meta("01. Czerwona Krlowa", creators=["Victoria Aveyard"], title="01. Czerwona Krlowa")
        search_page = (
            '<a class="authorAllBooks__singleTextTitle" href="/ksiazka/5182124/czerwona-krolowa"> Czerwona Krlowa </a>'
            '<div class="authorAllBooks__singleTextAuthor"><a href="/autor/72769/victoria-aveyard">Victoria Aveyard</a></div>'
        )
        detail_page = (
            '<span class="d-none d-sm-block mt-1"> Cykl:<a href="/cykl/4826/czerwona-krolowa"> Czerwona Krlowa (tom 1) </a></span>'
            '<a class="book__category d-sm-block d-none" href="/kategoria/beletrystyka/fantasy-science-fiction"> fantasy, science fiction </a>'
        )
        seen_urls: list[str] = []

        def fake_online_text_query(url: str, timeout: float) -> str:
            seen_urls.append(url)
            if "ksiazka/5182124" in url:
                return detail_page
            return search_page

        with mock.patch.object(kod_v3, "online_text_query", side_effect=fake_online_text_query):
            candidates = kod_v3.lubimyczytac_candidates(meta, 2.0)

        self.assertTrue(candidates)
        best = max(candidates, key=lambda item: item.score)
        self.assertEqual(best.series, "Czerwona Krlowa")
        self.assertEqual(best.volume, (1, "00"))
        self.assertTrue(
            any("phrase=Aveyard+Czerwona+Kr" in url for url in seen_urls),
            seen_urls,
        )

    def test_infer_record_handles_leading_author_title_with_bracket_index(self) -> None:
        meta = make_meta("[000] Centkiewiczowie Alina i Czesław - Okrutny biegun (1)")

        record = kod_v3.infer_record(meta, use_online=False, providers=[], timeout=1.0)

        self.assertEqual(record.author, "Centkiewicz Alina & Centkiewicz Czesław")
        self.assertEqual(record.series, "Standalone")
        self.assertIsNone(record.volume)
        self.assertEqual(record.title, "Okrutny biegun")

    def test_lubimyczytac_polish_match_fixes_leading_author_title_case(self) -> None:
        meta = make_meta("[000] Centkiewiczowie Alina i Czesław - Okrutny biegun (1)")
        online_candidates = [
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "Okrutny biegun",
                ["Czesław Centkiewicz", "Alina Centkiewicz"],
                [],
                320,
                "title-author-exact",
                genre="biografia",
            )
        ]

        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["lubimyczytac"], timeout=1.0)

        self.assertEqual(record.title, "Okrutny biegun")
        self.assertEqual(record.series, "Standalone")
        self.assertEqual(record.author, "Centkiewicz Czesław & Centkiewicz Alina")
        self.assertEqual(record.genre, "biografia")

    def test_online_validation_does_not_treat_hyphenated_title_part_as_author(self) -> None:
        meta = make_meta("[000] Doyle Arthur Conan - Tajemnica Doliny Boscombe")
        online_candidates = [
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "Tajemnica Doliny Boscombe",
                ["Tajemnica Doliny Boscombe"],
                [],
                180,
                "title-author-approx",
            ),
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "Tajemnica Doliny Boscombe",
                ["Arthur Conan Doyle"],
                [],
                320,
                "title-author-exact",
                genre="thriller",
            ),
        ]

        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["lubimyczytac"], timeout=1.0)

        self.assertEqual(record.author, "Doyle Arthur Conan")
        self.assertEqual(record.title, "Tajemnica Doliny Boscombe")
        self.assertEqual(
            record.filename,
            "Doyle Arthur Conan - Standalone - Tom 00.00 - Tajemnica Doliny Boscombe [thriller].epub",
        )

    def test_non_context_online_author_evidence_does_not_override_record(self) -> None:
        meta = make_meta("[000] Harlan Coben - Tylko jedno spojrzenie")
        online_candidates = [
            kod_v3.OnlineCandidate(
                "google-books",
                "google-books",
                "Tylko",
                ["spojrzenie Tylko jedno"],
                [],
                320,
                "title-author-exact",
                genre="thriller",
            ),
            kod_v3.OnlineCandidate(
                "lubimyczytac",
                "lubimyczytac",
                "Tylko jedno spojrzenie",
                ["Harlan Coben"],
                [],
                320,
                "title-author-exact",
                genre="thriller",
            ),
        ]

        with mock.patch.object(kod_v3, "fetch_online_candidates", return_value=online_candidates):
            record = kod_v3.infer_record(meta, use_online=True, providers=["google", "lubimyczytac"], timeout=1.0, online_mode="PL+")

        self.assertEqual(record.author, "Coben Harlan")
        self.assertEqual(record.title, "Tylko jedno spojrzenie")
        self.assertEqual(
            record.filename,
            "Coben Harlan - Standalone - Tom 00.00 - Tylko jedno spojrzenie [thriller].epub",
        )

    def test_online_cache_persists_to_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "cache.json"
            with mock.patch.object(kod_v3, "ONLINE_CACHE_PATH", cache_path):
                with kod_v3.ONLINE_CACHE_LOCK:
                    kod_v3.ONLINE_CACHE.clear()
                    kod_v3.ONLINE_CACHE["https://example.test"] = {"ok": True}
                kod_v3.save_online_cache()
                with kod_v3.ONLINE_CACHE_LOCK:
                    kod_v3.ONLINE_CACHE.clear()
                kod_v3.load_online_cache()
                with kod_v3.ONLINE_CACHE_LOCK:
                    self.assertEqual(kod_v3.ONLINE_CACHE["https://example.test"], {"ok": True})

    def test_save_online_cache_skips_large_text_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "cache.json"
            with mock.patch.object(kod_v3, "ONLINE_CACHE_PATH", cache_path):
                with kod_v3.ONLINE_CACHE_LOCK:
                    kod_v3.ONLINE_CACHE.clear()
                    kod_v3.ONLINE_CACHE["json:https://example.test"] = {"ok": True}
                    kod_v3.ONLINE_CACHE["text:https://example.test"] = "<html>big</html>"
                    kod_v3.ONLINE_CACHE_DIRTY = True
                    kod_v3.ONLINE_CACHE_PENDING_WRITES = 1
                kod_v3.save_online_cache()
                payload = json.loads(cache_path.read_text(encoding="utf-8"))
                self.assertEqual(payload, {"json:https://example.test": {"ok": True}})

    def test_load_online_cache_prunes_text_entries_and_marks_cache_dirty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "cache.json"
            cache_path.write_text(
                json.dumps(
                    {
                        "json:https://example.test": {"ok": True},
                        "text:https://example.test": "<html>big</html>",
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.object(kod_v3, "ONLINE_CACHE_PATH", cache_path):
                with kod_v3.ONLINE_CACHE_LOCK:
                    kod_v3.ONLINE_CACHE.clear()
                    kod_v3.ONLINE_CACHE_DIRTY = False
                    kod_v3.ONLINE_CACHE_PENDING_WRITES = 0
                kod_v3.load_online_cache()
                with kod_v3.ONLINE_CACHE_LOCK:
                    self.assertEqual(kod_v3.ONLINE_CACHE, {"json:https://example.test": {"ok": True}})
                    self.assertTrue(kod_v3.ONLINE_CACHE_DIRTY)
                    self.assertEqual(kod_v3.ONLINE_CACHE_PENDING_WRITES, 1)

    def test_reserve_lubimyczytac_request_delay_adds_non_uniform_spacing(self) -> None:
        with mock.patch.object(kod_v3.random, "uniform", side_effect=[2.5, 4.0]):
            with kod_v3.LUBIMYCZYTAC_RATE_LOCK:
                kod_v3.LUBIMYCZYTAC_NEXT_REQUEST_AT = 0.0
            first_delay = kod_v3.reserve_lubimyczytac_request_delay(now=10.0)
            second_delay = kod_v3.reserve_lubimyczytac_request_delay(now=11.0)
        self.assertEqual(first_delay, 0.0)
        self.assertEqual(second_delay, 1.5)
        with kod_v3.LUBIMYCZYTAC_RATE_LOCK:
            self.assertEqual(kod_v3.LUBIMYCZYTAC_NEXT_REQUEST_AT, 16.5)

    def test_build_online_request_uses_browser_headers_for_lubimyczytac(self) -> None:
        request = kod_v3.build_online_request("https://lubimyczytac.pl/szukaj/ksiazki?phrase=test")
        self.assertEqual(request.headers["Referer"], "https://lubimyczytac.pl/")
        self.assertIn("Mozilla/5.0", request.headers["User-agent"])

    def test_flush_online_cache_if_needed_respects_buffer_until_forced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "cache.json"
            with mock.patch.object(kod_v3, "ONLINE_CACHE_PATH", cache_path):
                with kod_v3.ONLINE_CACHE_LOCK:
                    kod_v3.ONLINE_CACHE.clear()
                    kod_v3.ONLINE_CACHE["https://example.test"] = {"ok": True}
                    kod_v3.ONLINE_CACHE_DIRTY = True
                    kod_v3.ONLINE_CACHE_PENDING_WRITES = 1
                    kod_v3.ONLINE_CACHE_LAST_SAVE = time.perf_counter()
                kod_v3.flush_online_cache_if_needed(force=False)
                self.assertFalse(cache_path.exists())
                kod_v3.flush_online_cache_if_needed(force=True)
                self.assertTrue(cache_path.exists())

    def test_online_fetch_caches_failures_temporarily(self) -> None:
        with mock.patch.object(kod_v3.ONLINE_HTTP_OPENER, "open", side_effect=OSError("timeout")) as opener:
            self.assertIsNone(kod_v3.online_fetch("https://example.test", 1.0, kind="json"))
            self.assertIsNone(kod_v3.online_fetch("https://example.test", 1.0, kind="json"))
        self.assertEqual(opener.call_count, 1)

    def test_save_online_cache_keeps_dirty_flags_on_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "cache.json"
            with mock.patch.object(kod_v3, "ONLINE_CACHE_PATH", cache_path):
                with kod_v3.ONLINE_CACHE_LOCK:
                    kod_v3.ONLINE_CACHE.clear()
                    kod_v3.ONLINE_CACHE["json:https://example.test"] = {"ok": True}
                    kod_v3.ONLINE_CACHE_DIRTY = True
                    kod_v3.ONLINE_CACHE_PENDING_WRITES = 2
                with mock.patch.object(Path, "write_text", side_effect=OSError("disk-full")):
                    with self.assertRaises(OSError):
                        kod_v3.save_online_cache()
                with kod_v3.ONLINE_CACHE_LOCK:
                    self.assertTrue(kod_v3.ONLINE_CACHE_DIRTY)
                    self.assertEqual(kod_v3.ONLINE_CACHE_PENDING_WRITES, 2)

    def test_apply_flag_defaults_to_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            (folder / "Book 1 - Author.epub").touch()
            code, lines = kod_v3.run_job(
                folder,
                apply_changes=False,
                use_online=False,
                providers=[],
                timeout=1.0,
                limit=0,
            )
            self.assertEqual(code, 0)
            self.assertIn("MODE=DRY-RUN", lines)
            self.assertTrue(any(line.startswith("PROFILE_TOTAL_MS=") for line in lines))

    def test_run_job_parallelizes_infer_record_across_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            for index in range(6):
                (folder / f"Book {index}.epub").touch()
            thread_ids: set[int] = set()
            lock = threading.Lock()

            def fake_infer(meta, use_online, providers, timeout):
                time.sleep(0.05)
                with lock:
                    thread_ids.add(threading.get_ident())
                return kod_v3.BookRecord(
                    path=meta.path,
                    author="Author",
                    series="Series",
                    volume=(1, "00"),
                    title=meta.stem,
                    source="test",
                    identifiers=[],
                    notes=[],
                    confidence=95,
                    review_reasons=[],
                    decision_reasons=[],
                )

            with mock.patch.object(kod_v3, "infer_record", side_effect=fake_infer):
                code, _ = kod_v3.run_job(
                    folder,
                    apply_changes=False,
                    use_online=False,
                    providers=[],
                    timeout=1.0,
                    limit=0,
                )
            self.assertEqual(code, 0)
            self.assertGreater(len(thread_ids), 1)

    def test_to_last_first_keeps_multiword_surname_particles(self) -> None:
        self.assertEqual(kod_v3.to_last_first("Ludwig van Beethoven"), "van Beethoven Ludwig")

    def test_extract_authors_sorts_coauthors_deterministically(self) -> None:
        left = kod_v3.extract_authors([], "Alex Beaumont & Cale Plamann")
        right = kod_v3.extract_authors([], "Cale Plamann & Alex Beaumont")
        self.assertEqual(left, "Beaumont Alex & Plamann Cale")
        self.assertEqual(right, left)

    def test_build_undo_plan_uses_raw_csv_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            report_path = folder / "log.csv"
            with report_path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.writer(handle, delimiter=";")
                writer.writerow(["source_name", "target_name", "mode", "execution_status"])
                writer.writerow([" Book-.epub ", " Target-.epub ", "apply", "renamed"])
            plan = kod_v3.build_undo_plan(report_path, folder)
            self.assertEqual(plan.moves[0].source.name, " Target-.epub ")
            self.assertEqual(plan.moves[0].destination.name, " Book-.epub ")

    def test_build_undo_plan_copy_uses_temp_backup_for_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            report_path = folder / "log.csv"
            with report_path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.writer(handle, delimiter=";")
                writer.writerow(["source_name", "target_name", "source_folder", "target_folder", "operation", "mode", "execution_status"])
                writer.writerow(["Source.epub", "Target.epub", str(folder), str(folder), "copy", "apply", "copied"])
            plan = kod_v3.build_undo_plan(report_path, folder)
            self.assertEqual(plan.moves[0].operation, "delete")
            self.assertIsNotNone(plan.moves[0].temp)
            assert plan.moves[0].temp is not None
            self.assertIn("__tmp_undo_delete_", plan.moves[0].temp.name)

    def test_execute_moves_delete_restores_files_when_delete_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            source_a = folder / "A.epub"
            source_b = folder / "B.epub"
            source_a.write_text("A", encoding="utf-8")
            source_b.write_text("B", encoding="utf-8")
            move_a = kod_v3.RenameMove(source_a, folder / "__tmp_undo_delete_0001.epub", folder / "ignored-A.epub", None, "delete")
            move_b = kod_v3.RenameMove(source_b, folder / "__tmp_undo_delete_0002.epub", folder / "ignored-B.epub", None, "delete")

            original_unlink = Path.unlink

            def flaky_unlink(path_obj: Path, *args, **kwargs):
                if path_obj == source_b:
                    raise OSError("locked")
                return original_unlink(path_obj, *args, **kwargs)

            with mock.patch.object(Path, "unlink", new=flaky_unlink):
                errors = kod_v3.execute_moves([move_a, move_b])

            self.assertTrue(errors)
            self.assertTrue(source_a.exists())
            self.assertTrue(source_b.exists())
            self.assertFalse(move_a.temp.exists())
            self.assertFalse(move_b.temp.exists())

    def test_execute_moves_copy_assigns_current_timestamp_to_destination(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            source = folder / "A.epub"
            destination = folder / "B.epub"
            source.write_text("A", encoding="utf-8")
            os.utime(source, (946684800, 946684800))

            errors = kod_v3.execute_moves([kod_v3.RenameMove(source, None, destination, None, "copy")])

            self.assertEqual(errors, [])
            self.assertTrue(destination.exists())
            self.assertGreater(destination.stat().st_mtime, 946684800)

    def test_execute_moves_rename_assigns_current_timestamp_to_destination(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            source = folder / "A.epub"
            temp = folder / "__tmp_rename.epub"
            destination = folder / "B.epub"
            source.write_text("A", encoding="utf-8")
            os.utime(source, (946684800, 946684800))

            errors = kod_v3.execute_moves([kod_v3.RenameMove(source, temp, destination, None, "rename")])

            self.assertEqual(errors, [])
            self.assertTrue(destination.exists())
            self.assertGreater(destination.stat().st_mtime, 946684800)

    def test_review_required_records_are_processed_in_apply_mode(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp, tempfile.TemporaryDirectory() as archive_tmp:
            source_folder = Path(src_tmp)
            destination_folder = Path(dst_tmp)
            archive_folder = Path(archive_tmp)
            source = source_folder / "A Touch of Power Omnibus -- Jay Boyce.epub"
            source.touch()
            code, lines = kod_v3.run_job(
                source_folder,
                destination_folder=destination_folder,
                archive_folder=archive_folder,
                apply_changes=True,
                use_online=False,
                providers=[],
                timeout=1.0,
                limit=0,
            )
            self.assertEqual(code, 0)
            self.assertTrue(any("TO_WRITE=2" in line for line in lines))
            self.assertTrue(any("WRITTEN=2" in line for line in lines))
            report_line = next(line for line in lines if line.startswith("REPORT="))
            report_path = Path(report_line.split("=", 1)[1])
            with report_path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle, delimiter=";"))
            self.assertEqual(rows[0]["execution_status"], "copied+archived")
            self.assertFalse(source.exists())
            self.assertTrue((archive_folder / source.name).exists())

    def test_apply_report_contains_actual_execution_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            source = folder / "Advent (Red Mage Book 1) -- Xander Boyce.mobi"
            source.touch()
            code, lines = kod_v3.run_job(
                folder,
                apply_changes=True,
                use_online=False,
                providers=[],
                timeout=1.0,
                limit=0,
            )
            self.assertEqual(code, 0)
            report_line = next(line for line in lines if line.startswith("REPORT="))
            report_path = Path(report_line.split("=", 1)[1])
            with report_path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle, delimiter=";"))
            self.assertTrue(rows)
            self.assertEqual(rows[0]["execution_status"], "renamed")
            self.assertEqual(rows[0]["online_checked"], "no")
            self.assertEqual(rows[0]["online_applied"], "no")

    def test_apply_copy_archive_mode_writes_to_destination_and_moves_source_to_archive(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp, tempfile.TemporaryDirectory() as archive_tmp:
            source_folder = Path(src_tmp)
            destination_folder = Path(dst_tmp)
            archive_folder = Path(archive_tmp)
            source = source_folder / "Advent (Red Mage Book 1) -- Xander Boyce.mobi"
            source.touch()
            code, lines = kod_v3.run_job(
                source_folder,
                destination_folder=destination_folder,
                archive_folder=archive_folder,
                apply_changes=True,
                use_online=False,
                providers=[],
                timeout=1.0,
                limit=0,
            )
            self.assertEqual(code, 0)
            self.assertFalse(source.exists())
            copied = destination_folder / "Boyce Xander - Red Mage - Tom 01.00 - Advent.mobi"
            self.assertTrue(copied.exists())
            self.assertTrue((archive_folder / source.name).exists())
            self.assertIn("OPERATION=COPY+ARCHIVE", lines)
            self.assertIn("INFER_WORKERS=1", lines)
            self.assertIn("ONLINE_HTTP_SLOTS=4", lines)

    def test_apply_copy_archive_streams_first_file_before_batch_finishes(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp, tempfile.TemporaryDirectory() as archive_tmp:
            source_folder = Path(src_tmp)
            destination_folder = Path(dst_tmp)
            archive_folder = Path(archive_tmp)
            (source_folder / "Alpha.epub").touch()
            (source_folder / "Beta.epub").touch()

            def fake_infer(meta, use_online, providers, timeout):
                if meta.path.name == "Beta.epub":
                    time.sleep(0.6)
                return kod_v3.BookRecord(
                    path=meta.path,
                    author="Author",
                    series="Series",
                    volume=(1, "00"),
                    title=meta.path.stem,
                    source="test",
                    identifiers=[],
                    notes=[],
                    confidence=90,
                    review_reasons=[],
                    decision_reasons=[],
                )

            result: list[tuple[int, list[str]]] = []

            def worker() -> None:
                result.append(
                    kod_v3.run_job(
                        source_folder,
                        destination_folder=destination_folder,
                        archive_folder=archive_folder,
                        apply_changes=True,
                        use_online=False,
                        providers=[],
                        timeout=1.0,
                        limit=0,
                    )
                )

            with mock.patch.object(kod_v3, "infer_record", side_effect=fake_infer):
                thread = threading.Thread(target=worker)
                thread.start()
                alpha_target = destination_folder / "Author - Series - Tom 01.00 - Alpha.epub"
                deadline = time.time() + 0.4
                while time.time() < deadline and not alpha_target.exists():
                    time.sleep(0.02)
                self.assertTrue(alpha_target.exists())
                thread.join()

            self.assertEqual(result[0][0], 0)

    def test_copy_archive_report_contains_archive_operation(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp, tempfile.TemporaryDirectory() as archive_tmp:
            source_folder = Path(src_tmp)
            destination_folder = Path(dst_tmp)
            archive_folder = Path(archive_tmp)
            source = source_folder / "Advent (Red Mage Book 1) -- Xander Boyce.mobi"
            source.touch()
            code, lines = kod_v3.run_job(
                source_folder,
                destination_folder=destination_folder,
                archive_folder=archive_folder,
                apply_changes=True,
                use_online=False,
                providers=[],
                timeout=1.0,
                limit=0,
            )
            self.assertEqual(code, 0)
            report_line = next(line for line in lines if line.startswith("REPORT="))
            report_path = Path(report_line.split("=", 1)[1])
            with report_path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle, delimiter=";"))
            self.assertEqual(rows[0]["operation"], "copy+archive")
            self.assertEqual(rows[0]["execution_status"], "copied+archived")
            self.assertEqual(rows[0]["target_folder"], str(destination_folder))
            self.assertEqual(rows[0]["archive_source_folder"], str(archive_folder))
            self.assertEqual(rows[0]["archive_source_name"], source.name)

    def test_existing_destination_collision_moves_new_file_to_dubel_subfolder(self) -> None:
        with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as dst_tmp, tempfile.TemporaryDirectory() as archive_tmp:
            source_folder = Path(src_tmp)
            destination_folder = Path(dst_tmp)
            archive_folder = Path(archive_tmp)
            source = source_folder / "Advent (Red Mage Book 1) -- Xander Boyce.mobi"
            source.touch()
            existing = destination_folder / "Boyce Xander - Red Mage - Tom 01.00 - Advent.mobi"
            existing.parent.mkdir(parents=True, exist_ok=True)
            existing.touch()

            code, _ = kod_v3.run_job(
                source_folder,
                destination_folder=destination_folder,
                archive_folder=archive_folder,
                apply_changes=True,
                use_online=False,
                providers=[],
                timeout=1.0,
                limit=0,
            )

            self.assertEqual(code, 0)
            self.assertFalse(source.exists())
            self.assertTrue(existing.exists())
            self.assertTrue((destination_folder / "dubel" / existing.name).exists())
            self.assertTrue((archive_folder / source.name).exists())

    def test_filename_for_folder_respects_destination_length_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source_folder = Path(tmp) / "src"
            destination_folder = Path(tmp) / ("dest_" + ("x" * 120))
            source_folder.mkdir()
            record = kod_v3.BookRecord(
                path=source_folder / "source.epub",
                author="Author",
                series="Series",
                volume=(1, "00"),
                title="Very Long Title " * 20,
                source="test",
                identifiers=[],
                notes=[],
                confidence=95,
                review_reasons=[],
                decision_reasons=[],
            )
            record.output_folder = source_folder
            source_name = record.filename
            record.output_folder = destination_folder
            destination_name = record.filename
            self.assertLessEqual(len(str(destination_folder / destination_name)), 240)
            self.assertLessEqual(len(destination_name), len(source_name))

    def test_dedupe_moves_existing_collision_to_dubel_folder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            conflict_name = "Author - Series - Tom 01.00 - Title.epub"
            (folder / conflict_name).touch()
            record = kod_v3.BookRecord(
                path=folder / "source.epub",
                author="Author",
                series="Series",
                volume=(1, "00"),
                title="Title",
                source="test",
                identifiers=[],
                notes=[],
                confidence=95,
                review_reasons=[],
                decision_reasons=[],
            )
            deduped = kod_v3.dedupe_destinations([record], folder)
            self.assertEqual(deduped[0].filename, "Author - Series - Tom 01.00 - Title.epub")
            self.assertEqual(deduped[0].output_folder, folder / "dubel")


if __name__ == "__main__":
    unittest.main()
