import re
import unittest

import infer_core
import providers_online
from models_core import LubimyczytacResult, OnlineCandidate
from runtime_config import SERIES_ONLY_PAREN_INDEX_RE


def clean(text: str | None) -> str:
    return infer_core.clean(text)


class ProvidersOnlineTests(unittest.TestCase):
    def test_lubimyczytac_search_parser_extracts_series_and_volume(self) -> None:
        parser = providers_online.LubimyczytacSearchParserBase(
            clean=clean,
            clean_series=clean,
            parse_volume_parts=infer_core.parse_volume_parts,
            series_only_paren_index_re=SERIES_ONLY_PAREN_INDEX_RE,
            result_type=LubimyczytacResult,
        )
        parser.feed(
            '<a class="authorAllBooks__singleTextTitle" href="/x">KrÄ‚Ĺ‚lewska klatka</a>'
            '<div class="authorAllBooks__singleTextAuthor"><a href="/a">Victoria Aveyard</a></div>'
            '<div class="listLibrary__info listLibrary__info--cycles"><a href="/c">Czerwona KrÄ‚Ĺ‚lowa (tom 3)</a></div>'
        )
        parser.close()

        self.assertEqual(
            parser.results,
            [
                LubimyczytacResult(
                    "KrÄ‚Ĺ‚lewska klatka",
                    ["Victoria Aveyard"],
                    "Czerwona KrÄ‚Ĺ‚lowa",
                    (3, "00"),
                    "/x",
                    cycle_source="search",
                )
            ],
        )

    def test_parse_lubimyczytac_detail_page_extracts_polish_cycle_and_categories(self) -> None:
        page = (
            '<span class="d-none d-sm-block mt-1"> Cykl:'
            '<a href="/cykl/4826/czerwona-krolowa"> Czerwona KrÄ‚Ĺ‚lowa (tom 1) </a></span>'
            '<a class="book__category d-sm-block d-none" href="/kategoria/fantasy"> fantasy </a>'
            '<a class="book__category d-sm-block d-none" href="/kategoria/science-fiction"> science fiction </a>'
        )
        series, volume, genres = providers_online.parse_lubimyczytac_detail_page(
            page,
            clean=clean,
            strip_html_tags=lambda text: clean(re.sub(r"<[^>]+>", " ", text or "")),
            clean_series=clean,
            parse_volume_parts=infer_core.parse_volume_parts,
            series_only_paren_index_re=SERIES_ONLY_PAREN_INDEX_RE,
        )

        self.assertEqual(series, "Czerwona KrÄ‚Ĺ‚lowa")
        self.assertEqual(volume, (1, "00"))
        self.assertEqual(genres, ["fantasy", "science fiction"])

    def test_parse_lubimyczytac_detail_page_ignores_footer_categories_when_book_category_exists(self) -> None:
        page = (
            '<a class="book__category d-sm-block d-none" href="/kategoria/beletrystyka/kryminal-sensacja-thriller">'
            " kryminaĹ‚, sensacja, thriller </a>"
            '<div class="footer__popular">'
            '<a class="footer__popular-link" href="/kategoria/beletrystyka/fantasy-science-fiction">'
            " fantasy, science fiction </a>"
            '<a class="footer__popular-link" href="/kategoria/beletrystyka/horror"> horror </a>'
            "</div>"
        )
        series, volume, genres = providers_online.parse_lubimyczytac_detail_page(
            page,
            clean=clean,
            strip_html_tags=lambda text: clean(re.sub(r"<[^>]+>", " ", text or "")),
            clean_series=clean,
            parse_volume_parts=infer_core.parse_volume_parts,
            series_only_paren_index_re=SERIES_ONLY_PAREN_INDEX_RE,
        )

        self.assertEqual(series, "")
        self.assertIsNone(volume)
        self.assertEqual(genres, ["kryminaĹ‚, sensacja, thriller"])

    def test_fetch_online_candidates_prefers_lubimyczytac_first_in_pl_mode(self) -> None:
        calls: list[str] = []

        def provider(name: str, results: list[OnlineCandidate]):
            def _provider(meta, timeout):
                del meta, timeout
                calls.append(name)
                return results

            return _provider

        providers_online.fetch_online_candidates(
            object(),
            ["google", "lubimyczytac", "openlibrary"],
            2.0,
            online_mode="PL",
            provider_functions={
                "google": provider("google", []),
                "openlibrary": provider("openlibrary", []),
                "lubimyczytac": provider(
                    "lubimyczytac",
                    [OnlineCandidate("lubimyczytac", "lubimyczytac", "Book", ["Author"], [], 286, "title-author-exact")],
                ),
            },
        )

        self.assertEqual(calls, ["lubimyczytac"])

    def test_fetch_online_candidates_skips_lubimyczytac_after_strong_google_hit_in_en_mode(self) -> None:
        calls: list[str] = []

        def provider(name: str, results: list[OnlineCandidate]):
            def _provider(meta, timeout):
                del meta, timeout
                calls.append(name)
                return results

            return _provider

        providers_online.fetch_online_candidates(
            object(),
            ["google", "lubimyczytac", "openlibrary"],
            2.0,
            online_mode="EN",
            provider_functions={
                "google": provider(
                    "google",
                    [OnlineCandidate("google-books", "google-books", "Book", ["Author"], [], 338, "title-author-exact")],
                ),
                "openlibrary": provider("openlibrary", []),
                "lubimyczytac": provider(
                    "lubimyczytac",
                    [OnlineCandidate("lubimyczytac", "lubimyczytac", "Book", ["Author"], [], 286, "title-author-exact")],
                ),
            },
        )

        self.assertEqual(calls, ["google"])


if __name__ == "__main__":
    unittest.main()
