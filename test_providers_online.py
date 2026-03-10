import unittest
import re

import infer_core
import providers_online
from models_core import LubimyczytacResult
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
            '<a class="authorAllBooks__singleTextTitle" href="/x">Królewska klatka</a>'
            '<div class="authorAllBooks__singleTextAuthor"><a href="/a">Victoria Aveyard</a></div>'
            '<div class="listLibrary__info listLibrary__info--cycles"><a href="/c">Czerwona Królowa (tom 3)</a></div>'
        )
        parser.close()

        self.assertEqual(
            parser.results,
            [LubimyczytacResult("Królewska klatka", ["Victoria Aveyard"], "Czerwona Królowa", (3, "00"), "/x")],
        )

    def test_parse_lubimyczytac_detail_page_extracts_polish_cycle_and_categories(self) -> None:
        page = (
            '<span class="d-none d-sm-block mt-1"> Cykl:'
            '<a href="/cykl/4826/czerwona-krolowa"> Czerwona Królowa (tom 1) </a></span>'
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

        self.assertEqual(series, "Czerwona Królowa")
        self.assertEqual(volume, (1, "00"))
        self.assertEqual(genres, ["fantasy", "science fiction"])


if __name__ == "__main__":
    unittest.main()
