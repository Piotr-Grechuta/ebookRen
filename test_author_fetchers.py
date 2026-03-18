import unittest

import fetch_lubimyczytac_author_patterns
import fetch_openlibrary_author_patterns


class OpenLibraryAuthorFetcherTests(unittest.TestCase):
    def test_parse_dump_line_extracts_author_row(self) -> None:
        line = (
            '/type/author\t/authors/OL1989704A\t4\t2010-07-10T00:13:10.903058\t'
            '{"name":"\\u00c5ke Holmberg","key":"/authors/OL1989704A","type":{"key":"/type/author"}}\n'
        )

        row = fetch_openlibrary_author_patterns.parse_dump_line(line)

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["author_first_last"], "\u00c5ke Holmberg")
        self.assertEqual(row["author_last_first"], "Holmberg \u00c5ke")
        self.assertEqual(row["source_author_id"], "OL1989704A")
        self.assertEqual(row["source_url"], "https://openlibrary.org/authors/OL1989704A")
        self.assertIn("revision=4", row["notes"])


class LubimyczytacAuthorFetcherTests(unittest.TestCase):
    def test_collect_author_rows_stops_on_404_even_if_page_html_repeats_authors(self) -> None:
        page_html = (
            '<a class="authorAllBooks__singleTextAuthor" href="/autor/44529/ake-holmberg">'
            "Holmberg \u00c5ke"
            "</a>"
        )
        pages = {
            1: (200, page_html),
            2: (404, page_html),
        }

        rows, page_stats = fetch_lubimyczytac_author_patterns.collect_author_rows(
            fetch_page=lambda page_number: pages[page_number],
            start_page=1,
            end_page=None,
            max_pages_without_new_authors=2,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["author_first_last"], "Ake Holmberg")
        self.assertEqual(page_stats, [(1, 200, 1, 1), (2, 404, 0, 0)])

    def test_collect_author_rows_stops_after_stale_pages_when_unbounded(self) -> None:
        page_html = (
            '<a class="authorAllBooks__singleTextAuthor" href="/autor/44529/ake-holmberg">'
            "Holmberg \u00c5ke"
            "</a>"
        )
        pages = {
            1: (200, page_html),
            2: (200, page_html),
            3: (200, page_html),
        }

        rows, page_stats = fetch_lubimyczytac_author_patterns.collect_author_rows(
            fetch_page=lambda page_number: pages[page_number],
            start_page=1,
            end_page=None,
            max_pages_without_new_authors=2,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(page_stats, [(1, 200, 1, 1), (2, 200, 1, 0), (3, 200, 1, 0)])


if __name__ == "__main__":
    unittest.main()
