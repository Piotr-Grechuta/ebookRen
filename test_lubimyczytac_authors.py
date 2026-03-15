import unittest
from unittest import mock

import lubimyczytac_authors


class LubimyczytacAuthorsTests(unittest.TestCase):
    def test_parse_book_page_extracts_title_and_authors(self) -> None:
        page = """
        <html>
          <head><meta property="og:title" content="Okrutny biegun - Lubimyczytac.pl"></head>
          <body>
            <h1 class="book__title">Okrutny biegun</h1>
            <div class="book__authors">
              <a href="/autor/1/czeslaw-centkiewicz">Czesław Centkiewicz</a>
              <a href="/autor/2/alina-centkiewicz">Alina Centkiewicz</a>
            </div>
          </body>
        </html>
        """

        payload = lubimyczytac_authors.parse_book_page(page)

        self.assertEqual(payload["title"], "Okrutny biegun")
        self.assertEqual(payload["authors"], ["Czesław Centkiewicz", "Alina Centkiewicz"])

    def test_search_book_authors_uses_existing_search_parser(self) -> None:
        search_page = (
            '<a class="authorAllBooks__singleTextTitle" href="/ksiazka/1/okrutny-biegun"> Okrutny biegun </a>'
            '<div class="authorAllBooks__singleTextAuthor">'
            '<a href="/autor/1/czeslaw-centkiewicz">Czesław Centkiewicz</a>'
            '<a href="/autor/2/alina-centkiewicz">Alina Centkiewicz</a>'
            "</div>"
        )

        with mock.patch.object(lubimyczytac_authors.runtime, "online_text_query", return_value=search_page):
            results = lubimyczytac_authors.search_book_authors("okrutny biegun", 2.0, limit=3)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["title"], "Okrutny biegun")
        self.assertEqual(results[0]["authors"], ["Czesław Centkiewicz", "Alina Centkiewicz"])
        self.assertEqual(results[0]["url"], "https://lubimyczytac.pl/ksiazka/1/okrutny-biegun")

    def test_is_book_url_recognizes_lubimyczytac_book_page(self) -> None:
        self.assertTrue(lubimyczytac_authors.is_book_url("https://lubimyczytac.pl/ksiazka/215697/okrutny-biegun"))
        self.assertFalse(lubimyczytac_authors.is_book_url("okrutny biegun"))


if __name__ == "__main__":
    unittest.main()
