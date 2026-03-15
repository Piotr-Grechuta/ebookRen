import csv
import tempfile
import unittest
from pathlib import Path

import author_catalog


FIELDNAMES = [
    "source",
    "author_raw",
    "author_first_last",
    "author_last_first",
    "title_example",
    "language",
    "source_author_id",
    "source_work_id",
    "source_url",
    "confidence",
    "notes",
]


class AuthorCatalogTests(unittest.TestCase):
    def test_load_author_catalog_resolves_aliases_and_splits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "author_patterns.csv"
            with path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
                writer.writeheader()
                writer.writerow(
                    {
                        "source": "lubimyczytac | openlibrary",
                        "author_raw": "Aguirre Ann",
                        "author_first_last": "Ann Aguirre",
                        "author_last_first": "Aguirre Ann",
                        "title_example": "Enklawa",
                        "language": "pl | en",
                        "source_author_id": "lubimyczytac:1 | openlibrary:OL1A",
                        "source_work_id": "",
                        "source_url": "https://lubimyczytac.pl/autor/1/ann-aguirre | https://openlibrary.org/authors/OL1A",
                        "confidence": "high",
                        "notes": "test",
                    }
                )
            catalog = author_catalog.load_author_catalog(path)

        self.assertEqual(catalog.resolve("Ann Aguirre"), "Ann Aguirre")
        self.assertEqual(catalog.resolve("Aguirre Ann"), "Ann Aguirre")
        self.assertTrue(catalog.is_known("Aguirre Ann"))
        self.assertEqual(catalog.split_prefix("Aguirre Ann Enklawa"), ("Ann Aguirre", "Enklawa"))
        self.assertEqual(catalog.split_suffix("Enklawa Ann Aguirre"), ("Ann Aguirre", "Enklawa"))

    def test_load_author_catalog_resolves_multi_author_segments_with_noise(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "author_patterns.csv"
            with path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
                writer.writeheader()
                writer.writerow(
                    {
                        "source": "lubimyczytac",
                        "author_raw": "Ludlum Robert",
                        "author_first_last": "Robert Ludlum",
                        "author_last_first": "Ludlum Robert",
                        "title_example": "",
                        "language": "pl",
                        "source_author_id": "lubimyczytac:1",
                        "source_work_id": "",
                        "source_url": "https://lubimyczytac.pl/autor/1/robert-ludlum",
                        "confidence": "medium",
                        "notes": "test",
                    }
                )
                writer.writerow(
                    {
                        "source": "lubimyczytac | openlibrary",
                        "author_raw": "Lustbader Eric van",
                        "author_first_last": "Eric van Lustbader",
                        "author_last_first": "Lustbader Eric van",
                        "title_example": "",
                        "language": "pl | en",
                        "source_author_id": "lubimyczytac:2 | openlibrary:OL2A",
                        "source_work_id": "",
                        "source_url": "https://lubimyczytac.pl/autor/2/eric-van-lustbader | https://openlibrary.org/authors/OL2A",
                        "confidence": "high",
                        "notes": "test",
                    }
                )
            catalog = author_catalog.load_author_catalog(path)

        self.assertEqual(
            catalog.resolve_authors("Robert Ludlum & Eric van Lustbader"),
            ["Robert Ludlum", "Eric van Lustbader"],
        )
        self.assertEqual(
            catalog.resolve_authors("Ludlum Robert Van Lustbader Eric"),
            ["Robert Ludlum", "Eric van Lustbader"],
        )
        self.assertEqual(
            catalog.resolve_authors("Bourne'a Ludlum Robert Van Lustbader Eric"),
            ["Robert Ludlum", "Eric van Lustbader"],
        )


if __name__ == "__main__":
    unittest.main()
