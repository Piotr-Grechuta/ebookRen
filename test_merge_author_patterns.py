import csv
import tempfile
import unittest
from pathlib import Path

import merge_author_patterns


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


class MergeAuthorPatternsTests(unittest.TestCase):
    def test_strip_name_noise_removes_year_tokens_and_rejects_numeric_only_rows(self) -> None:
        self.assertEqual(merge_author_patterns.strip_name_noise("Åke 1907-1991 Holmberg"), "Åke Holmberg")
        self.assertEqual(
            merge_author_patterns.strip_name_noise("-1834 (Samuel E. Smith )"),
            "Samuel E Smith",
        )
        self.assertFalse(merge_author_patterns.is_plausible_author_name("0870005719"))
        self.assertFalse(merge_author_patterns.is_plausible_author_name("0.0"))
        self.assertFalse(merge_author_patterns.is_plausible_author_name("-1927 joint author"))
        self.assertFalse(
            merge_author_patterns.is_plausible_author_name("Goodale Steam Car Brake Manufacturing Company")
        )
        self.assertFalse(
            merge_author_patterns.is_plausible_author_name("Gujarat India State Health Society")
        )
        self.assertFalse(
            merge_author_patterns.is_plausible_author_name("Guinness Encyclopedia of Popular Music")
        )
        self.assertTrue(merge_author_patterns.is_plausible_author_name("Åke Holmberg"))

    def test_main_merges_duplicate_author_keys_without_loading_rows_in_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            input_path = Path(tmp) / "authors.csv"
            output_path = Path(tmp) / "merged.csv"
            db_path = Path(tmp) / "merge.sqlite3"
            with input_path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
                writer.writeheader()
                writer.writerow(
                    {
                        "source": "openlibrary",
                        "author_raw": "Ake Holmberg",
                        "author_first_last": "Ake Holmberg",
                        "author_last_first": "Holmberg Ake",
                        "title_example": "",
                        "language": "",
                        "source_author_id": "OL1989704A",
                        "source_work_id": "",
                        "source_url": "https://openlibrary.org/authors/OL1989704A",
                        "confidence": "high",
                        "notes": "authors-dump revision=4",
                    }
                )
                writer.writerow(
                    {
                        "source": "openlibrary",
                        "author_raw": "Åke 1907-1991 Holmberg",
                        "author_first_last": "Åke 1907-1991 Holmberg",
                        "author_last_first": "Holmberg Åke 1907-1991",
                        "title_example": "",
                        "language": "",
                        "source_author_id": "OL2A",
                        "source_work_id": "",
                        "source_url": "https://openlibrary.org/authors/OL2A",
                        "confidence": "high",
                        "notes": "authors-dump revision=2",
                    }
                )
                writer.writerow(
                    {
                        "source": "openlibrary",
                        "author_raw": "0870005719",
                        "author_first_last": "0870005719",
                        "author_last_first": "0870005719",
                        "title_example": "",
                        "language": "",
                        "source_author_id": "OL3A",
                        "source_work_id": "",
                        "source_url": "https://openlibrary.org/authors/OL3A",
                        "confidence": "high",
                        "notes": "authors-dump revision=3",
                    }
                )
                writer.writerow(
                    {
                        "source": "lubimyczytac",
                        "author_raw": "Holmberg Ake",
                        "author_first_last": "Ake Holmberg",
                        "author_last_first": "Holmberg Ake",
                        "title_example": "",
                        "language": "pl",
                        "source_author_id": "44529",
                        "source_work_id": "",
                        "source_url": "https://lubimyczytac.pl/autor/44529/ake-holmberg",
                        "confidence": "medium",
                        "notes": "authors-list-html page=1",
                    }
                )
                writer.writerow(
                    {
                        "source": "openlibrary",
                        "author_raw": "Jane Doe",
                        "author_first_last": "Jane Doe",
                        "author_last_first": "Doe Jane",
                        "title_example": "",
                        "language": "",
                        "source_author_id": "OL1A",
                        "source_work_id": "",
                        "source_url": "https://openlibrary.org/authors/OL1A",
                        "confidence": "high",
                        "notes": "authors-dump revision=1",
                    }
                )

            exit_code = merge_author_patterns.main(
                ["--input", str(input_path), "--output", str(output_path), "--temp-db", str(db_path)]
            )

            self.assertEqual(exit_code, 0)
            with output_path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 2)
        merged = next(row for row in rows if row["author_first_last"] == "Ake Holmberg")
        self.assertEqual(merged["source"], "lc | ol")
        self.assertEqual(merged["author_raw"], "")


if __name__ == "__main__":
    unittest.main()
