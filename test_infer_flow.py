from pathlib import Path
import unittest

import infer_flow
from domain_naming import BookRecord
from models_core import EpubMetadata, OnlineVerification


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


if __name__ == "__main__":
    unittest.main()
