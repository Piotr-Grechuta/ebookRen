import unittest
from pathlib import Path

import infer_core
import local_parser


class LocalParserTests(unittest.TestCase):
    def test_parse_hybrid_local_extracts_title_author_and_volume_from_delimited_name(self) -> None:
        meta = type(
            "Meta",
            (),
            {
                "path": Path("x.epub"),
                "stem": "(03) - Burzowe Kocie - Maja Lidia Kossakowska",
                "segments": ["(03) - Burzowe Kocie - Maja Lidia Kossakowska"],
                "core": "(03) - Burzowe Kocie - Maja Lidia Kossakowska",
                "title": "",
                "creators": [],
                "identifiers": [],
                "subjects": [],
            },
        )()

        parsed = local_parser.parse_hybrid_local(
            meta,
            clean=infer_core.clean,
            clean_author_segment=infer_core.clean,
            looks_like_author_segment=lambda text: bool(text and any(char.isalpha() for char in text)),
            strip_leading_title_index=infer_core.clean,
            parse_volume_parts=infer_core.parse_volume_parts,
        )

        self.assertEqual(parsed.title_hint, "Burzowe Kocie")
        self.assertEqual(parsed.author_hint, "Maja Lidia Kossakowska")
        self.assertEqual(parsed.volume_hint, (3, "00"))
        self.assertEqual(parsed.source, "hybrid:delimited-index-title-author")

    def test_parse_hybrid_local_uses_catalog_prefix_for_compact_name(self) -> None:
        meta = type(
            "Meta",
            (),
            {
                "path": Path("x.epub"),
                "stem": "Aguirre Ann Enklawa",
                "segments": ["Aguirre Ann Enklawa"],
                "core": "Aguirre Ann Enklawa",
                "title": "",
                "creators": [],
                "identifiers": [],
                "subjects": [],
            },
        )()

        parsed = local_parser.parse_hybrid_local(
            meta,
            clean=infer_core.clean,
            clean_author_segment=infer_core.clean,
            looks_like_author_segment=lambda text: bool(text and any(char.isalpha() for char in text)),
            strip_leading_title_index=infer_core.clean,
            parse_volume_parts=infer_core.parse_volume_parts,
            resolve_known_author=lambda text: "Ann Aguirre" if infer_core.author_key(text or "") == infer_core.author_key("Aguirre Ann") else "",
            split_known_author_prefix=lambda text: ("Ann Aguirre", "Enklawa") if text == "Aguirre Ann Enklawa" else None,
            split_known_author_suffix=lambda text: None,
        )

        self.assertEqual(parsed.author_hint, "Ann Aguirre")
        self.assertEqual(parsed.title_hint, "Enklawa")
        self.assertEqual(parsed.source, "hybrid:compact-author-title")

    def test_parse_hybrid_local_uses_catalog_suffix_for_compact_name(self) -> None:
        meta = type(
            "Meta",
            (),
            {
                "path": Path("x.epub"),
                "stem": "Enklawa Ann Aguirre",
                "segments": ["Enklawa Ann Aguirre"],
                "core": "Enklawa Ann Aguirre",
                "title": "",
                "creators": [],
                "identifiers": [],
                "subjects": [],
            },
        )()

        parsed = local_parser.parse_hybrid_local(
            meta,
            clean=infer_core.clean,
            clean_author_segment=infer_core.clean,
            looks_like_author_segment=lambda text: bool(text and any(char.isalpha() for char in text)),
            strip_leading_title_index=infer_core.clean,
            parse_volume_parts=infer_core.parse_volume_parts,
            resolve_known_author=lambda text: "Ann Aguirre" if infer_core.author_key(text or "") == infer_core.author_key("Ann Aguirre") else "",
            split_known_author_prefix=lambda text: None,
            split_known_author_suffix=lambda text: ("Ann Aguirre", "Enklawa") if text == "Enklawa Ann Aguirre" else None,
        )

        self.assertEqual(parsed.author_hint, "Ann Aguirre")
        self.assertEqual(parsed.title_hint, "Enklawa")
        self.assertEqual(parsed.source, "hybrid:compact-title-author")

    def test_parse_hybrid_local_uses_catalog_for_delimited_mixed_author_segment(self) -> None:
        meta = type(
            "Meta",
            (),
            {
                "path": Path("x.epub"),
                "stem": "Cel Nagi & Snerg Adam Wisniewski - Oro. Otomi znaczy wyslaniec",
                "segments": ["Cel Nagi & Snerg Adam Wisniewski - Oro. Otomi znaczy wyslaniec"],
                "core": "Cel Nagi & Snerg Adam Wisniewski - Oro. Otomi znaczy wyslaniec",
                "title": "",
                "creators": [],
                "identifiers": [],
                "subjects": [],
            },
        )()

        parsed = local_parser.parse_hybrid_local(
            meta,
            clean=infer_core.clean,
            clean_author_segment=infer_core.clean,
            looks_like_author_segment=lambda text: bool(text and any(char.isalpha() for char in text)),
            strip_leading_title_index=infer_core.clean,
            parse_volume_parts=infer_core.parse_volume_parts,
            resolve_author_segment=lambda text: ["Adam Wisniewski Snerg"] if text == "Cel Nagi & Snerg Adam Wisniewski" else [],
        )

        self.assertEqual(parsed.author_hint, "Adam Wisniewski Snerg")
        self.assertEqual(parsed.title_hint, "Oro. Otomi znaczy wyslaniec")
        self.assertEqual(parsed.source, "hybrid:catalog-delimited-author-title")


if __name__ == "__main__":
    unittest.main()
