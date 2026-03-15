import unittest

import infer_core


class InferCoreTests(unittest.TestCase):
    def test_fold_text_preserves_l_stroke_signal(self) -> None:
        self.assertEqual(infer_core.author_key("Łukasz Żmijewski"), "lukaszzmijewski")

    def test_parse_volume_parts_handles_roman(self) -> None:
        self.assertEqual(infer_core.parse_volume_parts("IV"), (4, "00"))

    def test_format_volume_preserves_unknown_placeholder(self) -> None:
        self.assertEqual(infer_core.format_volume(None), "Tom 00.00")
        self.assertEqual(infer_core.format_volume((0, "00")), "Tom 00.00")

    def test_format_title_with_genre_replaces_existing_suffix(self) -> None:
        genre_suffix_re = __import__("re").compile(r"^(.*?)\s*\[([^\[\]]+)\]\s*$")
        self.assertEqual(
            infer_core.format_title_with_genre("Title [fantasy]", "sci-fi", genre_suffix_re=genre_suffix_re),
            "Title [sci-fi]",
        )

    def test_split_title_genre_suffix_preserves_full_label(self) -> None:
        genre_suffix_re = __import__("re").compile(r"^(.*?)\s*\[([^\[\]]+)\]\s*$")
        self.assertEqual(
            infer_core.split_title_genre_suffix(
                "Title [kryminał, sensacja, thriller]",
                genre_suffix_re=genre_suffix_re,
            ),
            ("Title", "kryminał, sensacja, thriller"),
        )


if __name__ == "__main__":
    unittest.main()
