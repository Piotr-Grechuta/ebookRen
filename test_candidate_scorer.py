import unittest
from pathlib import Path

import candidate_scorer
import infer_core
from models_core import Candidate, EpubMetadata, RankedOnlineMatch


class CandidateScorerTests(unittest.TestCase):
    def test_choose_best_local_title_candidate_rejects_author_like_title(self) -> None:
        meta = EpubMetadata(
            path=Path("x.epub"),
            stem="Burzowe Kocie - Maja Lidia Kossakowska",
            segments=["Burzowe Kocie - Maja Lidia Kossakowska"],
            core="Burzowe Kocie - Maja Lidia Kossakowska",
            title="Burzowe Kocie",
            creators=[],
        )
        candidates = [
            Candidate(95, "Standalone", None, "Maja Lidia Kossakowska", "core:title-author"),
            Candidate(90, "Standalone", None, "Burzowe Kocie", "hybrid:delimited-index-title-author"),
        ]

        best = candidate_scorer.choose_best_local_title_candidate(
            meta,
            candidates,
            "Standalone",
            clean=infer_core.clean,
            normalize_match_text=infer_core.normalize_match_text,
            similarity_score=infer_core.similarity_score,
            strip_leading_title_index=infer_core.clean,
            looks_like_author_segment=lambda text: bool(text and len(text.split()) >= 3),
        )

        self.assertIsNotNone(best)
        assert best is not None
        self.assertEqual(best.title_override, "Burzowe Kocie")

    def test_ranked_online_match_score_prefers_lubimyczytac_for_polish_metadata(self) -> None:
        meta = EpubMetadata(
            path=Path("x.epub"),
            stem="Tom 1 Czerwona Krolowa",
            segments=["Tom 1 Czerwona Krolowa"],
            core="Tom 1 Czerwona Krolowa",
            title="Tom 1 Czerwona Krolowa",
            creators=["Victoria Aveyard"],
        )
        google = RankedOnlineMatch(["google-books"], ["google-books"], "Czerwona Krolowa Deluxe", ["Victoria Aveyard"], [], 300, "title-author-exact")
        lubimy = RankedOnlineMatch(["lubimyczytac"], ["lubimyczytac"], "Czerwona Krolowa", ["Victoria Aveyard"], [], 280, "title-author-exact")

        google_score = candidate_scorer.ranked_online_match_score(
            meta,
            google,
            clean=infer_core.clean,
            normalize_match_text=infer_core.normalize_match_text,
            fold_text=infer_core.fold_text,
        )
        lubimy_score = candidate_scorer.ranked_online_match_score(
            meta,
            lubimy,
            clean=infer_core.clean,
            normalize_match_text=infer_core.normalize_match_text,
            fold_text=infer_core.fold_text,
        )

        self.assertGreater(lubimy_score, google_score)


if __name__ == "__main__":
    unittest.main()
