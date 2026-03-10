"""Candidate retrieval for LLM-based plant name matching.

Pulls top-N fuzzy candidates from multiple named reference lists,
formatted as a string ready for the LLM prompt.
"""

from rapidfuzz import fuzz, process

from .normalizers import normalize_for_comparison, normalize_gppd_name


class CandidateRetriever:
    """Retrieve fuzzy-match candidates from multiple named reference sources.

    Args:
        sources: Mapping of source name to list of reference plant names.
            Example: {"GEM": [...], "Crosswalk": [...], "GPPD": [...]}
        normalize_fn: Optional per-source normalization overrides.
            Keys are source names, values are (query_norm_fn, ref_norm_fn) tuples.
    """

    def __init__(self, sources: dict[str, list[str]]) -> None:
        self._sources: dict[str, list[str]] = {}
        self._normalized: dict[str, dict[str, str]] = {}  # norm -> original

        for name, names_list in sources.items():
            if name == "GPPD":
                norm_map = {normalize_gppd_name(n): n for n in names_list}
            else:
                norm_map = {normalize_for_comparison(n): n for n in names_list}
            self._sources[name] = list(norm_map.keys())
            self._normalized[name] = norm_map

    def get_candidates(self, plant_name: str, limit: int = 15) -> str:
        """Get top-N fuzzy candidates from each source, formatted for LLM prompt.

        Args:
            plant_name: Raw plant name to match.
            limit: Max candidates per source.

        Returns:
            Formatted string with scored candidates from all sources.
        """
        candidates: dict[str, float] = {}

        norm_default = normalize_for_comparison(plant_name)
        norm_gppd = normalize_gppd_name(plant_name)

        for source_name, norm_list in self._sources.items():
            query = norm_gppd if source_name == "GPPD" else norm_default
            hits = process.extract(query, norm_list, scorer=fuzz.token_sort_ratio, limit=limit)
            norm_map = self._normalized[source_name]
            for match_str, score, _ in hits:
                orig = norm_map[match_str]
                candidates[f"{source_name}: {orig}"] = score

        sorted_cands = sorted(candidates.items(), key=lambda x: -x[1])
        return "\n".join(f"  {name} (score: {score:.0f})" for name, score in sorted_cands)

    def get_all_candidates(self) -> str:
        """Return ALL candidates from all sources, formatted for LLM prompt.

        Used for cross-language matching (e.g., Japanese kanji vs English)
        where fuzzy retrieval is ineffective.
        """
        lines = []
        for source_name, norm_map in self._normalized.items():
            for orig in norm_map.values():
                lines.append(f"  {source_name}: {orig}")
        return "\n".join(lines)
