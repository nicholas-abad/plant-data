# plant_name_matchers/

Plant name normalization, candidate retrieval, and LLM-based matching for resolving plant names to reference database entries.

## Modules

| File | Purpose |
|------|---------|
| `normalizers.py` | Shared normalization functions: `normalize_for_comparison()`, `normalize_gppd_name()`, `extract_base_name()`, `validate_match()`. Strips power-plant suffixes (TPS, STPS, CCPP, etc.), removes stop words, and validates fuzzy matches. |
| `retriever.py` | `CandidateRetriever` -- pulls top-N fuzzy candidates from multiple named reference lists (GEM, Crosswalk, GPPD) and formats them for the LLM prompt. |
| `base.py` | `BaseNameMatcher` ABC and `MatchResult` dataclass. Defines the interface and system prompt for LLM-based matching. |
| `gemini.py` | `GeminiNameMatcher` -- concrete implementation using Google Gemini API (default model: `gemini-2.5-flash`). |

## Usage

```python
from plant_name_matchers import (
    # Normalization
    normalize_for_comparison,
    normalize_gppd_name,
    extract_base_name,
    validate_match,
    # LLM matching
    GeminiNameMatcher,
    CandidateRetriever,
)

# Normalize a plant name
normalize_for_comparison("VINDHYACHAL STPS")  # -> "VINDHYACHAL"

# Retrieve candidates for LLM
retriever = CandidateRetriever({"GEM": gem_names, "GPPD": gppd_names})
candidates_str = retriever.get_candidates("Dr. N.TATA RAO TPS", limit=15)

# LLM matching
matcher = GeminiNameMatcher(api_key="...")
result = matcher.match("Dr. N.TATA RAO TPS", candidates_str)
print(result.match, result.confidence)  # "GEM: Dr Narla Tata Rao TPS", "high"
```

## Adding a New LLM Backend

Subclass `BaseNameMatcher` and implement the `name` property and `match()` method. The system prompt and `MatchResult` format are defined in `base.py`.
