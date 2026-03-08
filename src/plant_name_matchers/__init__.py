"""Name matchers module for LLM-based plant name matching.

Provides an exchangeable interface for matching plant names across
reference databases (GEM, Crosswalk, GPPD) using different LLM backends.
"""

from .base import BaseNameMatcher, MatchResult
from .gemini import GeminiNameMatcher
from .normalizers import (
    extract_base_name,
    normalize_for_comparison,
    normalize_gppd_name,
    validate_match,
)
from .retriever import CandidateRetriever

__all__ = [
    "BaseNameMatcher",
    "CandidateRetriever",
    "GeminiNameMatcher",
    "MatchResult",
    "extract_base_name",
    "normalize_for_comparison",
    "normalize_gppd_name",
    "validate_match",
]
