"""Plant data repository - centralized plant coordinate matching."""

from .gcpt_loader import GCPTLoader
from .utils import load_crosswalk, save_crosswalk, get_data_dir, get_crosswalk_dir
from .plant_name_matchers import (
    BaseNameMatcher,
    CandidateRetriever,
    GeminiNameMatcher,
    MatchResult,
    extract_base_name,
    normalize_for_comparison,
    normalize_gppd_name,
    validate_match,
)

__all__ = [
    "GCPTLoader",
    "CandidateRetriever",
    "load_crosswalk",
    "save_crosswalk",
    "get_data_dir",
    "get_crosswalk_dir",
    "BaseNameMatcher",
    "GeminiNameMatcher",
    "MatchResult",
    "extract_base_name",
    "normalize_for_comparison",
    "normalize_gppd_name",
    "validate_match",
]
__version__ = "0.2.0"
