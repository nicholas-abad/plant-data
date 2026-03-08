"""Base interface for LLM-based plant name matchers."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class MatchResult:
    """Result of an LLM name matching attempt.

    Attributes:
        npp_plant: The original plant name from NPP.
        match: The matched candidate string (with source prefix), or None.
        source: Which reference DB matched (GEM, Crosswalk, GPPD), or None.
        confidence: high, medium, low, or None if no match.
        reasoning: Brief explanation from the LLM.
    """

    npp_plant: str
    match: Optional[str]
    source: Optional[str]
    confidence: Optional[str]
    reasoning: str


class BaseNameMatcher(ABC):
    """Abstract base class for LLM-based plant name matchers.

    Implementations take an unmatched plant name and a list of candidate
    matches from reference databases, then use an LLM to determine if
    any candidate refers to the same physical plant.

    Example:
        ```python
        matcher = GeminiNameMatcher(api_key="your-key")
        result = matcher.match("Dr. N.TATA RAO TPS", candidates_str)
        print(result.match, result.confidence)
        ```
    """

    SYSTEM_PROMPT = """You are an expert on Indian power plants. Given an NPP (National Power Portal) plant name and a list of candidate matches from reference databases, determine if any candidate is the same physical power plant.

Indian power plant naming patterns to consider:
- TPS = Thermal Power Station, STPS = Super TPS, CCPP = Combined Cycle Power Plant
- Names may use abbreviations: "Dr. N.TATA RAO" = "Dr Narla Tata Rao"
- Corporate prefixes/suffixes may differ: "NSPCL" prefix, "Ltd", "Pvt"
- Hindi/English transliteration variants: "Vindhyachal" vs "Vindhyanchal"
- Location-based names may have state qualifiers added/removed

If a candidate matches the same physical plant, respond with this JSON (no other text):
{"match": "<exact candidate text including source prefix>", "source": "GEM|Crosswalk|GPPD", "confidence": "high|medium|low", "reasoning": "<brief explanation>"}

If NO candidate is the same plant, respond with:
{"match": null, "source": null, "confidence": null, "reasoning": "<brief explanation>"}"""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name/identifier of this matcher implementation."""
        ...

    @abstractmethod
    def match(self, plant_name: str, candidates_str: str) -> MatchResult:
        """Match a plant name against candidates using an LLM.

        Args:
            plant_name: The NPP plant name to match.
            candidates_str: Formatted string of candidates with scores.

        Returns:
            MatchResult with the best match or a rejection.
        """
        ...
