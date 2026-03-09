"""Base interface for LLM-based plant name matchers."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class MatchResult:
    """Result of an LLM name matching attempt.

    Attributes:
        plant_name: The original plant name being matched.
        match: The matched candidate string (with source prefix), or None.
        source: Which reference DB matched (GEM, Crosswalk, GPPD), or None.
        confidence: high, medium, low, or None if no match.
        reasoning: Brief explanation from the LLM.
    """

    npp_plant: str  # kept for backward compat, but represents any source
    match: Optional[str]
    source: Optional[str]
    confidence: Optional[str]
    reasoning: str


# Per-source context for the LLM prompt
_SOURCE_CONTEXT = {
    "NPP": {
        "expertise": "Indian power plants",
        "label": "NPP (National Power Portal, India)",
        "patterns": (
            "Indian power plant naming patterns to consider:\n"
            "- TPS = Thermal Power Station, STPS = Super TPS, CCPP = Combined Cycle Power Plant\n"
            "- Names may use abbreviations: \"Dr. N.TATA RAO\" = \"Dr Narla Tata Rao\"\n"
            "- Corporate prefixes/suffixes may differ: \"NSPCL\" prefix, \"Ltd\", \"Pvt\"\n"
            "- Hindi/English transliteration variants: \"Vindhyachal\" vs \"Vindhyanchal\"\n"
            "- Location-based names may have state qualifiers added/removed"
        ),
    },
    "EIA": {
        "expertise": "US power plants",
        "label": "EIA (Energy Information Administration, USA)",
        "patterns": (
            "US power plant naming patterns to consider:\n"
            "- Names often include company/owner names: \"Duke Energy\", \"NextEra\"\n"
            "- Facility types: \"Solar Farm\", \"Wind Farm\", \"Generating Station\", \"Power Plant\"\n"
            "- Location names may differ slightly: city, county, or state qualifiers\n"
            "- Corporate mergers/acquisitions may change owner prefixes\n"
            "- Abbreviations: \"CC\" = Combined Cycle, \"CT\" = Combustion Turbine, \"GT\" = Gas Turbine"
        ),
    },
    "ENTSOE": {
        "expertise": "European power plants",
        "label": "ENTSOE (European Network of TSOs for Electricity)",
        "patterns": (
            "European power plant naming patterns to consider:\n"
            "- Names may be in local languages: German, French, Spanish, etc.\n"
            "- Facility types may differ: \"Kraftwerk\" (German), \"Centrale\" (French/Italian)\n"
            "- Owner/operator prefixes may differ between databases\n"
            "- Location names may use local vs English spellings\n"
            "- Unit numbers or block identifiers may be appended differently"
        ),
    },
    "ONS": {
        "expertise": "Brazilian power plants",
        "label": "ONS (Operador Nacional do Sistema Elétrico, Brazil)",
        "patterns": (
            "Brazilian power plant naming patterns to consider:\n"
            "- Names are typically in Portuguese\n"
            "- \"UHE\" = Usina Hidrelétrica (Hydroelectric Plant), \"UTE\" = Usina Termelétrica (Thermal Plant)\n"
            "- \"PCH\" = Pequena Central Hidrelétrica (Small Hydro)\n"
            "- River/location names may have accent differences: \"Itaipú\" vs \"Itaipu\"\n"
            "- Corporate prefixes may differ: \"CESP\", \"Eletrobras\", \"CPFL\""
        ),
    },
    "OE": {
        "expertise": "Australian power plants",
        "label": "OpenElectricity (Australia)",
        "patterns": (
            "Australian power plant naming patterns to consider:\n"
            "- Names often include location + type: \"Loy Yang Power Station\"\n"
            "- Owner names may differ: \"AGL\", \"Origin Energy\", \"EnergyAustralia\"\n"
            "- Wind/solar farms may have geographic qualifiers\n"
            "- State abbreviations: NSW, VIC, QLD, SA, WA, TAS"
        ),
    },
    "OCCTO": {
        "expertise": "Japanese power plants",
        "label": "OCCTO (Organization for Cross-regional Coordination of Transmission Operators, Japan)",
        "patterns": (
            "Japanese power plant naming patterns to consider:\n"
            "- Names are in Japanese (kanji): e.g., \"苫東厚真\" (Tomato-Atsuma), \"碧南\" (Hekinan)\n"
            "- Reference databases (GEM/GPPD) use romanized English names\n"
            "- Match kanji plant names to their romanized equivalents\n"
            "- Common suffixes in Japanese: \"火力発電所\" (thermal power plant), \"発電所\" (power plant)\n"
            "- Area/location names may appear as prefixes: \"磯子\" (Isogo), \"橘湾\" (Tachibana Bay)\n"
            "- Company names may be omitted or abbreviated: \"JERA\", \"電源開発\" (J-POWER)\n"
            "- Some plants use location + fuel: \"敦賀火力\" = Tsuruga Thermal"
        ),
    },
}

_DEFAULT_CONTEXT = {
    "expertise": "global power plants",
    "label": "generation database",
    "patterns": (
        "General power plant naming patterns to consider:\n"
        "- Names may include owner, location, and/or facility type\n"
        "- Abbreviations and suffixes may vary between databases\n"
        "- Corporate prefixes/suffixes may differ"
    ),
}


def get_system_prompt(source_system: str | None = None) -> str:
    """Build a source-aware system prompt for the LLM matcher."""
    ctx = _SOURCE_CONTEXT.get(source_system, _DEFAULT_CONTEXT) if source_system else _DEFAULT_CONTEXT

    return f"""You are an expert on {ctx["expertise"]}. Given a plant name from the {ctx["label"]} and a list of candidate matches from reference databases, determine if any candidate is the same physical power plant.

{ctx["patterns"]}

If a candidate matches the same physical plant, respond with this JSON (no other text):
{{"match": "<exact candidate text including source prefix>", "source": "GEM|Crosswalk|GPPD", "confidence": "high|medium|low", "reasoning": "<brief explanation>"}}

If NO candidate is the same plant, respond with:
{{"match": null, "source": null, "confidence": null, "reasoning": "<brief explanation>"}}"""


class BaseNameMatcher(ABC):
    """Abstract base class for LLM-based plant name matchers.

    Implementations take an unmatched plant name and a list of candidate
    matches from reference databases, then use an LLM to determine if
    any candidate refers to the same physical plant.

    Example:
        ```python
        matcher = GeminiNameMatcher(api_key="your-key")
        result = matcher.match("Hoover Dam", candidates_str, source_system="EIA")
        print(result.match, result.confidence)
        ```
    """

    # Keep as fallback; prefer get_system_prompt(source_system) instead
    SYSTEM_PROMPT = get_system_prompt(None)

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name/identifier of this matcher implementation."""
        ...

    @abstractmethod
    def match(self, plant_name: str, candidates_str: str,
              source_system: str | None = None) -> MatchResult:
        """Match a plant name against candidates using an LLM.

        Args:
            plant_name: The plant name to match.
            candidates_str: Formatted string of candidates with scores.
            source_system: Source system (NPP, EIA, ENTSOE, ONS, OE) for
                context-aware prompting.

        Returns:
            MatchResult with the best match or a rejection.
        """
        ...
