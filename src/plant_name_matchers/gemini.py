"""Gemini LLM-based plant name matcher."""

import json

from google import genai
from google.genai import types

from .base import BaseNameMatcher, MatchResult


class GeminiNameMatcher(BaseNameMatcher):
    """Plant name matcher using Google Gemini API.

    Attributes:
        model: The Gemini model identifier.

    Example:
        ```python
        matcher = GeminiNameMatcher(api_key="your-key")
        result = matcher.match("Dr. N.TATA RAO TPS", candidates_str)
        ```
    """

    DEFAULT_MODEL = "gemini-2.5-flash"

    def __init__(self, api_key: str, model: str | None = None) -> None:
        if not api_key:
            raise ValueError("API key cannot be empty")

        self.client = genai.Client(api_key=api_key)
        self.model = model or self.DEFAULT_MODEL

    @property
    def name(self) -> str:
        return "gemini"

    def match(self, plant_name: str, candidates_str: str) -> MatchResult:
        user_prompt = f"NPP Plant Name: {plant_name}\n\nCandidates:\n{candidates_str}\n\nJSON response:"

        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=[user_prompt],
                config=types.GenerateContentConfig(
                    system_instruction=self.SYSTEM_PROMPT,
                    temperature=0.1,
                    max_output_tokens=200,
                ),
            )

            raw = response.text.strip()

            # Parse JSON from response
            json_start = raw.find("{")
            json_end = raw.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                parsed = json.loads(raw[json_start:json_end])
                return MatchResult(
                    npp_plant=plant_name,
                    match=parsed.get("match"),
                    source=parsed.get("source"),
                    confidence=parsed.get("confidence"),
                    reasoning=parsed.get("reasoning", ""),
                )

        except Exception as e:
            return MatchResult(
                npp_plant=plant_name,
                match=None,
                source=None,
                confidence=None,
                reasoning=f"API error: {e}",
            )

        return MatchResult(
            npp_plant=plant_name,
            match=None,
            source=None,
            confidence=None,
            reasoning=f"Failed to parse: {raw[:100]}",
        )
