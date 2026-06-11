"""Gemini LLM-based plant name matcher."""

import json
import time

from google import genai
from google.genai import errors as genai_errors
from google.genai import types
from loguru import logger

from .base import BaseNameMatcher, MatchResult, get_system_prompt


class GeminiNameMatcher(BaseNameMatcher):
    """Plant name matcher using Google Gemini API.

    Attributes:
        model: The Gemini model identifier.

    Example:
        ```python
        matcher = GeminiNameMatcher(api_key="your-key")
        result = matcher.match("Hoover Dam", candidates_str, source_system="EIA")
        ```
    """

    DEFAULT_MODEL = "gemini-2.5-flash"
    MAX_RETRIES = 2  # transient API errors only
    RETRY_BACKOFF_S = 5.0

    def __init__(self, api_key: str, model: str | None = None) -> None:
        if not api_key:
            raise ValueError("API key cannot be empty")

        self.client = genai.Client(api_key=api_key)
        self.model = model or self.DEFAULT_MODEL

    @property
    def name(self) -> str:
        return "gemini"

    def _no_match(self, plant_name: str, reasoning: str) -> MatchResult:
        return MatchResult(
            npp_plant=plant_name,
            match=None,
            source=None,
            confidence=None,
            reasoning=reasoning,
        )

    def _generate(self, user_prompt: str, source_system: str | None) -> str:
        """One API call returning the raw response text. Raises on failure."""
        response = self.client.models.generate_content(
            model=self.model,
            contents=[user_prompt],
            config=types.GenerateContentConfig(
                system_instruction=get_system_prompt(source_system),
                temperature=0.1,
                max_output_tokens=1000,
                # gemini-2.5-flash is a thinking model: without this, thinking
                # tokens can consume the whole 1000-token budget and
                # response.text comes back None — which used to be swallowed
                # and returned as "no match" (the documented "LLM returned
                # 0 of 854 matches" episode).
                thinking_config=types.ThinkingConfig(thinking_budget=0),
            ),
        )
        if response.text is None:
            raise ValueError(
                "response.text is None (output budget exhausted or empty "
                "candidate) — not a 'no match'"
            )
        return response.text.strip()

    def match(
        self, plant_name: str, candidates_str: str, source_system: str | None = None
    ) -> MatchResult:
        user_prompt = f"Plant Name: {plant_name}\n\nCandidates:\n{candidates_str}\n\nJSON response:"

        raw = None
        last_err: Exception | None = None
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                raw = self._generate(user_prompt, source_system)
                break
            except (
                genai_errors.APIError,
                ValueError,
                ConnectionError,
                TimeoutError,
            ) as e:
                last_err = e
                logger.warning(
                    f"Gemini call failed for {plant_name!r} "
                    f"(attempt {attempt + 1}/{self.MAX_RETRIES + 1}): "
                    f"{type(e).__name__}: {e}"
                )
                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_BACKOFF_S * (attempt + 1))

        if raw is None:
            # An API failure is NOT the same as "the LLM said no match" —
            # callers/logs must be able to tell them apart.
            logger.error(
                f"Gemini gave no usable response for {plant_name!r} after "
                f"{self.MAX_RETRIES + 1} attempts: {last_err}"
            )
            return self._no_match(
                plant_name,
                f"API_FAILURE after {self.MAX_RETRIES + 1} attempts: {last_err}",
            )

        # Parse JSON from response
        json_start = raw.find("{")
        json_end = raw.rfind("}") + 1
        if json_start >= 0 and json_end > json_start:
            try:
                parsed = json.loads(raw[json_start:json_end])
            except json.JSONDecodeError as e:
                logger.warning(
                    f"Gemini returned malformed JSON for {plant_name!r}: {e}"
                )
                return self._no_match(plant_name, f"PARSE_FAILURE: {raw[:100]}")
            return MatchResult(
                npp_plant=plant_name,
                match=parsed.get("match"),
                source=parsed.get("source"),
                confidence=parsed.get("confidence"),
                reasoning=parsed.get("reasoning", ""),
            )

        logger.warning(f"Gemini response had no JSON object for {plant_name!r}")
        return self._no_match(plant_name, f"PARSE_FAILURE: {raw[:100]}")
