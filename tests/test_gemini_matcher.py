"""Tests for the Gemini matcher's failure-mode separation.

The historical bug: every failure (None response.text from an exhausted
thinking budget, rate limits, network errors, malformed JSON) was swallowed
by one blanket except and returned as a no-match — making an API outage
indistinguishable from "the LLM said no match" (the documented
"0 of 854 matches" episode).
"""

import json
from types import SimpleNamespace

import pytest

from src.plant_name_matchers.gemini import GeminiNameMatcher


@pytest.fixture
def matcher(monkeypatch):
    m = GeminiNameMatcher(api_key="test-key")
    monkeypatch.setattr(GeminiNameMatcher, "RETRY_BACKOFF_S", 0.0)
    return m


def _stub_client(responses):
    """A stub client whose generate_content pops scripted outcomes.

    Each item is either an Exception (raised) or a string (returned as
    response.text — may be None).
    """
    calls = {"n": 0}

    def generate_content(model, contents, config):
        outcome = responses[min(calls["n"], len(responses) - 1)]
        calls["n"] += 1
        if isinstance(outcome, Exception):
            raise outcome
        return SimpleNamespace(text=outcome)

    return SimpleNamespace(
        models=SimpleNamespace(generate_content=generate_content)
    ), calls


def test_valid_json_match(matcher):
    payload = json.dumps(
        {
            "match": "GEM: Korba power station",
            "source": "GEM",
            "confidence": "high",
            "reasoning": "same location",
        }
    )
    matcher.client, calls = _stub_client([payload])
    r = matcher.match("KORBA STPS", "GEM: Korba power station (score: 95)")
    assert r.match == "GEM: Korba power station"
    assert r.confidence == "high"
    assert calls["n"] == 1


def test_none_response_text_is_api_failure_not_no_match(matcher):
    """The thinking-budget failure: response.text None must be retried and,
    if persistent, reported as API_FAILURE — never as a plain no-match."""
    matcher.client, calls = _stub_client([None, None, None])
    r = matcher.match("KORBA STPS", "candidates")
    assert r.match is None
    assert r.reasoning.startswith("API_FAILURE")
    assert calls["n"] == matcher.MAX_RETRIES + 1, "must retry before giving up"


def test_transient_error_recovers_on_retry(matcher):
    payload = json.dumps(
        {
            "match": None,
            "source": None,
            "confidence": None,
            "reasoning": "no plausible candidate",
        }
    )
    matcher.client, calls = _stub_client([ConnectionError("blip"), payload])
    r = matcher.match("KORBA STPS", "candidates")
    assert r.reasoning == "no plausible candidate", "second attempt must be used"
    assert calls["n"] == 2


def test_genuine_llm_no_match_is_not_a_failure(matcher):
    payload = json.dumps(
        {
            "match": None,
            "source": None,
            "confidence": None,
            "reasoning": "none of the candidates fit",
        }
    )
    matcher.client, _ = _stub_client([payload])
    r = matcher.match("OBSCURE PLANT", "candidates")
    assert r.match is None
    assert "FAILURE" not in r.reasoning, "a real no-match must not look like an error"


def test_malformed_json_is_parse_failure(matcher):
    matcher.client, _ = _stub_client(['{"match": "GEM: Foo", broken'])
    r = matcher.match("FOO TPS", "candidates")
    assert r.match is None
    assert r.reasoning.startswith("PARSE_FAILURE")


def test_no_json_at_all_is_parse_failure(matcher):
    matcher.client, _ = _stub_client(["I could not find a match, sorry!"])
    r = matcher.match("FOO TPS", "candidates")
    assert r.match is None
    assert r.reasoning.startswith("PARSE_FAILURE")


def test_client_error_fails_fast_without_retry(matcher):
    """4xx is deterministic — must not burn 15s of backoff re-asking."""
    from google.genai import errors as genai_errors

    err = genai_errors.ClientError(400, {"error": {"message": "bad request"}})
    matcher.client, calls = _stub_client([err, err, err])
    r = matcher.match("KORBA STPS", "candidates")
    assert r.match is None
    assert r.reasoning.startswith("API_FAILURE")
    assert calls["n"] == 1, "client errors must not be retried"
