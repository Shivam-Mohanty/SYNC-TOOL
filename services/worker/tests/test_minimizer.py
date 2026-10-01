import pytest
from services.worker.src.minimizer import (
    minimize_transcript,
    estimate_tokens,
    format_turns_for_llm,
    BUDGET_TOKENS,
)

def test_system_strip():
    turns = [
        {"role": "system", "content": "You are a coding assistant."},
        {"role": "user", "content": "Design the authentication architecture using OAuth2 and JWT."},
        {"role": "assistant", "content": "We will use Auth0 with RS256 signed JWT tokens."}
    ]
    minimized = minimize_transcript(turns)
    assert len(minimized) == 2
    assert all(t["role"] != "system" for t in minimized)

def test_heuristic_filter():
    turns = [
        {"role": "user", "content": "Hello there"},
        {"role": "assistant", "content": "Hi! How can I help you today?"},
        {"role": "user", "content": "Thanks! Sounds good"},
        {"role": "user", "content": "Can you fix the typo on line 42?"},
        {"role": "user", "content": "We need to introduce Apache AGE for graph database storage alongside PostgreSQL."},
        {"role": "assistant", "content": "Apache AGE allows executing Cypher queries directly inside Postgres transactions."}
    ]
    minimized = minimize_transcript(turns)
    # Trivial turns (Hello, Thanks, Can you fix) should be dropped
    assert len(minimized) == 2
    assert "Apache AGE" in minimized[0]["content"]
    assert "Cypher" in minimized[1]["content"]

def test_delta_trim():
    turns = [
        {"role": "user", "content": "Old architectural choice for Redis", "timestamp": 1000},
        {"role": "assistant", "content": "Agreed on Redis 7", "timestamp": 1050},
        {"role": "user", "content": "New architectural decision to adopt HashiCorp Vault", "timestamp": 2000},
        {"role": "assistant", "content": "We will use Vault Transit KEK for envelope encryption", "timestamp": 2050},
    ]
    # Commit happened at timestamp 1500
    minimized = minimize_transcript(turns, last_extraction_ts=1500)
    assert len(minimized) == 2
    assert "Vault" in minimized[0]["content"]

def test_token_budget_cap():
    # Construct a transcript that exceeds budget
    long_content = "Architecture decision " * 500  # ~11,000 chars -> ~2,750 tokens
    turns = [
        {"role": "user", "content": f"Turn 1: {long_content}"},
        {"role": "assistant", "content": f"Turn 2: {long_content}"},
        {"role": "user", "content": f"Turn 3: {long_content}"},
        {"role": "assistant", "content": f"Turn 4: {long_content}"},
    ]
    # Set a tight budget of 3000 tokens
    budget = 3000
    minimized = minimize_transcript(turns, budget=budget)
    total_tokens = estimate_tokens(minimized)
    assert total_tokens <= budget
    # Oldest turns were dropped to satisfy budget
    assert "Turn 4:" in minimized[-1]["content"]

def test_format_turns_for_llm():
    turns = [
        {"role": "user", "content": "Let's use Kafka."},
        {"role": "assistant", "content": "Kafka provides partitioned topics."}
    ]
    formatted = format_turns_for_llm(turns)
    assert "USER: Let's use Kafka." in formatted
    assert "ASSISTANT: Kafka provides partitioned topics." in formatted
