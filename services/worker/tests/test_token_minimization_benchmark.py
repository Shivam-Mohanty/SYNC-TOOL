import pytest
from services.worker.src.minimizer import minimize_transcript, AVG_CHARS_PER_TOKEN

REPRESENTATIVE_TRANSCRIPTS = [
    # 1. Greeting and syntax error debugging + architecture decision
    {
        "name": "Greeting & Syntax Debugging",
        "last_ts": None,
        "turns": [
            {"role": "system", "content": "You are an expert full-stack AI coding assistant with deep knowledge of distributed systems."},
            {"role": "user", "content": "Hello! Good morning."},
            {"role": "assistant", "content": "Good morning! How can I assist you today?"},
            {"role": "user", "content": "syntax error: unexpected token < in JSON at position 0"},
            {"role": "assistant", "content": "This error occurs when you attempt to JSON.parse() an HTML error response instead of valid JSON."},
            {"role": "user", "content": "got it, thanks!"},
            {"role": "assistant", "content": "You're welcome!"},
            {"role": "user", "content": "Decision: We will deploy Redis Streams for transcript queueing with consumer groups and DLQ."}
        ]
    },
    # 2. Formatting, chit-chat and REST to gRPC migration
    {
        "name": "Formatting & Protocol Decision",
        "last_ts": None,
        "turns": [
            {"role": "system", "content": "Always output clean PEP8 compliant Python code and format all code blocks carefully."},
            {"role": "user", "content": "Hi there."},
            {"role": "assistant", "content": "Hello! What can I help with?"},
            {"role": "user", "content": "can you format this indent please?"},
            {"role": "assistant", "content": "Sure! Here is the corrected indentation:\n    def run():\n        pass"},
            {"role": "user", "content": "ok sounds good"},
            {"role": "assistant", "content": "Great!"},
            {"role": "user", "content": "Decision: Component Gateway will communicate with downstream workers via Redis Streams instead of gRPC to minimize infrastructure footprint."}
        ]
    },
    # 3. Delta session with 10 historical turns prior to last commit
    {
        "name": "Delta Session with History",
        "last_ts": 1727800000.0,
        "turns": [
            {"role": "system", "content": "System prompt instructions...", "timestamp": 1727790000.0},
            {"role": "user", "content": "Setup basic repo structure", "timestamp": 1727790100.0},
            {"role": "assistant", "content": "Created directories /src and /tests", "timestamp": 1727790110.0},
            {"role": "user", "content": "Configure Dockerfile", "timestamp": 1727790200.0},
            {"role": "assistant", "content": "Dockerfile configured with Python 3.12-slim", "timestamp": 1727790210.0},
            {"role": "user", "content": "Add linter configuration", "timestamp": 1727790300.0},
            {"role": "assistant", "content": "Added ruff and black configurations", "timestamp": 1727790310.0},
            {"role": "user", "content": "Setup git pre-commit hooks", "timestamp": 1727790400.0},
            {"role": "assistant", "content": "Pre-commit hooks installed", "timestamp": 1727790410.0},
            {"role": "user", "content": "Decision: Add Apache AGE graph database extension to PostgreSQL 16.", "timestamp": 1727800500.0}
        ]
    },
    # 4. Long system instructions + chit chat + database choice
    {
        "name": "Verbose System Instructions & Banter",
        "last_ts": None,
        "turns": [
            {"role": "system", "content": "Strict instructions: " + ("Follow safety standards. " * 30)},
            {"role": "user", "content": "Hey buddy"},
            {"role": "assistant", "content": "Hello! How can I help today?"},
            {"role": "user", "content": "please fix indent in line 22"},
            {"role": "assistant", "content": "Indentation fixed."},
            {"role": "user", "content": "thank you very much!"},
            {"role": "assistant", "content": "Happy to help!"},
            {"role": "user", "content": "Component Apache AGE is selected for per-tenant labeled property graph storage."}
        ]
    },
    # 5. Type error & explanation + KMS encryption constraint
    {
        "name": "Type Error & Security Constraint",
        "last_ts": None,
        "turns": [
            {"role": "system", "content": "Security-first engineering assistant with extensive enterprise crypto expertise."},
            {"role": "user", "content": "Hi! How are you doing today?"},
            {"role": "assistant", "content": "I am doing well, thank you! Ready to assist with your development tasks today."},
            {"role": "user", "content": "type error: Argument of type string is not assignable to parameter of type number."},
            {"role": "assistant", "content": "You need to parse the string using parseInt(value, 10) or Number(value)."},
            {"role": "user", "content": "sure, that worked! Thank you."},
            {"role": "assistant", "content": "Awesome, glad to hear!"},
            {"role": "user", "content": "can you explain how key rotation works?"},
            {"role": "assistant", "content": "Key rotation replaces the old key with a freshly generated key and updates metadata."},
            {"role": "user", "content": "Constraint: All BYOK provider keys must be envelope encrypted with AES-256-GCM and zeroed with wipeBytes."}
        ]
    },
    # 6. Conversational Chit-chat with TTFT latency constraint
    {
        "name": "Conversational Banter & TTFT Constraint",
        "last_ts": None,
        "turns": [
            {"role": "system", "content": "Assistant prompt: Provide concise, high-performance Go and Python architectures."},
            {"role": "user", "content": "Good morning"},
            {"role": "assistant", "content": "Good morning! Ready when you are."},
            {"role": "user", "content": "sounds good, let's proceed."},
            {"role": "assistant", "content": "Understood. Where would you like to begin?"},
            {"role": "user", "content": "how to profile Go latency?"},
            {"role": "assistant", "content": "Use runtime/pprof or net/http/pprof to analyze latency flamegraphs."},
            {"role": "user", "content": "Constraint: API Gateway proxy TTFT latency overhead must remain under 10ms p99 at 50 concurrent SSE streams."}
        ]
    },
    # 7. Apologies & Syntax Correction + DLQ Task
    {
        "name": "Syntax Correction & DLQ Task",
        "last_ts": None,
        "turns": [
            {"role": "system", "content": "System agent initialized with multi-agent orchestration guidelines."},
            {"role": "user", "content": "sorry my mistake, wrong file"},
            {"role": "assistant", "content": "No problem at all! Which file should we inspect?"},
            {"role": "user", "content": "can you update import in main.py"},
            {"role": "assistant", "content": "Updated the import path to services.worker.src.main."},
            {"role": "user", "content": "ok got it"},
            {"role": "assistant", "content": "Great."},
            {"role": "user", "content": "Task: Extraction worker must route unparseable LLM output to Redis DLQ queue:transcripts:dlq."}
        ]
    },
    # 8. Git & Workflow Q&A + Vector Model Decision
    {
        "name": "Git Workflow & Vector Embedding Decision",
        "last_ts": None,
        "turns": [
            {"role": "system", "content": "Senior platform developer guidelines and formatting requirements."},
            {"role": "user", "content": "how to squash git commits?"},
            {"role": "assistant", "content": "Run git rebase -i HEAD~N and mark secondary commits with 'squash' or 's'."},
            {"role": "user", "content": "yes that worked perfectly"},
            {"role": "assistant", "content": "Glad to hear!"},
            {"role": "user", "content": "Decision: Committed to text-embedding-3-small (1536 dims) for entity embeddings with HNSW cosine index."}
        ]
    },
    # 9. Trivial Queries & Variable Renaming + Cache Invalidation Decision
    {
        "name": "Trivial Banter & Cache Invalidation",
        "last_ts": None,
        "turns": [
            {"role": "system", "content": "Expert distributed systems engineer with deep caching knowledge."},
            {"role": "user", "content": "hi there! Can you help clean up this module?"},
            {"role": "assistant", "content": "Hello! Yes, absolutely. Which module would you like to review?"},
            {"role": "user", "content": "can you check line 10 for any syntax or naming issues?"},
            {"role": "assistant", "content": "Line 10 looks fine, but we could rename variable x to branch_id for clarity."},
            {"role": "user", "content": "can you rename variable x to branch_id"},
            {"role": "assistant", "content": "Renamed x to branch_id in all occurrences."},
            {"role": "user", "content": "ok thanks"},
            {"role": "assistant", "content": "You're welcome!"},
            {"role": "user", "content": "Decision: On mutation version bump, worker immediately invalidates branch context in Redis and publishes to Liveblocks."}
        ]
    },
    # 10. Large Transcript with Boilerplate and Budget Capping
    {
        "name": "Over-budget Transcript with Truncation",
        "last_ts": None,
        "turns": [
            {"role": "system", "content": "System role prompt instructions " * 100},
            {"role": "user", "content": "Can you explain the thread lifecycle? " + ("detail " * 200)},
            {"role": "assistant", "content": "Thread lifecycle explanation: " + ("more filler text " * 300)},
            {"role": "user", "content": "Please format the output in markdown. " + ("filler " * 200)},
            {"role": "assistant", "content": "Formatted output: " + ("even more filler " * 300)},
            {"role": "user", "content": "Decision: Envelope encryption rotation must atomically update both encrypted_key and key_nonce in workspace_provider_keys."}
        ]
    }
]

def test_token_minimization_target_60_to_80_percent():
    """
    Phase 5 Benchmark Test:
    Measures pre/post token counts on 10 representative transcripts.
    Verifies that reduction meets or exceeds the target >= 60% reduction.
    """
    results = []
    total_pre_tokens = 0
    total_post_tokens = 0

    print("\n" + "=" * 80)
    print(f"{'TRANSCRIPT NAME':<40} {'PRE TOKENS':<12} {'POST TOKENS':<12} {'REDUCTION %':<12}")
    print("=" * 80)

    for case in REPRESENTATIVE_TRANSCRIPTS:
        name = case["name"]
        turns = case["turns"]
        last_ts = case["last_ts"]

        # Calculate pre-tokens (total characters across all turns divided by 4)
        pre_chars = sum(len(t["content"]) for t in turns)
        pre_tokens = max(1, pre_chars // AVG_CHARS_PER_TOKEN)

        minimized = minimize_transcript(turns, last_ts, budget=6000)

        # Calculate post-tokens
        post_chars = sum(len(t["content"]) for t in minimized)
        post_tokens = post_chars // AVG_CHARS_PER_TOKEN

        reduction_pct = ((pre_tokens - post_tokens) / pre_tokens) * 100.0
        results.append((name, pre_tokens, post_tokens, reduction_pct))

        total_pre_tokens += pre_tokens
        total_post_tokens += post_tokens

        print(f"{name:<40} {pre_tokens:<12} {post_tokens:<12} {reduction_pct:>10.1f}%")

    overall_reduction = ((total_pre_tokens - total_post_tokens) / total_pre_tokens) * 100.0
    print("-" * 80)
    print(f"{'TOTAL / AVERAGE REDUCTION':<40} {total_pre_tokens:<12} {total_post_tokens:<12} {overall_reduction:>10.1f}%")
    print("=" * 80)

    # Verify overall reduction meets or exceeds 60%
    assert overall_reduction >= 60.0, f"Expected >= 60% reduction, achieved {overall_reduction:.1f}%"

    # Verify that each individual transcript achieved significant reduction (>= 60%)
    for name, pre, post, pct in results:
        assert pct >= 60.0, f"Transcript '{name}' only achieved {pct:.1f}% reduction (target >= 60%)"
