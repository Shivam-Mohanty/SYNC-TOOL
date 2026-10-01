import re
from typing import List, Optional, Dict, Any

BUDGET_TOKENS = 6_000
AVG_CHARS_PER_TOKEN = 4

# Patterns that reliably indicate non-architectural content (greetings, simple queries, syntax errors, filler)
TRIVIAL_PATTERNS = re.compile(
    r"^(hi|hello|hey|good (morning|afternoon|evening)|howdy|greetings|"
    r"thanks|thank you|thx|many thanks|ok|okay|sure|yes|yeah|yep|no|nope|got it|sounds good|understood|"
    r"you('re| are) welcome|happy to help|glad to hear|no problem|anytime|great|done|here (is|are)|sure thing|"
    r"sorry|my mistake|apologies|never mind|nevermind|"
    r"can you (fix|update|change|rename|explain)|please (fix|format|indent|update|explain)|"
    r"what (is|does|are|do)|how (do|does|to|can)|explain|"
    r"syntax error|type error|undefined|import error|reference error|"
    r"this error occurs|you need to|run git|here is|here are|awesome|glad)",
    re.IGNORECASE
)

# Architectural keywords: if present, the turn is considered meaningful architectural context
ARCHITECTURAL_MARKERS = re.compile(
    r"\b(component|decision|constraint|task|architecture|database|storage|"
    r"postgresql|postgres|apache age|\bage graph\b|cypher|redis stream|redis streams|kafka|vault transit|"
    r"hashicorp vault|cloud kms|\bkms\b|\bdek\b|\bkek\b|envelope encryption|aes-256-gcm|wipebytes|"
    r"graphql|grpc|pgvector|text-embedding-3-small|embedding vector|hnsw|"
    r"context_commits|mutation_version|liveblocks|p99 ttft|sse stream|streamgate|"
    r"api gateway proxy)\b",
    re.IGNORECASE
)

def minimize_transcript(
    turns: List[Dict[str, Any]],
    last_extraction_ts: Optional[float] = None,
    budget: int = BUDGET_TOKENS,
) -> List[Dict[str, Any]]:
    """
    Minimizes the transcript prior to sending it to the extraction LLM:
    1. System-strip: drop all system-role turns
    2. Heuristic filter: drop turns matching TRIVIAL_PATTERNS and conversational Q&A without architecture markers
    3. Delta trimmer: keep only turns newer than last extraction commit
    4. Token budget cap: truncate oldest turns until estimated tokens <= budget
    """
    if not turns:
        return []

    # 1. Strip system turns
    stripped = [t for t in turns if t.get("role", "").lower() != "system"]

    # 2. Heuristic filter: identify trivial queries and conversational turns
    filtered = []
    in_trivial_block = False

    for t in stripped:
        content = t.get("content", "").strip()
        role = t.get("role", "user").lower()
        is_trivial = bool(TRIVIAL_PATTERNS.match(content[:120]))
        has_arch = bool(ARCHITECTURAL_MARKERS.search(content))

        if has_arch:
            in_trivial_block = False
            filtered.append(t)
            continue

        if is_trivial:
            in_trivial_block = True
            continue

        if in_trivial_block:
            # Continue dropping assistant/user replies within a trivial Q&A thread until architecture is discussed
            continue

        filtered.append(t)

    # 3. Delta trim: only turns after last extraction commit on this branch
    if last_extraction_ts is not None:
        filtered = [
            t for t in filtered
            if t.get("timestamp", float("inf")) > last_extraction_ts
        ]

    # 4. Token budget: truncate from oldest turns until estimated tokens <= budget
    while filtered and estimate_tokens(filtered) > budget:
        filtered.pop(0)

    return filtered

def estimate_tokens(turns: List[Dict[str, Any]]) -> int:
    """Estimates tokens using the standard 4-characters-per-token heuristic."""
    total_chars = sum(len(t.get("content", "")) for t in turns)
    return total_chars // AVG_CHARS_PER_TOKEN

def format_turns_for_llm(turns: List[Dict[str, Any]]) -> str:
    """Formats minimized turns into standard dialogue format for LLM prompt."""
    return "\n\n".join(
        f"{t.get('role', 'user').upper()}: {t.get('content', '').strip()}"
        for t in turns
    )
