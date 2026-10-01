import re
from typing import List, Optional, Dict, Any

BUDGET_TOKENS = 6_000
AVG_CHARS_PER_TOKEN = 4

# Patterns that reliably indicate no architectural content (greetings, simple queries, syntax errors)
TRIVIAL_PATTERNS = re.compile(
    r"^(hi|hello|hey|thanks|thank you|thx|ok|okay|sure|yes|no|got it|sounds good|"
    r"can you (fix|update|change|rename)|please (fix|format|indent)|"
    r"what (is|does|are)|how (do|does|to)|explain|"
    r"syntax error|type error|undefined|import error)",
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
    2. Heuristic filter: drop turns matching TRIVIAL_PATTERNS
    3. Delta trimmer: keep only turns newer than last extraction commit
    4. Token budget cap: truncate oldest turns until estimated tokens <= budget
    """
    if not turns:
        return []

    # 1. Strip system turns
    filtered = [t for t in turns if t.get("role", "").lower() != "system"]

    # 2. Heuristic filter: drop trivially non-architectural turns
    filtered = [
        t for t in filtered
        if not TRIVIAL_PATTERNS.match(t.get("content", "").strip()[:120])
    ]

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
