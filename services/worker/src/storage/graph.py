import re
import logging
from typing import Optional
from ..models import ContextExtractionResult

logger = logging.getLogger(__name__)

# FIX #1: slug sanitizer — all LLM-produced IDs must pass before entering Cypher
SLUG_RE = re.compile(r'^[a-z0-9][a-z0-9\-_]{0,127}$')

ALLOWED_LABELS = {
    "Component", "Decision", "Constraint", "Task",
    "DEPENDS_ON", "MODIFIES", "CONSTRAINED_BY", "DECIDED_IN", "BLOCKS"
}

def safe_slug(value: str) -> str:
    """Validates that a slug is strictly lowercase alphanumeric with hyphens/underscores."""
    if not isinstance(value, str) or not SLUG_RE.match(value):
        raise ValueError(f"Unsafe node or edge slug rejected: {value!r}")
    return value

def safe_label(value: str) -> str:
    """Validates that an AGE label is in the allowlist."""
    if value not in ALLOWED_LABELS:
        raise ValueError(f"Unknown or unauthorized graph label: {value!r}")
    return value

async def get_last_extraction_ts(workspace_id: str, branch_id: str) -> Optional[float]:
    """
    Retrieves the timestamp of the latest successful context extraction commit on this branch.
    Returns None if no previous extraction exists.
    """
    # In Phase 3, this can query database or default to None
    return None

async def commit_to_graph_and_vector(
    workspace_id: str,
    branch_id: str,
    transcript_id: str,
    extraction: ContextExtractionResult,
    org_id: Optional[str] = None
):
    """
    Persists validated extraction entities to Apache AGE per-tenant graph and pgvector.
    (Full atomic transaction implementation is detailed in Phase 4).
    """
    logger.info(
        "Committing extraction: workspace=%s branch=%s nodes=%d edges=%d",
        workspace_id, branch_id, len(extraction.nodes), len(extraction.edges)
    )
