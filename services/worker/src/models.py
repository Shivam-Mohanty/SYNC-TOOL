from enum import Enum
from typing import List
from pydantic import BaseModel, Field, field_validator
import re

SLUG_RE = re.compile(r'^[a-z0-9][a-z0-9\-_]{0,127}$')

class NodeType(str, Enum):
    COMPONENT  = "Component"
    DECISION   = "Decision"
    CONSTRAINT = "Constraint"
    TASK       = "Task"

class EdgeType(str, Enum):
    DEPENDS_ON      = "DEPENDS_ON"
    MODIFIES        = "MODIFIES"
    CONSTRAINED_BY  = "CONSTRAINED_BY"
    DECIDED_IN      = "DECIDED_IN"
    BLOCKS          = "BLOCKS"

class ExtractedNode(BaseModel):
    id: str = Field(description="Slug: lowercase alphanumeric + hyphens, max 128 chars")
    name: str = Field(description="Concise human-readable name of the entity")
    node_type: NodeType
    summary: str = Field(description="Max 2 sentences summarizing the entity role or decision")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score between 0.0 and 1.0")

    @field_validator("id")
    @classmethod
    def validate_id_slug(cls, v: str) -> str:
        v = v.strip().lower()
        if not SLUG_RE.match(v):
            # Attempt to normalize by replacing non-alphanumerics
            normalized = re.sub(r'[^a-z0-9\-_]', '-', v).strip('-')
            if not SLUG_RE.match(normalized):
                raise ValueError(f"Node id must be valid slug matching ^[a-z0-9][a-z0-9-_]{{0,127}}$, got {v!r}")
            return normalized
        return v

class ExtractedEdge(BaseModel):
    source_id: str = Field(description="Slug of source node")
    target_id: str = Field(description="Slug of target node")
    edge_type: EdgeType

    @field_validator("source_id", "target_id")
    @classmethod
    def validate_edge_slugs(cls, v: str) -> str:
        v = v.strip().lower()
        if not SLUG_RE.match(v):
            normalized = re.sub(r'[^a-z0-9\-_]', '-', v).strip('-')
            if not SLUG_RE.match(normalized):
                raise ValueError(f"Edge endpoint must be valid slug, got {v!r}")
            return normalized
        return v

class ContextExtractionResult(BaseModel):
    has_meaningful_context: bool = Field(
        description="True if the transcript contained meaningful architecture, components, decisions, or constraints"
    )
    nodes: List[ExtractedNode] = Field(default_factory=list)
    edges: List[ExtractedEdge] = Field(default_factory=list)
