import re
import logging
from typing import Optional, List, Dict, Any
import psycopg
from psycopg.rows import dict_row
import redis.asyncio as aioredis

from ..config import config
from ..models import ContextExtractionResult, NodeType, EdgeType
from .vector import generate_embedding
from .cache import invalidate_branch_cache, broadcast_mutation

logger = logging.getLogger(__name__)

# FIX #1: slug sanitizer — all LLM-produced IDs must pass before entering Cypher
SLUG_RE = re.compile(r'^[a-z0-9][a-z0-9\-_]{0,127}$')

# Name regex: allow human-readable alphanumeric strings, spaces, and safe punctuation.
# Explicitly disallow single quotes ('), double quotes ("), dollar signs ($), semicolons (;),
# backslashes (\), control characters, and SQL/Cypher comment delimiters.
NAME_RE = re.compile(r'^[a-zA-Z0-9\s\-_.,:()/@#+]{1,255}$')

# Org/Tenant identifier regex for per-tenant graph name isolation (FIX #6)
ORG_ID_RE = re.compile(r'^[a-zA-Z0-9\-_]{1,64}$')

ALLOWED_LABELS = {
    "Component", "Decision", "Constraint", "Task",
    "DEPENDS_ON", "MODIFIES", "CONSTRAINED_BY", "DECIDED_IN", "BLOCKS"
}

def safe_slug(value: str) -> str:
    """
    Validates that a slug is strictly lowercase alphanumeric with hyphens/underscores.
    Rejects any string containing SQL/Cypher metacharacters, spaces, quotes, uppercase, or invalid lengths.
    """
    if not isinstance(value, str) or not SLUG_RE.match(value):
        raise ValueError(f"Unsafe node or edge slug rejected: {value!r}")
    return value

def safe_label(value: str) -> str:
    """Validates that an AGE label is in the strict allowlist."""
    if not isinstance(value, str) or value not in ALLOWED_LABELS:
        raise ValueError(f"Unknown or unauthorized graph label: {value!r}")
    return value

def safe_name(value: str) -> str:
    """
    Validates that an entity name is a safe human-readable string without SQL or Cypher metacharacters.
    Rejects strings containing single quotes, semicolons, dollar signs, backslashes, comment markers, etc.
    """
    if not isinstance(value, str):
        raise ValueError(f"Unsafe node name rejected (must be str): {value!r}")
    stripped = value.strip()
    if not stripped or len(stripped) > 255:
        raise ValueError(f"Unsafe node name rejected (length {len(stripped)} outside 1-255): {value!r}")
    if "--" in value or "/*" in value or "*/" in value:
        raise ValueError(f"Unsafe node name rejected (contains comment metacharacters): {value!r}")
    if not NAME_RE.match(stripped):
        raise ValueError(f"Unsafe node name rejected (contains disallowed metacharacters): {value!r}")
    return stripped

def safe_summary(value: str) -> str:
    """
    Sanitizes summary text for insertion into Cypher property literal:
    - Rejects dollar quote breakout ($$) or null bytes
    - Escapes backslashes and single quotes
    - Limits length to 500 characters
    """
    if not isinstance(value, str):
        raise ValueError(f"Unsafe summary rejected (must be str): {value!r}")
    if "\x00" in value:
        raise ValueError("Unsafe summary rejected (contains null byte)")
    if "$$" in value:
        raise ValueError("Unsafe summary rejected (contains dollar-quote breakout)")
    clean = value.replace("\\", "\\\\").replace("'", "\\'")[:500]
    return clean

def safe_org_id(value: str) -> str:
    """
    Validates org_id for per-tenant graph naming (workspace_graph_<org_id>).
    Must be alphanumeric, hyphens, or underscores.
    """
    if not isinstance(value, str) or not ORG_ID_RE.match(value):
        raise ValueError(f"Unsafe org_id rejected: {value!r}")
    return value


async def get_last_extraction_ts(
    workspace_id: str,
    branch_id: str,
    db_url: Optional[str] = None,
    conn: Optional[psycopg.AsyncConnection] = None
) -> Optional[float]:
    """
    Retrieves the timestamp (seconds since epoch) of the latest successful context extraction commit.
    Returns None if no previous extraction exists.
    """
    try:
        if conn is not None:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT EXTRACT(EPOCH FROM created_at) AS last_ts
                    FROM context_commits
                    WHERE workspace_id = %s AND branch_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                """, (workspace_id, branch_id))
                row = await cur.fetchone()
                return float(row["last_ts"]) if row and row.get("last_ts") is not None else None
        else:
            async with await psycopg.AsyncConnection.connect(db_url or config.db_url) as c:
                async with c.cursor(row_factory=dict_row) as cur:
                    await cur.execute("""
                        SELECT EXTRACT(EPOCH FROM created_at) AS last_ts
                        FROM context_commits
                        WHERE workspace_id = %s AND branch_id = %s
                        ORDER BY created_at DESC
                        LIMIT 1;
                    """, (workspace_id, branch_id))
                    row = await cur.fetchone()
                    return float(row["last_ts"]) if row and row.get("last_ts") is not None else None
    except Exception as e:
        logger.debug("get_last_extraction_ts returned None: %s", e)
        return None


async def commit_to_graph_and_vector(
    workspace_id: str,
    branch_id: str,
    transcript_id: str,
    extraction: ContextExtractionResult,
    org_id: Optional[str] = None,
    db_url: Optional[str] = None,
    conn: Optional[psycopg.AsyncConnection] = None,
    redis_client: Optional[aioredis.Redis] = None,
) -> Dict[str, Any]:
    """
    Persists validated extraction entities to Apache AGE per-tenant graph and pgvector
    within a single atomic transaction:
    1. Bump mutation_version first and capture new_version (FIX #5)
    2. Upsert vertices to Apache AGE via safe MERGE Cypher (FIX #1, #6)
    3. Upsert embeddings to entity_embeddings table with new_version
    4. Upsert edges to Apache AGE via safe MATCH & MERGE Cypher
    5. Write audit entry to context_commits table (FIX #14)
    6. Strip raw_payload on transcript and mark extraction_done=TRUE (FIX #7)
    7. Invalidate branch cache and broadcast mutation to collaborative clients
    """
    clean_org = safe_org_id(org_id or workspace_id)
    graph_name = f"workspace_graph_{clean_org.replace('-', '')}"  # FIX #6: per-tenant graph

    # Validate all nodes and edges upfront before initiating transaction
    validated_nodes = []
    for node in extraction.nodes:
        nid = safe_slug(node.id)
        label = safe_label(node.node_type.value)
        name = safe_name(node.name)
        summ = safe_summary(node.summary)
        validated_nodes.append((nid, label, name, summ, node.summary))

    validated_edges = []
    for edge in extraction.edges:
        src = safe_slug(edge.source_id)
        tgt = safe_slug(edge.target_id)
        etype = safe_label(edge.edge_type.value)
        validated_edges.append((src, tgt, etype))

    close_conn = False
    if conn is None:
        conn = await psycopg.AsyncConnection.connect(db_url or config.db_url)
        close_conn = True

    try:
        async with conn.transaction():
            # Initialize AGE catalog
            await conn.execute("LOAD 'age';")
            await conn.execute("SET search_path = ag_catalog, '$user', public;")

            # Step 1: Bump version FIRST and use returned value everywhere below (FIX #5)
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    UPDATE branches
                    SET mutation_version = mutation_version + 1, updated_at = NOW()
                    WHERE id = %s
                    RETURNING mutation_version;
                """, (branch_id,))
                branch_row = await cur.fetchone()
                if not branch_row:
                    raise ValueError(f"Branch not found or inactive: {branch_id}")
                new_version = branch_row["mutation_version"]

            nodes_written = 0
            for nid, label, name, summ, raw_summary in validated_nodes:
                # FIX #1: parameterized Cypher generation with validated slugs and labels
                cypher_vertex = f"""
                    SELECT * FROM cypher('{graph_name}', $$
                        MERGE (v:{label} {{id: '{nid}'}})
                        ON CREATE SET v.name='{name}', v.summary='{summ}',
                                      v.branch_id='{branch_id}', v.updated_at=timestamp()
                        ON MATCH  SET v.summary='{summ}', v.updated_at=timestamp()
                        RETURN v
                    $$) AS (v agtype);
                """
                await conn.execute(cypher_vertex)

                # Generate 1536-dim embedding vector
                vec = await generate_embedding(f"{name}: {raw_summary}")

                # Upsert into pgvector entity_embeddings table with new_version
                await conn.execute("""
                    INSERT INTO entity_embeddings
                        (workspace_id, branch_id, graph_node_id, node_type, name,
                         description, embedding, mutation_version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (branch_id, graph_node_id) DO UPDATE
                        SET description = EXCLUDED.description,
                            embedding = EXCLUDED.embedding,
                            mutation_version = EXCLUDED.mutation_version,
                            updated_at = NOW();
                """, (workspace_id, branch_id, nid, label, name, raw_summary, vec, new_version))
                nodes_written += 1

            edges_written = 0
            for src, tgt, etype in validated_edges:
                cypher_edge = f"""
                    SELECT * FROM cypher('{graph_name}', $$
                        MATCH (a {{id:'{src}'}}), (b {{id:'{tgt}'}})
                        MERGE (a)-[r:{etype} {{branch_id:'{branch_id}'}}]->(b)
                        RETURN r
                    $$) AS (r agtype);
                """
                await conn.execute(cypher_edge)
                edges_written += 1

            # Step 5: Write context_commits audit record (FIX #14)
            await conn.execute("""
                INSERT INTO context_commits
                    (workspace_id, branch_id, transcript_id, mutation_version,
                     nodes_written, edges_written)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (workspace_id, branch_id, transcript_id, new_version, nodes_written, edges_written))

            # Step 6: Mark transcript raw_payload as extractable-to-null (FIX #7)
            await conn.execute("""
                UPDATE transcripts
                SET raw_payload = NULL, extraction_done = TRUE
                WHERE id = %s;
            """, (transcript_id,))

        # Step 7: Post-transaction cache invalidation and Liveblocks broadcast
        await invalidate_branch_cache(workspace_id, branch_id, new_version, redis_client=redis_client)
        await broadcast_mutation(
            workspace_id=workspace_id,
            branch_id=branch_id,
            mutation_version=new_version,
            nodes_written=nodes_written,
            edges_written=edges_written,
            redis_client=redis_client
        )

        return {
            "mutation_version": new_version,
            "nodes_written": nodes_written,
            "edges_written": edges_written
        }
    finally:
        if close_conn:
            await conn.close()
