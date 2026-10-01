import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from services.worker.src.storage.graph import (
    commit_to_graph_and_vector,
    get_last_extraction_ts,
    safe_slug,
    safe_name,
)
from services.worker.src.models import (
    ContextExtractionResult,
    ExtractedNode,
    NodeType,
    ExtractedEdge,
    EdgeType,
)

class MockAsyncCursor:
    def __init__(self, queries_log, fetch_results):
        self.queries_log = queries_log
        self.fetch_results = fetch_results
        self.current_fetch = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def execute(self, query, params=None):
        self.queries_log.append({"query": query.strip(), "params": params})
        for key, res in self.fetch_results.items():
            if key in query:
                self.current_fetch = res
                return

    async def fetchone(self):
        return self.current_fetch

class MockAsyncConnection:
    def __init__(self, branch_version=42, fail_on=None):
        self.branch_version = branch_version
        self.fail_on = fail_on
        self.executed_queries = []
        self.transaction_committed = False
        self.transaction_rolled_back = False
        self.in_transaction = False

    class _TransactionContext:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            self.conn.in_transaction = True
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            self.conn.in_transaction = False
            if exc_type is not None:
                self.conn.transaction_rolled_back = True
            else:
                self.conn.transaction_committed = True

    def transaction(self):
        return self._TransactionContext(self)

    def cursor(self, row_factory=None):
        return MockAsyncCursor(
            self.executed_queries,
            {
                "UPDATE branches": {"mutation_version": self.branch_version},
                "SELECT EXTRACT(EPOCH FROM created_at)": {"last_ts": 1727800800.5}
            }
        )

    async def execute(self, query, params=None):
        if self.fail_on and self.fail_on in query:
            raise RuntimeError(f"Simulated failure on query: {self.fail_on}")
        self.executed_queries.append({"query": query.strip(), "params": params})

    async def close(self):
        pass

@pytest.mark.asyncio
async def test_atomic_transaction_full_flow():
    """
    Verifies the atomic execution sequence:
    1. Bump mutation_version first (FIX #5)
    2. Upsert AGE Cypher nodes
    3. Upsert pgvector embeddings with new_version
    4. Upsert AGE Cypher edges
    5. Write audit context_commits with new_version (FIX #14)
    6. Strip raw_payload on transcripts (FIX #7)
    7. Invalidate cache and broadcast mutation
    """
    mock_conn = MockAsyncConnection(branch_version=43)
    mock_redis = AsyncMock()

    extraction = ContextExtractionResult(
        has_meaningful_context=True,
        nodes=[
            ExtractedNode(
                id="api-gateway",
                name="API Gateway",
                node_type=NodeType.COMPONENT,
                summary="Chi HTTP SSE proxy",
                confidence=0.95
            ),
            ExtractedNode(
                id="redis-queue",
                name="Redis Stream",
                node_type=NodeType.COMPONENT,
                summary="Completed transcripts queue",
                confidence=0.9
            )
        ],
        edges=[
            ExtractedEdge(
                source_id="api-gateway",
                target_id="redis-queue",
                edge_type=EdgeType.DEPENDS_ON
            )
        ]
    )

    with patch("services.worker.src.storage.graph.invalidate_branch_cache", new_callable=AsyncMock) as mock_inv, \
         patch("services.worker.src.storage.graph.broadcast_mutation", new_callable=AsyncMock) as mock_bcast:

        result = await commit_to_graph_and_vector(
            workspace_id="ws-uuid-1",
            branch_id="branch-uuid-1",
            transcript_id="tx-uuid-1",
            extraction=extraction,
            org_id="acme",
            conn=mock_conn,
            redis_client=mock_redis
        )

        assert result["mutation_version"] == 43
        assert result["nodes_written"] == 2
        assert result["edges_written"] == 1
        assert mock_conn.transaction_committed is True
        assert mock_conn.transaction_rolled_back is False

        # Verify query order and presence
        queries = [entry["query"] for entry in mock_conn.executed_queries]

        # 1. AGE initialization
        assert any("LOAD 'age'" in q for q in queries)
        assert any("SET search_path = ag_catalog" in q for q in queries)

        # 2. Version bump was first write
        assert any("UPDATE branches" in q and "mutation_version = mutation_version + 1" in q for q in queries)

        # 3. Cypher vertices merged into tenant graph
        cypher_nodes = [q for q in queries if "MERGE (v:Component" in q]
        assert len(cypher_nodes) == 2
        assert "workspace_graph_acme" in cypher_nodes[0]
        assert "api-gateway" in cypher_nodes[0]
        assert "redis-queue" in cypher_nodes[1]

        # 4. Embeddings upserted with new_version (43)
        embedding_inserts = [entry for entry in mock_conn.executed_queries if "INSERT INTO entity_embeddings" in entry["query"]]
        assert len(embedding_inserts) == 2
        for emb in embedding_inserts:
            assert emb["params"][7] == 43  # new_version parameter
            assert len(emb["params"][6]) == 1536  # 1536-dim vector

        # 5. Cypher edges merged
        cypher_edges = [q for q in queries if "MERGE (a)-[r:DEPENDS_ON" in q]
        assert len(cypher_edges) == 1
        assert "api-gateway" in cypher_edges[0]
        assert "redis-queue" in cypher_edges[0]

        # 6. Audit commit record
        audit_inserts = [entry for entry in mock_conn.executed_queries if "INSERT INTO context_commits" in entry["query"]]
        assert len(audit_inserts) == 1
        assert audit_inserts[0]["params"][3] == 43  # mutation_version
        assert audit_inserts[0]["params"][4] == 2   # nodes_written
        assert audit_inserts[0]["params"][5] == 1   # edges_written

        # 7. Transcript raw_payload stripped
        transcript_updates = [entry for entry in mock_conn.executed_queries if "UPDATE transcripts" in entry["query"]]
        assert len(transcript_updates) == 1
        assert "raw_payload = NULL" in transcript_updates[0]["query"]
        assert "extraction_done = TRUE" in transcript_updates[0]["query"]

        # 8. Post-transaction notifications dispatched
        mock_inv.assert_awaited_once_with("ws-uuid-1", "branch-uuid-1", 43, redis_client=mock_redis)
        mock_bcast.assert_awaited_once_with(
            workspace_id="ws-uuid-1",
            branch_id="branch-uuid-1",
            mutation_version=43,
            nodes_written=2,
            edges_written=1,
            redis_client=mock_redis
        )

@pytest.mark.asyncio
async def test_atomic_transaction_rolls_back_on_error():
    """
    Verifies that if any step inside the transaction fails,
    the transaction is rolled back, and no cache or broadcast events are fired.
    """
    mock_conn = MockAsyncConnection(branch_version=43, fail_on="INSERT INTO context_commits")
    mock_redis = AsyncMock()

    extraction = ContextExtractionResult(
        has_meaningful_context=True,
        nodes=[ExtractedNode(id="node-1", name="Node 1", node_type=NodeType.TASK, summary="Task summary", confidence=1.0)],
        edges=[]
    )

    with patch("services.worker.src.storage.graph.invalidate_branch_cache", new_callable=AsyncMock) as mock_inv, \
         patch("services.worker.src.storage.graph.broadcast_mutation", new_callable=AsyncMock) as mock_bcast:

        with pytest.raises(RuntimeError, match="Simulated failure on query"):
            await commit_to_graph_and_vector(
                workspace_id="ws-1",
                branch_id="main",
                transcript_id="tx-1",
                extraction=extraction,
                conn=mock_conn,
                redis_client=mock_redis
            )

        assert mock_conn.transaction_rolled_back is True
        assert mock_conn.transaction_committed is False
        mock_inv.assert_not_called()
        mock_bcast.assert_not_called()

@pytest.mark.asyncio
async def test_get_last_extraction_ts_returns_epoch():
    """Verifies get_last_extraction_ts extracts and returns float timestamp."""
    mock_conn = MockAsyncConnection()
    ts = await get_last_extraction_ts("ws-1", "main", conn=mock_conn)
    assert ts == 1727800800.5
