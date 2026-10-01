import pytest
from unittest.mock import AsyncMock
from services.worker.src.main import process_transcript
from services.worker.src.models import ContextExtractionResult, ExtractedNode, NodeType
from services.worker.src.extractor import ContextExtractor

class MockRedisClient:
    """In-memory Redis mock supporting async set(nx=True, ex=...)"""
    def __init__(self):
        self.store = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx:
            if key in self.store:
                return False
            self.store[key] = value
            return True
        self.store[key] = value
        return True

    async def get(self, key):
        return self.store.get(key)

@pytest.mark.asyncio
async def test_duplicate_delivery_single_graph_write():
    """
    Phase 5 Test:
    Send same idempotency key twice -> assert single graph write.
    """
    mock_redis = MockRedisClient()
    mock_extractor = AsyncMock(spec=ContextExtractor)
    mock_extractor.extract_context.return_value = ContextExtractionResult(
        has_meaningful_context=True,
        nodes=[
            ExtractedNode(
                id="service-auth",
                name="Auth Service",
                node_type=NodeType.COMPONENT,
                summary="JWT and session verification",
                confidence=0.9
            )
        ],
        edges=[]
    )
    mock_commit = AsyncMock()

    idem_key = "ws-1:main:1727800800000"
    event = {
        "idempotency_key": idem_key,
        "workspace_id": "ws-1",
        "branch_id": "main",
        "transcript_id": "tx-100",
        "turns": [
            {"role": "user", "content": "Deploy Auth Service with JWT sessions."}
        ]
    }

    # Delivery 1: Fresh event
    res1 = await process_transcript(event, mock_redis, mock_extractor, commit_fn=mock_commit)
    assert res1 is True
    assert mock_extractor.extract_context.call_count == 1
    assert mock_commit.call_count == 1
    assert await mock_redis.get(f"idem:{idem_key}") == "1"

    # Delivery 2: Exact duplicate event (network replay / retry)
    res2 = await process_transcript(event, mock_redis, mock_extractor, commit_fn=mock_commit)
    assert res2 is True
    # Asserts that extractor and graph write were NOT called a second time
    assert mock_extractor.extract_context.call_count == 1
    assert mock_commit.call_count == 1  # EXACTLY single graph write!

@pytest.mark.asyncio
async def test_different_idempotency_keys_trigger_separate_writes():
    """Different idempotency keys on the same branch both execute graph writes."""
    mock_redis = MockRedisClient()
    mock_extractor = AsyncMock(spec=ContextExtractor)
    mock_extractor.extract_context.return_value = ContextExtractionResult(
        has_meaningful_context=True,
        nodes=[ExtractedNode(id="node-a", name="Node A", node_type=NodeType.COMPONENT, summary="Summary", confidence=1.0)],
        edges=[]
    )
    mock_commit = AsyncMock()

    event1 = {
        "idempotency_key": "ws-1:main:1001",
        "workspace_id": "ws-1",
        "branch_id": "main",
        "transcript_id": "tx-1",
        "turns": [{"role": "user", "content": "Add Node A"}]
    }
    event2 = {
        "idempotency_key": "ws-1:main:1002",
        "workspace_id": "ws-1",
        "branch_id": "main",
        "transcript_id": "tx-2",
        "turns": [{"role": "user", "content": "Add Node B"}]
    }

    await process_transcript(event1, mock_redis, mock_extractor, commit_fn=mock_commit)
    await process_transcript(event2, mock_redis, mock_extractor, commit_fn=mock_commit)

    assert mock_commit.call_count == 2
