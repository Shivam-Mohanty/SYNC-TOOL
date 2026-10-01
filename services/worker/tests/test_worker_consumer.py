import asyncio
from unittest.mock import AsyncMock
from services.worker.src.main import process_transcript, worker_loop
from services.worker.src.models import ContextExtractionResult, ExtractedNode, NodeType
from services.worker.src.extractor import ContextExtractor

def test_idempotency_check_skips_duplicates():
    async def _test():
        # Setup mock redis
        mock_redis = AsyncMock()
        # First call to SET NX returns True, second returns False (duplicate)
        mock_redis.set.side_effect = [True, False]

        # Setup mock extractor
        mock_extractor = AsyncMock(spec=ContextExtractor)
        mock_extractor.extract_context.return_value = ContextExtractionResult(
            has_meaningful_context=True,
            nodes=[ExtractedNode(id="node-1", name="Node 1", node_type=NodeType.COMPONENT, summary="Test", confidence=1.0)],
            edges=[]
        )

        event = {
            "idempotency_key": "ws-1:main:1700000000000",
            "workspace_id": "ws-1",
            "branch_id": "main",
            "transcript_id": "tx-1",
            "turns": [
                {"role": "user", "content": "Adopt Apache AGE for graph storage"}
            ]
        }

        # First call: is new -> processed
        res1 = await process_transcript(event, mock_redis, mock_extractor)
        assert res1 is True
        assert mock_extractor.extract_context.call_count == 1

        # Second call: duplicate key -> skipped without invoking extractor
        res2 = await process_transcript(event, mock_redis, mock_extractor)
        assert res2 is True
        assert mock_extractor.extract_context.call_count == 1  # Unchanged!

    asyncio.run(_test())

def test_worker_consumer_acks_on_success():
    async def _test():
        mock_redis = AsyncMock()
        mock_redis.set.return_value = True

        # Single message entry from xreadgroup
        msg_id = b"1700000000000-0"
        payload = b'{"idempotency_key":"ws-1:main:123","workspace_id":"ws-1","branch_id":"main","turns":[{"role":"user","content":"Adopt AGE"}]}'
        mock_redis.xreadgroup.side_effect = [
            [("queue:transcripts:completed", [(msg_id, {b"data": payload})])],
            []  # next loop empty
        ]

        mock_extractor = AsyncMock(spec=ContextExtractor)
        mock_extractor.extract_context.return_value = ContextExtractionResult(has_meaningful_context=False)

        stop_event = asyncio.Event()

        async def run_worker():
            await asyncio.sleep(0.05)
            stop_event.set()

        asyncio.create_task(run_worker())
        await worker_loop(redis_client=mock_redis, extractor=mock_extractor, stop_event=stop_event)

        # Verify message was ACK'd
        mock_redis.xack.assert_called_once()
        assert mock_redis.xack.call_args[0][0] == "queue:transcripts:completed"

    asyncio.run(_test())

def test_worker_consumer_routes_to_dlq_on_error():
    async def _test():
        mock_redis = AsyncMock()
        mock_redis.set.return_value = True

        # Malformed JSON payload that causes exception
        msg_id = b"1700000000001-0"
        bad_payload = b'{"not_json_closed": true'
        mock_redis.xreadgroup.side_effect = [
            [("queue:transcripts:completed", [(msg_id, {b"data": bad_payload})])],
            []
        ]

        mock_extractor = AsyncMock(spec=ContextExtractor)
        stop_event = asyncio.Event()

        async def run_worker():
            await asyncio.sleep(0.05)
            stop_event.set()

        asyncio.create_task(run_worker())
        await worker_loop(redis_client=mock_redis, extractor=mock_extractor, stop_event=stop_event)

        # Verify pushed to DLQ
        mock_redis.xadd.assert_called_once()
        assert mock_redis.xadd.call_args[0][0] == "queue:transcripts:dlq"
        # Verify ACK'd after confirmed DLQ write
        mock_redis.xack.assert_called_once()

    asyncio.run(_test())
