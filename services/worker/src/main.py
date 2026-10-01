import asyncio
import json
import logging
import os
import signal
import sys
from typing import Optional, Any
import redis.asyncio as aioredis

from .config import config
from .models import ContextExtractionResult
from .minimizer import minimize_transcript, format_turns_for_llm
from .extractor import ContextExtractor
from .storage.graph import get_last_extraction_ts, commit_to_graph_and_vector

if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("worker")

async def process_transcript(
    event: dict,
    redis_client: aioredis.Redis,
    extractor: ContextExtractor,
    commit_fn: Optional[Any] = None
) -> bool:
    """
    Processes a single transcript event:
    1. Idempotency check with SET NX (24h TTL)
    2. Minimizes turns (system-strip -> heuristic filter -> delta trim -> budget cap)
    3. Structured extraction via Gemini Flash (or mock if unconfigured)
    4. Validates Pydantic v2 schema
    5. Dispatches to graph and vector commit
    """
    idem_key = f"idem:{event['idempotency_key']}"

    # FIX #10: idempotency check — skip if already processed within 24h
    is_new = await redis_client.set(idem_key, "1", nx=True, ex=86400)
    if not is_new:
        logger.info("Duplicate delivery detected for key %s; skipped", event['idempotency_key'])
        return True

    workspace_id = event["workspace_id"]
    branch_id = event["branch_id"]
    turns = event.get("turns", [])

    last_ts = await get_last_extraction_ts(workspace_id, branch_id)
    minimized_turns = minimize_transcript(turns, last_ts, config.token_budget)

    if not minimized_turns:
        logger.info("Transcript minimized to 0 turns; skipping LLM extraction")
        return True

    transcript_text = format_turns_for_llm(minimized_turns)
    result: ContextExtractionResult = await extractor.extract_context(transcript_text)

    if not result.has_meaningful_context or not result.nodes:
        logger.info("No meaningful architectural context found in transcript; graph untouched")
        return True

    if commit_fn is not None:
        await commit_fn(
            workspace_id=workspace_id,
            branch_id=branch_id,
            transcript_id=event.get("transcript_id", ""),
            extraction=result,
            org_id=event.get("org_id", workspace_id)
        )
    else:
        await commit_to_graph_and_vector(
            workspace_id=workspace_id,
            branch_id=branch_id,
            transcript_id=event.get("transcript_id", ""),
            extraction=result,
            org_id=event.get("org_id", workspace_id)
        )
    return True

async def worker_loop(
    redis_client: Optional[aioredis.Redis] = None,
    extractor: Optional[ContextExtractor] = None,
    stop_event: Optional[asyncio.Event] = None
):
    """
    Main extraction worker consumer loop:
    Reads from Redis Streams consumer group with ACK-only-on-success / confirmed DLQ pattern.
    """
    if redis_client is None:
        redis_client = aioredis.from_url(config.redis_url)
    if extractor is None:
        extractor = ContextExtractor()
    if stop_event is None:
        stop_event = asyncio.Event()

    group_name = config.consumer_group
    consumer_name = f"worker_{os.getpid()}"
    stream_name = config.stream_name
    dlq_name = config.dlq_stream_name

    try:
        await redis_client.xgroup_create(stream_name, group_name, mkstream=True)
        logger.info("Created consumer group %s on stream %s", group_name, stream_name)
    except Exception as e:
        # Group already exists
        if "BUSYGROUP" not in str(e):
            logger.warning("xgroup_create notice: %s", e)

    logger.info("Worker started: consumer=%s listening on stream=%s", consumer_name, stream_name)

    while not stop_event.is_set():
        try:
            entries = await redis_client.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={stream_name: ">"},
                count=5,
                block=2000
            )
        except asyncio.CancelledError:
            break
        except Exception as read_err:
            logger.error("Error reading from Redis Stream: %s", read_err)
            await asyncio.sleep(1)
            continue

        if not entries:
            continue

        for _, messages in entries:
            for msg_id, data in messages:
                success = False
                confirmed_dlq = False
                raw_payload = data.get(b"data") or data.get("data")
                if isinstance(raw_payload, bytes):
                    raw_str = raw_payload.decode("utf-8")
                else:
                    raw_str = str(raw_payload)

                try:
                    event = json.loads(raw_str)
                    await process_transcript(event, redis_client, extractor)
                    success = True
                except Exception as e:  # FIX #12: single except, not redundant tuple
                    logger.error("Error processing transcript %s: %s; routing to DLQ", msg_id, e)
                    try:
                        await redis_client.xadd(dlq_name, {
                            "payload": raw_str,
                            "error": str(e),
                        })
                        confirmed_dlq = True
                    except Exception as dlq_err:
                        logger.critical("Failed to enqueue to DLQ %s: %s", dlq_name, dlq_err)
                finally:
                    # FIX #3: ACK only on success or confirmed DLQ
                    if success or confirmed_dlq:
                        msg_id_str = msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id)
                        await redis_client.xack(stream_name, group_name, msg_id_str)

    logger.info("Worker loop gracefully stopped")

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop_event = asyncio.Event()

    def handle_signal():
        logger.info("Received termination signal; shutting down worker...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            # Signal handlers not implemented on Windows event loop for add_signal_handler
            pass

    try:
        loop.run_until_complete(worker_loop(stop_event=stop_event))
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received; exiting")
    finally:
        loop.close()

if __name__ == "__main__":
    main()
