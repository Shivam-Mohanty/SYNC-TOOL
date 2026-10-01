import json
import logging
import time
from typing import Optional
import redis.asyncio as aioredis
from ..config import config

logger = logging.getLogger(__name__)

async def invalidate_branch_cache(
    workspace_id: str,
    branch_id: str,
    mutation_version: int,
    redis_client: Optional[aioredis.Redis] = None
) -> None:
    """
    Invalidates cached context for a branch in Redis and publishes an invalidation notice.
    """
    cache_key = f"cache:branch:{workspace_id}:{branch_id}:context"
    ver_key = f"branch:{workspace_id}:{branch_id}:version"
    channel = "branch:invalidated"

    close_client = False
    if redis_client is None:
        try:
            redis_client = aioredis.from_url(config.redis_url)
            close_client = True
        except Exception as e:
            logger.warning("Could not connect to Redis for cache invalidation: %s", e)
            return

    try:
        # Delete context cache and update version key
        await redis_client.delete(cache_key)
        await redis_client.set(ver_key, str(mutation_version))
        # Publish invalidation event for distributed cache sync
        payload = json.dumps({
            "event": "BRANCH_CACHE_INVALIDATED",
            "workspace_id": workspace_id,
            "branch_id": branch_id,
            "mutation_version": mutation_version,
            "timestamp": time.time()
        })
        await redis_client.publish(channel, payload)
        logger.info("Invalidated cache: ws=%s branch=%s version=%d", workspace_id, branch_id, mutation_version)
    except Exception as e:
        logger.error("Failed to invalidate branch cache in Redis: %s", e)
    finally:
        if close_client:
            await redis_client.aclose()


async def broadcast_mutation(
    workspace_id: str,
    branch_id: str,
    mutation_version: int,
    nodes_written: int = 0,
    edges_written: int = 0,
    redis_client: Optional[aioredis.Redis] = None
) -> None:
    """
    Broadcasts mutation notification to Liveblocks / collaborative clients via Redis pub/sub.
    """
    channel = "liveblocks:mutations"
    close_client = False
    if redis_client is None:
        try:
            redis_client = aioredis.from_url(config.redis_url)
            close_client = True
        except Exception as e:
            logger.warning("Could not connect to Redis for broadcast: %s", e)
            return

    try:
        payload = json.dumps({
            "type": "MUTATION_COMMITTED",
            "workspace_id": workspace_id,
            "branch_id": branch_id,
            "mutation_version": mutation_version,
            "nodes_written": nodes_written,
            "edges_written": edges_written,
            "timestamp_ms": int(time.time() * 1000)
        })
        await redis_client.publish(channel, payload)
        logger.info(
            "Broadcasted mutation to %s: ws=%s branch=%s version=%d (nodes=%d, edges=%d)",
            channel, workspace_id, branch_id, mutation_version, nodes_written, edges_written
        )
    except Exception as e:
        logger.error("Failed to broadcast mutation: %s", e)
    finally:
        if close_client:
            await redis_client.aclose()
