# Cache invalidation and Liveblocks broadcast helper
import logging

logger = logging.getLogger(__name__)

async def invalidate_branch_cache(workspace_id: str, branch_id: str, mutation_version: int):
    logger.info("Invalidated cache: ws=%s branch=%s version=%d", workspace_id, branch_id, mutation_version)

async def broadcast_mutation(workspace_id: str, branch_id: str, mutation_version: int):
    logger.info("Broadcasted mutation: ws=%s branch=%s version=%d", workspace_id, branch_id, mutation_version)
