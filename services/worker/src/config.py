import os
from pydantic import BaseModel

class WorkerConfig(BaseModel):
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    db_url: str = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/sync_tool")
    stream_name: str = os.getenv("STREAM_NAME", "queue:transcripts:completed")
    dlq_stream_name: str = os.getenv("DLQ_STREAM_NAME", "queue:transcripts:dlq")
    consumer_group: str = os.getenv("CONSUMER_GROUP", "extraction_workers")
    gemini_api_key: str = os.getenv("GEMINI_API_KEY", "")
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
    token_budget: int = int(os.getenv("TOKEN_BUDGET", "6000"))

config = WorkerConfig()
