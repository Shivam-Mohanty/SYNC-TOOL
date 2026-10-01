import os
import json
import logging
from typing import Optional
from google import genai
from google.genai import types

from .models import ContextExtractionResult
from .config import config

logger = logging.getLogger(__name__)

EXTRACTION_SYSTEM_PROMPT = """
You are an expert software architect analyzing development transcripts to extract structured knowledge into a team context graph.

Your task:
Analyze the conversation between developer(s) and AI assistant(s) to identify architectural knowledge.

Extract entities belonging ONLY to these categories:
1. Component: Services, libraries, modules, databases, API gateways, storage engines.
2. Decision: Key technical or architectural choices made (e.g. choice of database, protocol, algorithm).
3. Constraint: Hard requirements, performance limits, security boundaries, or compliance rules.
4. Task: Concrete development tasks or roadmap items identified.

Extract relationships between entities:
- DEPENDS_ON: Component -> Component
- MODIFIES: Task/Component -> Component
- CONSTRAINED_BY: Decision/Component -> Constraint
- DECIDED_IN: Decision -> Component
- BLOCKS: Task/Constraint -> Task

Rules:
- ID must be a lowercase slug containing alphanumeric and hyphens (max 128 characters, e.g., 'redis-cache', 'byok-encryption').
- Summary must be at most 2 sentences.
- Confidence must be between 0.0 and 1.0.
- If the conversation is purely casual (syntax fixing, greetings, trivial Q&A) and contains no architectural context, set has_meaningful_context to false and leave nodes and edges empty.
"""

class ContextExtractor:
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key or config.gemini_api_key
        self.model = model or config.gemini_model
        self.client = None
        if self.api_key:
            self.client = genai.Client(api_key=self.api_key)

    async def extract_context(self, transcript_text: str) -> ContextExtractionResult:
        """
        Sends the transcript text to Gemini Flash with structured JSON schema output
        and validates with ContextExtractionResult Pydantic schema.
        """
        if not transcript_text.strip():
            return ContextExtractionResult(has_meaningful_context=False)

        if not self.client:
            logger.warning("No GEMINI_API_KEY provided; returning empty context")
            return ContextExtractionResult(has_meaningful_context=False)

        # Call Gemini Flash with structured output schema
        response = self.client.models.generate_content(
            model=self.model,
            contents=transcript_text,
            config=types.GenerateContentConfig(
                system_instruction=EXTRACTION_SYSTEM_PROMPT,
                response_mime_type="application/json",
                response_schema=ContextExtractionResult,
                temperature=0.1,
            ),
        )

        raw_json = response.text
        return ContextExtractionResult.model_validate_json(raw_json)
