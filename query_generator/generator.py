"""Splunk query generator with LLM and guardrails."""
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
import structlog

from query_generator.config import QueryGeneratorConfig
from query_generator.guardrails import QueryGuardrails
from query_generator.llm_client import LLMClient
from splunk_integration.client import SplunkClient

logger = structlog.get_logger()

class SplunkQueryGenerator:
    """Splunk query generator with LLM and guardrails."""
    
    def __init__(self):
        self.config = QueryGeneratorConfig()
        self.guardrails = QueryGuardrails()
        self.llm_client = LLMClient(self.config)
        self.splunk_client = SplunkClient()
    
    async def generate_query(
        self,
        hypothesis: str,
        question: str,
        time_window: Tuple[datetime, datetime],
        historical_context: Optional[list] = None,
        intent: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate and validate SPL query for a hypothesis."""
        # Format historical context
        historical_examples = None
        if historical_context:
            historical_examples = "\n".join([
                f"- {inc.get('document', '')[:200]}" for inc in historical_context[:3]
            ])
        
        # Generate query using LLM
        query = await self.llm_client.generate_spl_query(
            hypothesis=hypothesis,
            question=question,
            historical_examples=historical_examples,
            intent=intent
        )
        
        # Validate query
        if self.config.enable_guardrails:
            self.guardrails.validate_query(query)
            query = self.guardrails.constrain_query(query, time_window)
        
        logger.info("Generated validated SPL query", query=query[:100])
        return query
    
    async def execute_query(self, query: str) -> Dict[str, Any]:
        """Execute SPL query through Splunk API."""
        results = await self.splunk_client.search(query)
        logger.info("Executed SPL query", results_count=len(results.get("results", [])))
        return results

