"""LLM client for generating SPL queries using Amazon Bedrock."""
from typing import Optional, Dict, Any
import structlog
import os

from query_generator.config import QueryGeneratorConfig
from shared.bedrock_client import BedrockClient
from shared.service_catalog import ServiceCatalog

logger = structlog.get_logger()

class LLMClient:
    """LLM client for generating SPL queries using Amazon Bedrock."""
    
    def __init__(self, config: QueryGeneratorConfig):
        self.config = config
        # Initialize Bedrock client (uses shared AWS credentials from environment)
        self.bedrock_client = BedrockClient(
            region_name=config.aws_region or os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            model_id=config.llm_model
        )
        # Initialize service catalog for index-aware query generation
        self.service_catalog = ServiceCatalog()
        logger.info("Initialized Bedrock LLM client", provider=config.llm_provider, model=config.llm_model)
    
    async def generate_spl_query(
        self,
        hypothesis: str,
        question: str,
        historical_examples: Optional[str] = None,
        intent: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate SPL query using Amazon Bedrock."""
        system_prompt = """You are a Splunk Query Language (SPL) expert. Your task is to generate valid SPL queries based on investigation hypotheses.

Guidelines:
- Generate only valid SPL queries
- Do not include explanations, markdown formatting, or code blocks
- Use appropriate SPL commands and syntax
- Include time constraints when relevant
- Focus on the specific hypothesis provided
- Use the extracted entities and keywords to make queries more precise"""

        # Build historical context section
        historical_section = ""
        if historical_examples:
            historical_section = f"\nHistorical similar queries:\n{historical_examples}\n"
        
        # Build intent section with entities and keywords
        intent_section = ""
        index_context = ""
        if intent:
            entities = intent.get("entities", [])
            symptom_keywords = intent.get("symptom_keywords", [])
            if entities or symptom_keywords:
                intent_section = "\nExtracted Information:\n"
                if entities:
                    intent_section += f"Key Entities to search for: {', '.join(entities)}\n"
                if symptom_keywords:
                    intent_section += f"Keywords/Patterns to match: {', '.join(symptom_keywords)}\n"
                intent_section += "Use these entities and keywords in your SPL query to make it more targeted.\n"
                
                # Get Splunk indexes for matched services
                matched_services = self.service_catalog.find_services_by_entities(entities)
                if matched_services:
                    index_context = "\nSplunk Index Information:\n"
                    for service in matched_services:
                        service_id = service.get("service_id")
                        indexes = self.service_catalog.get_splunk_indexes(service_id)
                        if indexes:
                            index_context += f"- Service '{service_id}' uses indexes: {', '.join(indexes)}\n"
                            index_context += f"  Use 'index={indexes[0]}' or 'index={' OR index='.join(indexes)}' in your SPL query for this service.\n"
                    
                    if not index_context.endswith("\n\n"):
                        index_context += "\n"
                    index_context += "IMPORTANT: Use the correct Splunk indexes from the service catalog above. Do not guess or hallucinate index names.\n"
        
        user_prompt = f"""Generate a Splunk Query Language (SPL) query to investigate the following hypothesis.

Hypothesis: {hypothesis}
Original Question: {question}
{intent_section}{index_context}{historical_section}
Generate only the SPL query without any additional text, explanations, or markdown formatting.
Use the correct Splunk indexes from the service catalog information provided above."""

        try:
            response = await self.bedrock_client.invoke(
                prompt=user_prompt,
                system_prompt=system_prompt,
                temperature=self.config.llm_temperature,
                max_tokens=500
            )
            
            # Clean up the response - remove markdown code blocks if present
            query = response.strip()
            query = query.replace("```spl", "").replace("```", "").strip()
            # Remove any leading/trailing quotes
            query = query.strip('"').strip("'")
            
            logger.info("Generated SPL query using Bedrock", query=query[:100])
            return query
            
        except Exception as e:
            logger.error("Failed to generate SPL query using Bedrock", error=str(e))
            # Fallback to a simple query
            return "index=* error OR failed | stats count by source"
