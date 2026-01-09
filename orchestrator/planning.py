"""Planning engine for generating investigation hypotheses using Amazon Bedrock."""
from typing import List, Dict, Any, Optional
from datetime import datetime
from orchestrator.models import InvestigationHypothesis
import structlog
import os
import json

from shared.bedrock_client import BedrockClient
from shared.service_catalog import ServiceCatalog

logger = structlog.get_logger()

class PlanningEngine:
    """Planning engine for generating investigation hypotheses using Amazon Bedrock."""
    
    def __init__(self):
        # Initialize Bedrock client for planning (uses shared AWS credentials from environment)
        self.bedrock_client = BedrockClient(
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            model_id=os.getenv("PLANNING_MODEL", "claude-3-sonnet")
        )
        # Initialize service catalog for dependency-aware hypothesis generation
        self.service_catalog = ServiceCatalog()
        logger.info("Initialized Bedrock planning engine")
    
    async def extract_intent(self, question: str) -> Dict[str, Any]:
        """Extract intent and key information from the question using Bedrock."""
        system_prompt = """You are an expert at analyzing technical questions and extracting key information.
Extract entities, time references, and symptom keywords from the question."""

        user_prompt = f"""Analyze the following question and extract:
1. Key entities (services, systems, components)
2. Time references (if any)
3. Symptom keywords (errors, issues, problems)

Question: {question}

Provide your response as a JSON object with keys: entities, time_references, symptom_keywords."""

        try:
            response = await self.bedrock_client.invoke(
                prompt=user_prompt,
                system_prompt=system_prompt,
                temperature=0.3,
                max_tokens=500
            )
            
            # Try to parse JSON response
            try:
                intent_data = json.loads(response)
            except json.JSONDecodeError:
                # If not JSON, create basic structure
                intent_data = {
                    "entities": [],
                    "time_references": [],
                    "symptom_keywords": []
                }
            
            intent = {
                "question": question,
                "entities": intent_data.get("entities", []),
                "time_references": intent_data.get("time_references", []),
                "symptom_keywords": intent_data.get("symptom_keywords", [])
            }
            
            logger.info("Extracted intent", entities_count=len(intent["entities"]))
            return intent
            
        except Exception as e:
            logger.error("Failed to extract intent using Bedrock", error=str(e))
            # Fallback
            return {
                "question": question,
                "entities": [],
                "time_references": [],
                "symptom_keywords": []
            }
    
    async def generate_hypotheses(
        self,
        question: str,
        historical_context: Optional[List[Dict[str, Any]]] = None,
        intent: Optional[Dict[str, Any]] = None
    ) -> List[InvestigationHypothesis]:
        """Generate investigation hypotheses based on the question using Bedrock."""
        system_prompt = """You are an expert at root cause analysis and system troubleshooting.
Generate investigation hypotheses that will help identify the root cause of issues."""

        # Format historical context
        historical_text = ""
        if historical_context:
            historical_text = "\n\nHistorical Similar Incidents:\n"
            for i, incident in enumerate(historical_context[:3], 1):
                historical_text += f"{i}. {incident.get('document', '')[:200]}...\n"

        # Format intent information
        intent_section = ""
        service_context = ""
        if intent:
            entities = intent.get("entities", [])
            symptom_keywords = intent.get("symptom_keywords", [])
            if entities or symptom_keywords:
                intent_section = "\n\nExtracted Information:\n"
                if entities:
                    intent_section += f"Key Entities: {', '.join(entities)}\n"
                if symptom_keywords:
                    intent_section += f"Symptom Keywords: {', '.join(symptom_keywords)}\n"
                
                # Find services matching entities and get dependency information
                matched_services = self.service_catalog.find_services_by_entities(entities)
                if matched_services:
                    service_context = "\n\nService Architecture Context:\n"
                    for service in matched_services:
                        service_id = service.get("service_id")
                        service_info = self.service_catalog.get_service_info(service_id)
                        service_context += f"- Service: {service_id} (Domain: {service_info.get('domain')}, Tier: {service_info.get('tier')}, Criticality: {service_info.get('criticality', 'not specified')})\n"
                        service_context += f"  Splunk Indexes: {', '.join(service_info.get('splunk_indexes', []))}\n"
                        
                        upstream = service_info.get("upstream_dependencies", [])
                        if upstream:
                            upstream_services = [dep.get("service") if isinstance(dep, dict) else dep for dep in upstream]
                            service_context += f"  Upstream Dependencies: {', '.join(upstream_services)}\n"
                            # Add failure modes if available
                            for dep in upstream:
                                if isinstance(dep, dict):
                                    dep_service = dep.get("service")
                                    failure_modes = dep.get("failure_modes", [])
                                    if failure_modes:
                                        service_context += f"    - {dep_service} failure modes: {', '.join(failure_modes)}\n"
                        
                        downstream = service_info.get("downstream_dependencies", [])
                        if downstream:
                            service_context += f"  Downstream Dependencies: {', '.join(downstream)}\n"
                    
                    service_context += "\nWhen generating hypotheses, consider:\n"
                    service_context += "1. Check the service itself for errors\n"
                    service_context += "2. Check upstream dependencies (services this depends on)\n"
                    service_context += "3. Check downstream dependencies (services that depend on this)\n"
                    service_context += "4. Use the correct Splunk indexes for each service\n"
                    service_context += "5. Check for specific failure modes (timeout, 5xx, etc.)\n"

        user_prompt = f"""Based on the following question, generate 3-5 investigation hypotheses that should be tested to find the root cause.

Question: {question}
{intent_section}{service_context}{historical_text}

For each hypothesis, provide:
1. A clear hypothesis statement
2. A priority (1 = highest, 5 = lowest)
3. A suggested SPL query template (optional)

Focus on the entities and symptom keywords identified above when generating hypotheses.
Use the service architecture context to generate hypotheses that follow dependency chains.
Prioritize hypotheses for high-criticality services and check upstream dependencies when a service fails.
Respond in JSON format with a list of hypotheses, each with: hypothesis, priority, query_template."""

        try:
            response = await self.bedrock_client.invoke(
                prompt=user_prompt,
                system_prompt=system_prompt,
                temperature=0.5,
                max_tokens=1000
            )
            
            # Try to parse JSON response
            try:
                # Extract JSON from response if wrapped in markdown
                if "```json" in response:
                    response = response.split("```json")[1].split("```")[0].strip()
                elif "```" in response:
                    response = response.split("```")[1].split("```")[0].strip()
                
                hypotheses_data = json.loads(response)
                if isinstance(hypotheses_data, dict) and "hypotheses" in hypotheses_data:
                    hypotheses_data = hypotheses_data["hypotheses"]
                
                hypotheses = []
                for h in hypotheses_data:
                    hypotheses.append(InvestigationHypothesis(
                        hypothesis=h.get("hypothesis", "Unknown hypothesis"),
                        priority=int(h.get("priority", 5)),
                        query_template=h.get("query_template")
                    ))
                
                # Sort by priority
                hypotheses.sort(key=lambda x: x.priority)
                
                logger.info("Generated hypotheses using Bedrock", count=len(hypotheses))
                return hypotheses
                
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning("Failed to parse hypotheses JSON, using fallback", error=str(e))
                return self._generate_fallback_hypotheses(question)
                
        except Exception as e:
            logger.error("Failed to generate hypotheses using Bedrock", error=str(e))
            return self._generate_fallback_hypotheses(question)
    
    def _generate_fallback_hypotheses(self, question: str) -> List[InvestigationHypothesis]:
        """Generate fallback hypotheses if Bedrock fails."""
        hypotheses = [
            InvestigationHypothesis(
                hypothesis="Check for error logs matching the symptom",
                priority=1,
                query_template="index=* error OR failed OR exception | timechart count"
            ),
            InvestigationHypothesis(
                hypothesis="Check for service outages or degradation",
                priority=2,
                query_template="index=* status=* | stats count by status"
            ),
        ]
        logger.info("Generated fallback hypotheses", count=len(hypotheses))
        return hypotheses
