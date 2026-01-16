"""Planning engine for generating investigation hypotheses using Amazon Bedrock."""
from typing import List, Dict, Any, Optional
from datetime import datetime
from orchestrator.models import InvestigationHypothesis
import structlog
import os
import json
import re

from shared.bedrock_client import BedrockClient
from shared.service_catalog import ServiceCatalog
from shared.logfields_catalog import LogFieldsCatalog

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
        self.logfields_catalog = LogFieldsCatalog()
        logger.info("Initialized Bedrock planning engine")
    
    async def extract_intent(self, question: str) -> Dict[str, Any]:
        """Extract intent and key information from the question using Bedrock.
        Service catalog aware - only extracts entities that match known services."""
        
        # Get available services and their information from catalog
        available_services = {}
        for service_id, service_data in self.service_catalog.services.items():
            indexes = self.service_catalog.get_splunk_indexes(service_id)
            available_services[service_id] = {
                "service_id": service_id,
                "domain": service_data.get("domain"),
                "tier": service_data.get("tier"),
                "splunk_indexes": indexes
            }
        
        # Build service catalog context for the LLM
        service_catalog_context = "Available Services in Catalog and their associated domains, splunk indexes and apps:\n"
        for service_id, info in available_services.items():
            service_catalog_context += f"- {service_id}"
            if info.get("domain"):
                service_catalog_context += f" (domain: {info['domain']}, tier: {info['tier']})"
            if info.get("splunk_indexes"):
                service_catalog_context += f" - Splunk indexes: {', '.join(info['splunk_indexes'])}"
            apps = self.service_catalog.get_apps(service_id)
            if apps:
                service_catalog_context += f" - Apps: {', '.join(apps)}"
            service_catalog_context += "\n"
        
        system_prompt = """You are an expert at analyzing technical questions about Splunk logs and extracting key information for investigation.
Extract:
- services (catalog-bound)
- indexes (catalog-bound)
- apps (catalog-bound)
- entities/identifiers (transactionId/traceId/certId/UUIDs/hostnames/etc; NOT catalog-bound)
- time references
- symptom keywords

IMPORTANT:
- Only extract service names that exist in the provided service catalog.
- Only extract Splunk index names that appear in the provided service catalog.
- Only extract app names that appear in the provided service catalog.
- Do not guess/hallucinate service names, index names, or app names."""

        logfields_context = self.logfields_catalog.as_prompt_block()

        user_prompt = f"""Analyze the following question and extract:
1. Services - ONLY service names from the catalog below
2. Indexes - ONLY index names from the catalog below
3. Apps - ONLY app names from the catalog below (these map to a service/index)
4. Entities/Identifiers - transactionId values, trace IDs, UUIDs, cert IDs, hostnames, error strings (NOT catalog-bound)
5. Time references (if any)
6. Symptom keywords (errors, issues, problems)
7. Special query patterns:
   - "origin" / "first occurrence" / "earliest" → find the FIRST/EARLIEST occurrence
   - "trace" / "follow" → follow a transaction/request
   - "count" / "how many" → aggregate/count results

{service_catalog_context}
{logfields_context}

Question: {question}

CRITICAL INSTRUCTIONS:
- Services MUST EXACTLY match one of the services listed in the catalog above (use the exact service_id).
- Indexes MUST match one of the catalog indexes above.
- Do NOT guess/hallucinate service names or indexes.

Provide your response as a JSON object with keys: services, indexes, entities, time_references, symptom_keywords, query_patterns.
- services: service_id values from the catalog
- indexes: index names from the catalog
- apps: app names from the catalog
- entities: identifiers (transactionId/traceId/certId/UUIDs/hostnames/etc)
- time_references: any time windows mentioned
- symptom_keywords: errors, issues, problems mentioned
- query_patterns: special patterns like ["origin", "first_occurrence"] if asking about origin/first occurrence"""

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
                    "services": [],
                    "indexes": [],
                    "apps": [],
                    "entities": [],
                    "time_references": [],
                    "symptom_keywords": []
                }
            
            extracted_services = intent_data.get("services", []) or []
            extracted_indexes = intent_data.get("indexes", []) or []
            extracted_apps = intent_data.get("apps", []) or []
            extracted_entities = intent_data.get("entities", []) or []

            validated_services: List[str] = []
            validated_indexes: List[str] = []
            validated_apps: List[str] = []

            # Validate services
            for svc in extracted_services:
                matched_service = self.service_catalog.find_service(str(svc))
                if matched_service and matched_service.get("service_id"):
                    validated_services.append(matched_service.get("service_id"))
                else:
                    logger.warning("Service not found in service catalog, ignoring", service=svc)

            # Validate indexes, and map them to owning services (to aid dependency-aware hypothesis generation)
            for idx in extracted_indexes:
                idx_str = str(idx)
                owner = self.service_catalog.find_service_by_index(idx_str)
                if owner:
                    validated_indexes.append(idx_str)
                    if owner not in validated_services:
                        validated_services.append(owner)
                else:
                    logger.warning("Index not found in service catalog, ignoring", index=idx_str)

            # Validate apps, and map them to owning services (so app mentions can drive service selection)
            for app in extracted_apps:
                app_str = str(app)
                owner = self.service_catalog.find_service_by_app(app_str)
                if owner:
                    validated_apps.append(app_str)
                    if owner not in validated_services:
                        validated_services.append(owner)
                else:
                    logger.warning("App not found in service catalog, ignoring", app=app_str)

            # Deterministically extract known apps from the question (helps when LLM misses it)
            question_lower_scan = (question or "").lower()
            for app in self.service_catalog.get_all_apps():
                a = (app or "").strip()
                if not a:
                    continue
                # simple substring match; app names are short tokens in our catalog
                if a.lower() in question_lower_scan and a not in validated_apps:
                    validated_apps.append(a)
                    owner = self.service_catalog.find_service_by_app(a)
                    if owner and owner not in validated_services:
                        validated_services.append(owner)

            # Deterministically extract UUIDs from the question (helps transactionId origin queries)
            import re
            uuid_matches = re.findall(
                r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b",
                question
            )
            for u in uuid_matches:
                if u not in extracted_entities:
                    extracted_entities.append(u)
            
            # Extract query patterns (like "origin", "first occurrence", etc.)
            query_patterns = []
            question_lower = question.lower()
            if any(keyword in question_lower for keyword in ["origin", "first occurrence", "earliest", "where did it start", "where did it come from"]):
                query_patterns.append("origin")
                query_patterns.append("first_occurrence")
            
            intent = {
                "question": question,
                # Catalog-grounded selections:
                "services": validated_services,
                "indexes": validated_indexes,
                "apps": validated_apps,
                # Non-catalog entities/identifiers (transactionId, UUIDs, etc.)
                "entities": extracted_entities,
                "time_references": intent_data.get("time_references", []),
                "symptom_keywords": intent_data.get("symptom_keywords", []),
                "query_patterns": intent_data.get("query_patterns", query_patterns)  # Patterns like "origin", "first_occurrence"
            }
            
            logger.info(
                "Extracted intent",
                services_count=len(intent["services"]),
                indexes_count=len(intent["indexes"]),
                entities_count=len(intent["entities"]),
                validated_services=intent["services"]
            )
            return intent
            
        except Exception as e:
            logger.error("Failed to extract intent using Bedrock", error=str(e))
            # Fallback
            return {
                "question": question,
                "services": [],
                "indexes": [],
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
Generate investigation hypotheses that will help identify the root cause of issues.

Grounding rules:
- Service names MUST come from the provided service catalog context.
- App names MUST come from the provided service catalog context.
- Do NOT associate an app with a service unless that app is listed under that service in the catalog."""

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
            services = intent.get("services", [])
            entities = intent.get("entities", [])
            symptom_keywords = intent.get("symptom_keywords", [])
            apps = intent.get("apps", [])
            if services or entities or symptom_keywords:
                intent_section = "\n\nExtracted Information:\n"
                if services:
                    intent_section += f"Services: {', '.join(services)}\n"
                if apps:
                    intent_section += f"Apps: {', '.join(apps)}\n"
                if entities:
                    intent_section += f"Identifiers/Entities: {', '.join(entities)}\n"
                if symptom_keywords:
                    intent_section += f"Symptom Keywords: {', '.join(symptom_keywords)}\n"
                
                # Find services matching the explicit services list
                matched_services = self.service_catalog.find_services_by_entities(services)
                if matched_services:
                    service_context = "\n\nService Architecture Context:\n"
                    for service in matched_services:
                        service_id = service.get("service_id")
                        service_info = self.service_catalog.get_service_info(service_id)
                        service_context += f"- Service: {service_id} (Domain: {service_info.get('domain')}, Tier: {service_info.get('tier')}, Criticality: {service_info.get('criticality', 'not specified')})\n"
                        service_context += f"  Splunk Indexes: {', '.join(service_info.get('splunk_indexes', []))}\n"
                        svc_apps = self.service_catalog.get_apps(service_id)
                        if svc_apps:
                            service_context += f"  Apps: {', '.join(svc_apps)}\n"
                        
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
4. A suggested next step to further investigate this hypothesis

Focus on the entities and symptom keywords identified above when generating hypotheses.
Use the service architecture context to generate hypotheses that follow dependency chains.
Prioritize hypotheses for high-criticality services and check upstream dependencies when a service fails.
Respond in JSON format with a list of hypotheses, each with: hypothesis, priority, query_template, next_step."""

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
                    hypothesis_text = h.get("hypothesis", "Unknown hypothesis")
                    hypothesis_text = self._normalize_hypothesis_app_service_mismatch(hypothesis_text)
                    hypotheses.append(InvestigationHypothesis(
                        hypothesis=hypothesis_text,
                        priority=int(h.get("priority", 5)),
                        query_template=h.get("query_template"),
                        next_step=h.get("next_step")
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

    def _normalize_hypothesis_app_service_mismatch(self, hypothesis: str) -> str:
        """
        Fix a common hallucination: associating a catalog app with the wrong service.
        Example: "ottdata ... in the thingspace-core service" → owner is "provider".
        We only rewrite when the hypothesis explicitly says "in the <service> service".
        """
        text = (hypothesis or "").strip()
        if not text:
            return text

        # Find explicit "in the X service" mentions
        m = re.search(r"\bin\s+the\s+([A-Za-z0-9_.:-]+)\s+service\b", text, flags=re.IGNORECASE)
        if not m:
            return text
        mentioned_service = m.group(1)

        # If an app is mentioned, ensure it maps to the same service
        for app in self.service_catalog.get_all_apps():
            if not app:
                continue
            if app.lower() in text.lower():
                owner = self.service_catalog.find_service_by_app(app)
                if owner and owner.lower() != mentioned_service.lower():
                    logger.warning(
                        "Normalized hypothesis app/service mismatch",
                        app=app,
                        mentioned_service=mentioned_service,
                        owner_service=owner,
                        original=hypothesis,
                    )
                    # Replace only the service mention, keep the rest intact
                    return re.sub(
                        r"\bin\s+the\s+([A-Za-z0-9_.:-]+)\s+service\b",
                        f"in the {owner} service",
                        text,
                        count=1,
                        flags=re.IGNORECASE,
                    )

        return text
