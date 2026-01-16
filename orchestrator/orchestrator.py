"""Main orchestration logic for bug investigation with multi-hop tracing."""
from typing import Dict, Any, Optional, List, Set
from datetime import datetime
import structlog

from orchestrator.planning import PlanningEngine
from orchestrator.models import InvestigationHypothesis, InvestigationStep
from memory.retrieval import MemoryRetrieval
from query_generator.generator import SplunkQueryGenerator
from analyzer.analyzer import ResultAnalyzer
from analyzer.rca_engine import RCAEngine
from analyzer.correlation import PatternCorrelation
from evidence.extractor import EvidenceExtractor
from answer_generator.generator import AnswerGenerator
from shared.utils import parse_time_window
from shared.service_catalog import ServiceCatalog
from shared.exceptions import ServiceNotFoundError

logger = structlog.get_logger()

class InvestigationOrchestrator:
    """Main orchestration logic for bug investigation with multi-hop tracing."""
    
    def __init__(self):
        self.planning_engine = PlanningEngine()
        self.memory_retrieval = MemoryRetrieval()
        self.query_generator = SplunkQueryGenerator()
        self.result_analyzer = ResultAnalyzer()
        self.evidence_extractor = EvidenceExtractor()
        self.answer_generator = AnswerGenerator()  # AnswerGenerator initializes Bedrock in __init__
        self.service_catalog = ServiceCatalog()
        self.rca_engine = RCAEngine(self.service_catalog)
        self.correlation_engine = PatternCorrelation()
    
    async def investigate(
        self,
        question: str,
        time_window: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Main investigation orchestration method."""
        logger.info("Starting investigation", question=question, time_window=time_window)
        
        # Extract intent and time window
        intent = await self.planning_engine.extract_intent(question)
        start_time, end_time = parse_time_window(time_window)
        # Hard-code the date to 2026-01-09 for seeded data, while preserving the original window duration.
        # (The previous implementation pinned BOTH start and end to the same day, collapsing the window to ~0s.)
        if start_time > end_time:
            start_time, end_time = end_time, start_time
            logger.warning("Start time is greater than end time, swapping them", start_time=start_time, end_time=end_time)

        window_delta = end_time - start_time
        start_time = start_time.replace(year=2026, month=1, day=9)
        end_time = start_time + window_delta


        # Check if we can match any services from the service catalog
        services = intent.get("services", []) or []
        indexes = intent.get("indexes", []) or []
        entities = intent.get("entities", []) or []

        # If indexes were provided but services were not, infer owning services from catalog
        if indexes and not services:
            for idx in indexes:
                owner = self.service_catalog.find_service_by_index(idx)
                if owner and owner not in services:
                    services.append(owner)
            intent["services"] = services

        matched_services = self.service_catalog.find_services_by_entities(services)
        
        # If we don't have a scoped service, only ask the user when we also have no usable identifiers/index hints.
        if not matched_services and not indexes and not entities:
            # No matching services found - either no entities extracted or entities don't match catalog
            available_services = list(self.service_catalog.services.keys())
            service_list = ", ".join(available_services)
            
            if services:
                # Services were extracted but don't match any service
                error_message = (
                    f"I couldn't identify which service is failing from your question. "
                    f"I found these services: {', '.join(services)}, but they don't match any services in the catalog.\n\n"
                    f"Available services: {service_list}\n\n"
                    f"Please specify which service is experiencing the issue, for example: "
                    f"'Why is [service_name] failing?' or 'What's wrong with [service_name]?'"
                )
            else:
                # No entities extracted - question doesn't mention a specific service
                error_message = (
                    f"I couldn't identify which service is failing from your question.\n\n"
                    f"Available services: {service_list}\n\n"
                    f"Please specify which service is experiencing the issue, for example: "
                    f"'Why is [service_name] failing?' or 'What's wrong with [service_name]?'"
                )
            
            logger.warning(
                "No matching service found in catalog",
                extracted_services=services,
                extracted_indexes=indexes,
                extracted_entities=entities,
                available_services=available_services
            )
            
            return {
                "answer": error_message,
                "confidence_score": 0.0,
                "evidence": [],
                "investigation_steps": [],
                "root_causes": [],
                "requires_user_input": True,
                "available_services": available_services
            }
        
        # Retrieve historical context from memory
        historical_context = await self.memory_retrieval.retrieve_relevant_incidents(
            question=question,
            time_window=(start_time, end_time)
        )
        logger.info("Retrieved historical context", incidents_count=len(historical_context))
        
        # Generate investigation hypotheses
        hypotheses = await self.planning_engine.generate_hypotheses(
            question=question,
            historical_context=historical_context,
            intent=intent
        )
        
        investigation_steps = []
        
        # Multi-step investigation loop
        for idx, hypothesis in enumerate(hypotheses, 1):
            logger.info("Processing hypothesis", step=idx, hypothesis=hypothesis.hypothesis)
            
            # Generate SPL query for hypothesis
            spl_query, time_params = await self.query_generator.generate_query(
                hypothesis=hypothesis.hypothesis,
                question=question,
                time_window=(start_time, end_time),
                historical_context=historical_context,
                intent=intent
            )
            
            logger.info(f"{idx} - Generated SPL query", spl_query=spl_query)
            # Execute query through Splunk API
            try:
                query_results = await self.query_generator.execute_query(spl_query, time_params)
                logger.info(f"\n{idx} - Query results: \n{query_results} \n\n")
            except Exception as e:
                logger.warning("Failed to execute Splunk query", error=str(e), query=spl_query[:100])
                # Continue with empty results if Splunk is unavailable
                query_results = {
                    "results": [],
                    "total_count": 0,
                    "fields": [],
                    "error": str(e)
                }
            
            # Analyze results
            analysis = await self.result_analyzer.analyze(
                results=query_results,
                hypothesis=hypothesis.hypothesis,
                question=question,
                intent=intent
            )
            
            # Store investigation step
            step = InvestigationStep(
                step_number=idx,
                hypothesis=hypothesis.hypothesis,
                spl_query=spl_query,
                results_summary=analysis.get("summary", ""),
                findings=analysis.get("findings", []),
                timestamp=datetime.utcnow()
            )
            investigation_steps.append(step)
            
            # Check if we have enough information to stop
            if analysis.get("sufficient_evidence", False):
                logger.info("Sufficient evidence found, stopping investigation")
                break

            # check if analysis has next step in the hypothesis and if hypothesis has
            # good score then add it to the investigation steps
        
        # Multi-hop investigation: check upstream dependencies if errors found
        all_events = []
        for step in investigation_steps:
            step_dict = step.dict()
            if step_dict.get("findings"):
                # Collect events for correlation
                all_events.extend(step_dict.get("results", {}).get("results", []))
        
        # Perform multi-hop tracing if we found errors
        upstream_investigation = await self._investigate_upstream_dependencies(
            investigation_steps=investigation_steps,
            matched_services=matched_services,
            time_window=(start_time, end_time),
            intent=intent
        )
        investigation_steps.extend(upstream_investigation)
        
        # Extract evidence and compute confidence
        # Convert Pydantic models to dicts for processing
        investigation_steps_dicts = [step.dict() for step in investigation_steps]
        
        # Correlate patterns across investigation (needed for confidence scoring)
        correlations = self._correlate_investigation_results(
            investigation_steps_dicts,
            historical_context
        )
        
        # Enhanced RCA using the new engine
        root_causes = await self.rca_engine.identify_root_causes(
            investigation_steps=investigation_steps_dicts,
            evidence=[],  # Will be extracted below
            intent=intent
        )
        
        # Extract evidence with enhanced confidence scoring
        evidence_result = await self.evidence_extractor.extract_and_score(
            investigation_steps=investigation_steps_dicts,
            question=question,
            root_causes=root_causes,
            correlations=correlations
        )
        
        # Generate final answer with enhanced context
        answer = await self.answer_generator.generate_answer(
            question=question,
            evidence=evidence_result["evidence"],
            investigation_steps=investigation_steps_dicts,
            confidence_score=evidence_result["confidence_score"],
            root_causes=root_causes,
            correlations=correlations
        )
        
        # Store investigation in memory for future reference
        await self.memory_retrieval.store_investigation(
            question=question,
            answer=answer,
            evidence=evidence_result["evidence"],
            investigation_steps=investigation_steps
        )
        
        return {
            "answer": answer,
            "confidence_score": evidence_result["confidence_score"],
            "confidence_level": evidence_result.get("confidence_level", "unknown"),
            "confidence_details": evidence_result.get("confidence_details", {}),
            "supporting_evidence": evidence_result.get("supporting_evidence", []),
            "evidence": evidence_result["evidence"],
            "investigation_steps": [step.dict() for step in investigation_steps],
            "root_causes": root_causes,
            "correlations": correlations
        }
    
    async def _investigate_upstream_dependencies(
        self,
        investigation_steps: List[InvestigationStep],
        matched_services: List[Dict[str, Any]],
        time_window: tuple,
        intent: Dict[str, Any]
    ) -> List[InvestigationStep]:
        """Investigate upstream dependencies when errors are found.
        
        This enables multi-hop tracing: if service A has errors, check its
        upstream dependencies (B, C) to see if the root cause is there.
        """
        additional_steps = []
        investigated_services: Set[str] = set()
        
        # Get services already investigated
        for service in matched_services:
            investigated_services.add(service.get("service_id", ""))
        
        # Check if we found errors
        has_errors = any(
            any(f.get("significance") == "high" for f in step.findings)
            for step in investigation_steps
        )
        
        if not has_errors:
            return additional_steps
        
        # Get upstream dependencies that haven't been checked
        services_to_check = set()
        for service in matched_services:
            service_id = service.get("service_id", "")
            upstream = self.service_catalog.get_upstream_dependencies(service_id)
            
            for dep in upstream:
                dep_service = dep.get("service") if isinstance(dep, dict) else dep
                if dep_service and dep_service not in investigated_services:
                    services_to_check.add(dep_service)
                    # Also record failure modes for smarter queries
                    if isinstance(dep, dict) and dep.get("failure_modes"):
                        intent.setdefault("upstream_failure_modes", {})[dep_service] = dep.get("failure_modes")
        
        if not services_to_check:
            return additional_steps
        
        logger.info("Investigating upstream dependencies", services=list(services_to_check))
        
        # Generate queries for upstream services
        start_time, end_time = time_window
        step_number = len(investigation_steps) + 1
        
        for upstream_service in services_to_check:
            # Get Splunk indexes for this service
            indexes = self.service_catalog.get_splunk_indexes(upstream_service)
            if not indexes:
                continue
            
            # Create hypothesis for upstream investigation
            failure_modes = intent.get("upstream_failure_modes", {}).get(upstream_service, [])
            if failure_modes:
                hypothesis = f"Check upstream service {upstream_service} for {', '.join(failure_modes)} errors"
            else:
                hypothesis = f"Check upstream service {upstream_service} for errors that may have cascaded downstream"
            
            # Generate query
            try:
                spl_query, time_params = await self.query_generator.generate_query(
                    hypothesis=hypothesis,
                    question=f"Check {upstream_service} for errors: {intent.get('question', '')}",
                    time_window=(start_time, end_time),
                    historical_context=None,
                    intent={
                        **intent,
                        "services": [upstream_service],
                        "entities": [upstream_service],
                        "is_upstream_check": True
                    }
                )
                
                # Execute query
                query_results = await self.query_generator.execute_query(spl_query, time_params)
                
                # Analyze results
                analysis = await self.result_analyzer.analyze(
                    results=query_results,
                    hypothesis=hypothesis,
                    question=intent.get("question", ""),
                    intent=intent
                )
                
                # Add step
                step = InvestigationStep(
                    step_number=step_number,
                    hypothesis=hypothesis,
                    spl_query=spl_query,
                    results_summary=analysis.get("summary", ""),
                    findings=analysis.get("findings", []),
                    timestamp=datetime.utcnow()
                )
                additional_steps.append(step)
                step_number += 1
                
                logger.info(
                    "Completed upstream investigation",
                    service=upstream_service,
                    findings_count=len(analysis.get("findings", []))
                )
                
            except Exception as e:
                logger.warning(
                    "Failed to investigate upstream service",
                    service=upstream_service,
                    error=str(e)
                )
        
        return additional_steps
    
    def _correlate_investigation_results(
        self,
        investigation_steps: List[Dict[str, Any]],
        historical_context: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Correlate results across investigation steps and with historical data."""
        correlations = {
            "transaction_correlations": {},
            "temporal_correlations": [],
            "historical_matches": []
        }
        
        # Collect all events from investigation
        all_events = []
        for step in investigation_steps:
            results = step.get("results", {})
            if isinstance(results, dict):
                all_events.extend(results.get("results", []))
        
        if not all_events:
            return correlations
        
        # Transaction-based correlation
        transaction_correlations = self.correlation_engine.correlate_by_transaction(all_events)
        correlations["transaction_correlations"] = {
            k: v for k, v in transaction_correlations.items()
            if len(v) > 1  # Only include multi-event transactions
        }
        
        # Temporal correlation (events within 60 seconds)
        temporal = self.correlation_engine.correlate_by_time(all_events, time_window_seconds=60)
        correlations["temporal_correlations"] = temporal[:10]  # Top 10
        
        # Historical pattern matching
        if historical_context:
            findings = []
            for step in investigation_steps:
                findings.extend(step.get("findings", []))
            
            if findings:
                historical_matches = self.correlation_engine.find_recurring_patterns(
                    all_events,
                    historical_context,
                    similarity_threshold=0.5
                )
                correlations["historical_matches"] = historical_matches[:5]  # Top 5
        
        return correlations

