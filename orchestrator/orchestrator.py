"""Main orchestration logic for bug investigation."""
from typing import Dict, Any, Optional, List
from datetime import datetime
import structlog

from orchestrator.planning import PlanningEngine
from orchestrator.models import InvestigationHypothesis, InvestigationStep
from memory.retrieval import MemoryRetrieval
from query_generator.generator import SplunkQueryGenerator
from analyzer.analyzer import ResultAnalyzer
from evidence.extractor import EvidenceExtractor
from answer_generator.generator import AnswerGenerator
from shared.utils import parse_time_window

logger = structlog.get_logger()

class InvestigationOrchestrator:
    """Main orchestration logic for bug investigation."""
    
    def __init__(self):
        self.planning_engine = PlanningEngine()
        self.memory_retrieval = MemoryRetrieval()
        self.query_generator = SplunkQueryGenerator()
        self.result_analyzer = ResultAnalyzer()
        self.evidence_extractor = EvidenceExtractor()
        self.answer_generator = AnswerGenerator()  # AnswerGenerator initializes Bedrock in __init__
    
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
            spl_query = await self.query_generator.generate_query(
                hypothesis=hypothesis.hypothesis,
                question=question,
                time_window=(start_time, end_time),
                historical_context=historical_context,
                intent=intent
            )
            
            logger.info(f"{idx} - Generated SPL query", spl_query=spl_query)
            # Execute query through Splunk API
            try:
                query_results = await self.query_generator.execute_query(spl_query)
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
        
        # Extract evidence and compute confidence
        # Convert Pydantic models to dicts for processing
        investigation_steps_dicts = [step.dict() for step in investigation_steps]
        evidence_result = await self.evidence_extractor.extract_and_score(
            investigation_steps=investigation_steps_dicts,
            question=question
        )
        
        # Generate final answer
        answer = await self.answer_generator.generate_answer(
            question=question,
            evidence=evidence_result["evidence"],
            investigation_steps=investigation_steps_dicts,
            confidence_score=evidence_result["confidence_score"]
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
            "evidence": evidence_result["evidence"],
            "investigation_steps": [step.dict() for step in investigation_steps],
            "root_causes": evidence_result.get("root_causes", [])
        }

