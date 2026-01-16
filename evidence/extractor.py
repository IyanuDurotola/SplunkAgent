"""Evidence extraction and scoring with enhanced confidence analysis."""
from typing import List, Dict, Any
from datetime import datetime
import structlog

from shared.service_catalog import ServiceCatalog
from evidence.confidence import ConfidenceScorer

logger = structlog.get_logger()


class EvidenceExtractor:
    """Evidence extraction with enhanced confidence scoring."""
    
    def __init__(self):
        """Initialize evidence extractor with service catalog and confidence scorer."""
        self.service_catalog = ServiceCatalog()
        self.confidence_scorer = ConfidenceScorer()
    
    async def extract_and_score(
        self,
        investigation_steps: List[Any],
        question: str,
        root_causes: List[Dict[str, Any]] = None,
        correlations: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Extract evidence and compute confidence scores with supporting evidence.
        
        Args:
            investigation_steps: List of investigation step results
            question: Original user question
            root_causes: Optional root causes from RCA engine
            correlations: Optional correlation data
            
        Returns:
            Dict containing:
            - evidence: List of evidence items
            - confidence_score: Overall confidence (0-1)
            - confidence_details: Detailed breakdown of confidence factors
            - supporting_evidence: Evidence items that support the confidence score
            - root_causes: Identified root causes
        """
        # Extract evidence from investigation steps
        evidence = self._extract_evidence(investigation_steps)
        
        # Identify root causes if not provided
        if root_causes is None:
            root_causes = self._identify_root_causes(evidence)
        
        # Calculate confidence with detailed supporting evidence
        confidence_result = self.confidence_scorer.calculate_confidence(
            evidence=evidence,
            investigation_steps=investigation_steps,
            root_causes=root_causes,
            correlations=correlations
        )
        
        logger.info(
            "Extracted evidence and computed confidence",
            evidence_count=len(evidence),
            confidence_score=confidence_result["score"],
            confidence_level=confidence_result["level"]
        )
        
        return {
            "evidence": evidence,
            "confidence_score": confidence_result["score"],
            "confidence_level": confidence_result["level"],
            "confidence_details": {
                "factors": confidence_result["factors"],
                "reasoning": confidence_result["reasoning"]
            },
            "supporting_evidence": confidence_result["supporting_evidence"],
            "root_causes": root_causes
        }
    
    def _extract_evidence(
        self, 
        investigation_steps: List[Any]
    ) -> List[Dict[str, Any]]:
        """Extract evidence items from investigation steps."""
        evidence = []
        
        for step in investigation_steps:
            step_num = step.get("step_number", 0)
            hypothesis = step.get("hypothesis", "Unknown")
            findings = step.get("findings", [])
            results = step.get("results", {})
            
            # Extract from findings
            for finding in findings:
                significance = finding.get("significance", "low")
                if significance in ["high", "medium"]:
                    evidence.append({
                        "source": f"Step {step_num}: {hypothesis}",
                        "content": f"{finding.get('field', 'unknown')}={finding.get('pattern', '')} (count: {finding.get('count', 0)})",
                        "relevance_score": 0.9 if significance == "high" else 0.7,
                        "significance": significance,
                        "matches_intent": finding.get("matches_intent", False),
                        "timestamp": step.get("timestamp"),
                        "step_number": step_num,
                        "finding_type": "pattern"
                    })
            
            # Extract key metrics from results
            if isinstance(results, dict):
                result_count = results.get("total_count", 0)
                if result_count > 0:
                    # Extract sample errors from results
                    result_list = results.get("results", [])
                    error_samples = self._extract_error_samples(result_list[:5])
                    
                    if error_samples:
                        for sample in error_samples:
                            evidence.append({
                                "source": f"Step {step_num}: {hypothesis}",
                                "content": sample.get("message", ""),
                                "relevance_score": 0.75,
                                "significance": "medium",
                                "timestamp": sample.get("timestamp"),
                                "step_number": step_num,
                                "finding_type": "error_sample",
                                "service": sample.get("service")
                            })
        
        # Sort by relevance
        evidence.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
        
        return evidence
    
    def _extract_error_samples(
        self, 
        results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Extract error samples from Splunk results."""
        samples = []
        
        for result in results:
            raw = result.get("_raw", "")
            message = result.get("message", raw)
            
            # Check if it's an error
            is_error = any(
                keyword in str(message).lower() 
                for keyword in ["error", "exception", "failed", "failure", "timeout"]
            )
            
            if is_error and message:
                samples.append({
                    "message": str(message)[:200],
                    # Prefer `time` if present; fallback to Splunk `_time` / generic `timestamp`.
                    "timestamp": result.get("time") or result.get("_time") or result.get("timestamp"),
                    "service": result.get("index", result.get("source", "unknown")),
                    "level": result.get("level", "error")
                })
        
        return samples
    
    def _identify_root_causes(
        self, 
        evidence: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Identify root causes from evidence, considering service dependencies."""
        # Group evidence by pattern/field
        cause_groups = {}
        service_mentions = set()
        
        for e in evidence:
            content = e.get("content", "")
            key = content.split("=")[0].strip() if "=" in content else content[:50]
            
            if key not in cause_groups:
                cause_groups[key] = {
                    "items": [],
                    "total_relevance": 0.0
                }
            
            cause_groups[key]["items"].append(e)
            cause_groups[key]["total_relevance"] += e.get("relevance_score", 0)
            
            # Track service mentions
            source = e.get("source", "").lower()
            service = e.get("service", "")
            for service_id in self.service_catalog.services.keys():
                if service_id.lower() in source or service_id.lower() in content.lower() or service_id == service:
                    service_mentions.add(service_id)
        
        # Build root causes from grouped evidence
        root_causes = []
        sorted_groups = sorted(
            cause_groups.items(), 
            key=lambda x: x[1]["total_relevance"], 
            reverse=True
        )
        
        for key, data in sorted_groups[:5]:
            avg_relevance = data["total_relevance"] / len(data["items"])
            
            # Build root cause entry
            root_cause = {
                "description": key,
                "confidence": round(min(avg_relevance, 1.0), 2),
                "type": "frequent_error",
                "evidence": {
                    "occurrence_count": len(data["items"]),
                    "avg_relevance": round(avg_relevance, 2),
                    "sample_sources": list(set(e.get("source", "") for e in data["items"][:3]))
                }
            }
            
            # Enhance with service dependency info
            for service_id in service_mentions:
                if service_id.lower() in key.lower():
                    root_cause["service"] = service_id
                    
                    # Check upstream dependencies
                    upstream = self.service_catalog.get_upstream_dependencies(service_id)
                    if upstream:
                        upstream_names = [
                            dep.get("service") if isinstance(dep, dict) else dep 
                            for dep in upstream
                        ]
                        root_cause["potential_upstream"] = upstream_names
                        root_cause["description"] += f" (may be caused by upstream: {', '.join(upstream_names)})"
                    break
            
            root_causes.append(root_cause)
        
        return root_causes
