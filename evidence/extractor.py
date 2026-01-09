"""Evidence extraction and scoring."""
from typing import List, Dict, Any
from datetime import datetime
import structlog

from shared.service_catalog import ServiceCatalog

logger = structlog.get_logger()

class EvidenceExtractor:
    """Evidence extraction and scoring."""
    
    def __init__(self):
        """Initialize evidence extractor with service catalog."""
        self.service_catalog = ServiceCatalog()
    
    async def extract_and_score(
        self,
        investigation_steps: List[Any],
        question: str
    ) -> Dict[str, Any]:
        """Extract evidence and compute confidence scores."""
        evidence = []
        
        for step in investigation_steps:
            findings = step.get("findings", [])
            for finding in findings:
                if finding.get("significance") in ["high", "medium"]:
                    evidence.append({
                        "source": f"Step {step.get('step_number')}: {step.get('hypothesis')}",
                        "content": f"{finding.get('field')}={finding.get('pattern')} (count: {finding.get('count')})",
                        "relevance_score": 0.8 if finding.get("significance") == "high" else 0.6,
                        "timestamp": step.get("timestamp")
                    })
        
        # Compute overall confidence
        confidence_score = self._compute_confidence(evidence, investigation_steps)
        
        # Identify root causes
        root_causes = self._identify_root_causes(evidence)
        
        return {
            "evidence": evidence,
            "confidence_score": confidence_score,
            "root_causes": root_causes
        }
    
    def _compute_confidence(
        self,
        evidence: List[Dict[str, Any]],
        investigation_steps: List[Any]
    ) -> float:
        """Compute overall confidence score."""
        if not evidence:
            return 0.0
        
        # Base confidence on number of evidence items and their scores
        avg_relevance = sum(e["relevance_score"] for e in evidence) / len(evidence)
        evidence_count_factor = min(len(evidence) / 5.0, 1.0)  # Cap at 5 evidence items
        
        confidence = (avg_relevance * 0.7) + (evidence_count_factor * 0.3)
        return round(confidence, 2)
    
    def _identify_root_causes(self, evidence: List[Dict[str, Any]]) -> List[str]:
        """Identify root causes from evidence, considering service dependencies."""
        # Group by content pattern
        cause_groups = {}
        service_mentions = set()
        
        for e in evidence:
            key = e["content"].split("=")[0] if "=" in e["content"] else e["content"]
            if key not in cause_groups:
                cause_groups[key] = []
            cause_groups[key].append(e)
            
            # Try to identify service names in evidence
            content_lower = e["content"].lower()
            for service_id in self.service_catalog.services.keys():
                if service_id.lower() in content_lower or content_lower in service_id.lower():
                    service_mentions.add(service_id)
        
        # Select top causes by relevance
        root_causes = []
        for key, items in sorted(cause_groups.items(), key=lambda x: sum(i["relevance_score"] for i in x[1]), reverse=True)[:3]:
            root_causes.append(key)
        
        # Enhance root causes with dependency chain information
        enhanced_root_causes = []
        for cause in root_causes:
            enhanced_cause = cause
            
            # Check if any mentioned services have upstream dependencies
            for service_id in service_mentions:
                if service_id.lower() in cause.lower() or cause.lower() in service_id.lower():
                    upstream = self.service_catalog.get_upstream_dependencies(service_id)
                    if upstream:
                        upstream_services = [dep.get("service") if isinstance(dep, dict) else dep for dep in upstream]
                        if upstream_services:
                            enhanced_cause += f" (may be caused by upstream: {', '.join(upstream_services)})"
                            break
            
            enhanced_root_causes.append(enhanced_cause)
        
        return enhanced_root_causes[:3]

