"""Confidence scoring utilities."""
from typing import List, Dict, Any
import structlog

logger = structlog.get_logger()

class ConfidenceScorer:
    """Confidence scoring engine."""
    
    def calculate_confidence(
        self,
        evidence_quality: float,
        evidence_quantity: int,
        result_consistency: float
    ) -> float:
        """Calculate overall confidence score."""
        # Weighted combination of factors
        quality_weight = 0.5
        quantity_weight = 0.3
        consistency_weight = 0.2
        
        quantity_score = min(evidence_quantity / 5.0, 1.0)  # Normalize to 0-1
        
        confidence = (
            evidence_quality * quality_weight +
            quantity_score * quantity_weight +
            result_consistency * consistency_weight
        )
        
        return round(confidence, 2)
    
    def assess_evidence_quality(self, evidence: List[Dict[str, Any]]) -> float:
        """Assess the quality of evidence."""
        if not evidence:
            return 0.0
        
        avg_relevance = sum(e.get("relevance_score", 0) for e in evidence) / len(evidence)
        return avg_relevance

