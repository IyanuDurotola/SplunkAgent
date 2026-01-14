"""Enhanced confidence scoring with supporting evidence."""
from typing import List, Dict, Any, Tuple
from datetime import datetime
import structlog

logger = structlog.get_logger()


class ConfidenceScorer:
    """Enhanced confidence scoring engine that provides supporting evidence for scores."""
    
    # Confidence factors and their weights
    WEIGHTS = {
        "evidence_quality": 0.25,      # How relevant/strong is the evidence
        "evidence_quantity": 0.15,     # How much evidence do we have
        "pattern_consistency": 0.20,   # Do patterns consistently point to same root cause
        "service_correlation": 0.15,   # Does evidence correlate with service dependencies
        "temporal_correlation": 0.10,  # Do events align temporally
        "historical_match": 0.15       # Does this match historical incidents
    }
    
    # Thresholds for confidence levels
    CONFIDENCE_LEVELS = {
        "very_high": 0.85,
        "high": 0.70,
        "medium": 0.50,
        "low": 0.30,
        "very_low": 0.0
    }
    
    def calculate_confidence(
        self,
        evidence: List[Dict[str, Any]],
        investigation_steps: List[Dict[str, Any]],
        root_causes: List[Dict[str, Any]] = None,
        correlations: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Calculate overall confidence score with detailed supporting evidence.
        
        Returns:
            Dict containing:
            - score: float (0-1)
            - level: str (very_high, high, medium, low, very_low)
            - factors: Dict of individual factor scores
            - supporting_evidence: List of evidence items that support the score
            - reasoning: str explaining the confidence assessment
        """
        factors = {}
        supporting_evidence = []
        reasoning_parts = []
        
        # Factor 1: Evidence Quality
        quality_score, quality_evidence = self._assess_evidence_quality(evidence)
        factors["evidence_quality"] = {
            "score": quality_score,
            "weight": self.WEIGHTS["evidence_quality"],
            "weighted_score": quality_score * self.WEIGHTS["evidence_quality"],
            "details": quality_evidence
        }
        supporting_evidence.extend(quality_evidence)
        
        # Factor 2: Evidence Quantity
        quantity_score, quantity_evidence = self._assess_evidence_quantity(evidence)
        factors["evidence_quantity"] = {
            "score": quantity_score,
            "weight": self.WEIGHTS["evidence_quantity"],
            "weighted_score": quantity_score * self.WEIGHTS["evidence_quantity"],
            "details": quantity_evidence
        }
        supporting_evidence.extend(quantity_evidence)
        
        # Factor 3: Pattern Consistency
        consistency_score, consistency_evidence = self._assess_pattern_consistency(
            evidence, investigation_steps
        )
        factors["pattern_consistency"] = {
            "score": consistency_score,
            "weight": self.WEIGHTS["pattern_consistency"],
            "weighted_score": consistency_score * self.WEIGHTS["pattern_consistency"],
            "details": consistency_evidence
        }
        supporting_evidence.extend(consistency_evidence)
        
        # Factor 4: Service Correlation
        service_score, service_evidence = self._assess_service_correlation(
            evidence, root_causes
        )
        factors["service_correlation"] = {
            "score": service_score,
            "weight": self.WEIGHTS["service_correlation"],
            "weighted_score": service_score * self.WEIGHTS["service_correlation"],
            "details": service_evidence
        }
        supporting_evidence.extend(service_evidence)
        
        # Factor 5: Temporal Correlation
        temporal_score, temporal_evidence = self._assess_temporal_correlation(
            correlations
        )
        factors["temporal_correlation"] = {
            "score": temporal_score,
            "weight": self.WEIGHTS["temporal_correlation"],
            "weighted_score": temporal_score * self.WEIGHTS["temporal_correlation"],
            "details": temporal_evidence
        }
        supporting_evidence.extend(temporal_evidence)
        
        # Factor 6: Historical Match
        historical_score, historical_evidence = self._assess_historical_match(
            correlations
        )
        factors["historical_match"] = {
            "score": historical_score,
            "weight": self.WEIGHTS["historical_match"],
            "weighted_score": historical_score * self.WEIGHTS["historical_match"],
            "details": historical_evidence
        }
        supporting_evidence.extend(historical_evidence)
        
        # Calculate final score
        final_score = sum(f["weighted_score"] for f in factors.values())
        final_score = round(min(max(final_score, 0.0), 1.0), 2)
        
        # Determine confidence level
        level = self._get_confidence_level(final_score)
        
        # Generate reasoning
        reasoning = self._generate_reasoning(factors, final_score, level)
        
        logger.info(
            "Calculated confidence score",
            score=final_score,
            level=level,
            factor_count=len(factors)
        )
        
        return {
            "score": final_score,
            "level": level,
            "factors": factors,
            "supporting_evidence": self._dedupe_evidence(supporting_evidence),
            "reasoning": reasoning
        }
    
    def _assess_evidence_quality(
        self, 
        evidence: List[Dict[str, Any]]
    ) -> Tuple[float, List[Dict[str, Any]]]:
        """Assess quality of evidence based on relevance scores (simplified)."""
        if not evidence:
            return 0.0, [{"type": "quality", "finding": "No evidence found", "impact": "negative"}]
        
        total_relevance = sum(e.get("relevance_score", 0.5) for e in evidence)
        avg_relevance = total_relevance / len(evidence)
        high_quality_count = sum(1 for e in evidence if e.get("relevance_score", 0) >= 0.7)
        high_quality_ratio = high_quality_count / len(evidence)
        
        # Score based on average relevance and proportion of high-quality evidence
        score = (avg_relevance * 0.6) + (high_quality_ratio * 0.4)
        
        supporting = [{
            "type": "quality",
            "finding": f"{high_quality_count} high-relevance items (avg: {avg_relevance:.2f})",
            "impact": "positive" if avg_relevance >= 0.7 else "neutral" if avg_relevance >= 0.5 else "negative"
        }]
        
        return score, supporting
    
    def _assess_evidence_quantity(
        self, 
        evidence: List[Dict[str, Any]]
    ) -> Tuple[float, List[Dict[str, Any]]]:
        """Assess quantity of evidence - more evidence increases confidence."""
        count = len(evidence)
        
        # Score: 0-2 items = low, 3-5 = medium, 6-10 = high, 10+ = very high
        if count == 0:
            score = 0.0
            finding = "No evidence items found"
            impact = "negative"
        elif count <= 2:
            score = 0.3
            finding = f"Limited evidence ({count} items)"
            impact = "negative"
        elif count <= 5:
            score = 0.6
            finding = f"Moderate evidence ({count} items)"
            impact = "neutral"
        elif count <= 10:
            score = 0.85
            finding = f"Good evidence coverage ({count} items)"
            impact = "positive"
        else:
            score = 1.0
            finding = f"Extensive evidence ({count} items)"
            impact = "positive"
        
        return score, [{
            "type": "quantity",
            "finding": finding,
            "count": count,
            "impact": impact
        }]
    
    def _assess_pattern_consistency(
        self,
        evidence: List[Dict[str, Any]],
        investigation_steps: List[Dict[str, Any]]
    ) -> Tuple[float, List[Dict[str, Any]]]:
        """Assess if patterns consistently point to the same root cause (simplified)."""
        if not evidence or not investigation_steps:
            return 0.0, [{"type": "consistency", "finding": "Insufficient data", "impact": "negative"}]
        
        # Extract services from evidence
        services = [e.get("service") for e in evidence if e.get("service")]
        if not services:
            return 0.3, [{"type": "consistency", "finding": "No service patterns", "impact": "neutral"}]
        
        # Check if evidence points to same service
        service_counts = {}
        for svc in services:
            service_counts[svc] = service_counts.get(svc, 0) + 1
        
        dominant_service = max(service_counts.items(), key=lambda x: x[1]) if service_counts else None
        if dominant_service:
            dominance_ratio = dominant_service[1] / len(services)
            
            if dominance_ratio >= 0.6:
                score = 0.9
                finding = f"Consistent pattern: {dominant_service[0]} ({dominance_ratio:.0%})"
                impact = "positive"
            elif dominance_ratio >= 0.4:
                score = 0.6
                finding = f"Moderate consistency: {dominant_service[0]} ({dominance_ratio:.0%})"
                impact = "neutral"
            else:
                score = 0.3
                finding = "Scattered patterns across services"
                impact = "negative"
        else:
            score = 0.3
            finding = "No clear service pattern"
            impact = "neutral"
        
        # Boost if multiple steps found findings
        steps_with_findings = sum(1 for s in investigation_steps if s.get("findings"))
        if steps_with_findings >= 2:
            score = min(score + 0.1, 1.0)
        
        return score, [{"type": "consistency", "finding": finding, "impact": impact}]
    
    def _assess_service_correlation(
        self,
        evidence: List[Dict[str, Any]],
        root_causes: List[Dict[str, Any]] = None
    ) -> Tuple[float, List[Dict[str, Any]]]:
        """Assess if evidence correlates with service dependency patterns."""
        supporting = []
        
        if not root_causes:
            return 0.5, [{"type": "service", "finding": "No root cause analysis available", "impact": "neutral"}]
        
        # Check for cascade patterns (strongest indicator)
        cascade_causes = [rc for rc in root_causes if rc.get("type") == "cascade_origin"]
        if cascade_causes:
            score = 0.95
            for rc in cascade_causes:
                chain = rc.get("evidence", {}).get("cascade_chain", [])
                if chain:
                    chain_str = " → ".join(f"{c['from']}→{c['to']}" for c in chain)
                    supporting.append({
                        "type": "service",
                        "finding": f"Cascade failure detected: {chain_str}",
                        "service": rc.get("service"),
                        "confidence": rc.get("confidence", 0),
                        "impact": "positive"
                    })
            return score, supporting
        
        # Check for upstream failure patterns
        upstream_causes = [rc for rc in root_causes if rc.get("type") == "upstream_failure"]
        if upstream_causes:
            score = 0.85
            for rc in upstream_causes:
                supporting.append({
                    "type": "service",
                    "finding": f"Upstream failure: {rc.get('description', 'Unknown')}",
                    "service": rc.get("service"),
                    "confidence": rc.get("confidence", 0),
                    "impact": "positive"
                })
            return score, supporting
        
        # Check for any identified root causes
        if root_causes:
            avg_confidence = sum(rc.get("confidence", 0) for rc in root_causes) / len(root_causes)
            score = avg_confidence * 0.8
            supporting.append({
                "type": "service",
                "finding": f"Root causes identified with avg confidence {avg_confidence:.0%}",
                "root_cause_count": len(root_causes),
                "impact": "positive" if avg_confidence >= 0.6 else "neutral"
            })
            return score, supporting
        
        return 0.4, [{"type": "service", "finding": "No service correlation patterns found", "impact": "neutral"}]
    
    def _assess_temporal_correlation(
        self,
        correlations: Dict[str, Any] = None
    ) -> Tuple[float, List[Dict[str, Any]]]:
        """Assess temporal correlation of events."""
        if not correlations:
            return 0.5, [{"type": "temporal", "finding": "No correlation data available", "impact": "neutral"}]
        
        supporting = []
        score = 0.5
        
        # Check transaction correlations
        tx_correlations = correlations.get("transaction_correlations", {})
        if tx_correlations:
            multi_service_txs = sum(1 for tx_events in tx_correlations.values() if len(set(e.get("service") for e in tx_events)) > 1)
            
            if multi_service_txs > 0:
                score += 0.3
                supporting.append({
                    "type": "temporal",
                    "finding": f"Found {multi_service_txs} transactions spanning multiple services",
                    "transaction_count": len(tx_correlations),
                    "multi_service_count": multi_service_txs,
                    "impact": "positive"
                })
        
        # Check temporal correlations
        temporal = correlations.get("temporal_correlations", [])
        if temporal:
            score += 0.2
            supporting.append({
                "type": "temporal",
                "finding": f"Found {len(temporal)} temporally correlated event clusters",
                "cluster_count": len(temporal),
                "impact": "positive"
            })
        
        if not supporting:
            supporting.append({
                "type": "temporal",
                "finding": "Limited temporal correlation data",
                "impact": "neutral"
            })
        
        return min(score, 1.0), supporting
    
    def _assess_historical_match(
        self,
        correlations: Dict[str, Any] = None
    ) -> Tuple[float, List[Dict[str, Any]]]:
        """Assess if current issue matches historical incidents."""
        if not correlations:
            return 0.5, [{"type": "historical", "finding": "No historical comparison available", "impact": "neutral"}]
        
        supporting = []
        historical_matches = correlations.get("historical_matches", [])
        
        if not historical_matches:
            return 0.5, [{"type": "historical", "finding": "No matching historical incidents found", "impact": "neutral"}]
        
        # Check best match
        best_match = max(historical_matches, key=lambda x: x.get("similarity", 0))
        similarity = best_match.get("similarity", 0)
        resolution = best_match.get("historical_resolution", "")
        
        if similarity >= 0.8:
            score = 0.95
            impact = "positive"
            finding = f"Strong match ({similarity:.0%}) with historical incident"
        elif similarity >= 0.6:
            score = 0.75
            impact = "positive"
            finding = f"Moderate match ({similarity:.0%}) with historical incident"
        elif similarity >= 0.4:
            score = 0.55
            impact = "neutral"
            finding = f"Weak match ({similarity:.0%}) with historical incident"
        else:
            score = 0.4
            impact = "neutral"
            finding = f"No significant historical matches"
        
        supporting.append({
            "type": "historical",
            "finding": finding,
            "similarity": similarity,
            "has_resolution": bool(resolution),
            "resolution_preview": resolution[:100] + "..." if len(resolution) > 100 else resolution,
            "impact": impact
        })
        
        # Bonus for having resolution
        if resolution and similarity >= 0.6:
            score = min(score + 0.05, 1.0)
            supporting.append({
                "type": "historical",
                "finding": "Historical resolution available - can apply known fix",
                "impact": "positive"
            })
        
        return score, supporting
    
    def _get_confidence_level(self, score: float) -> str:
        """Convert numerical score to confidence level."""
        for level, threshold in sorted(self.CONFIDENCE_LEVELS.items(), key=lambda x: x[1], reverse=True):
            if score >= threshold:
                return level
        return "very_low"
    
    def _generate_reasoning(
        self, 
        factors: Dict[str, Any], 
        score: float, 
        level: str
    ) -> str:
        """Generate human-readable reasoning for the confidence score."""
        reasoning_parts = []
        
        # Overall assessment
        if level in ["very_high", "high"]:
            reasoning_parts.append(f"The confidence score of {score:.0%} ({level.replace('_', ' ')}) indicates strong evidence supporting the root cause analysis.")
        elif level == "medium":
            reasoning_parts.append(f"The confidence score of {score:.0%} ({level}) suggests moderate certainty in the findings.")
        else:
            reasoning_parts.append(f"The confidence score of {score:.0%} ({level.replace('_', ' ')}) indicates limited certainty - additional investigation may be needed.")
        
        # Highlight strongest factors
        sorted_factors = sorted(factors.items(), key=lambda x: x[1]["weighted_score"], reverse=True)
        top_factors = sorted_factors[:2]
        
        strengths = []
        for name, data in top_factors:
            if data["score"] >= 0.7:
                readable_name = name.replace("_", " ")
                strengths.append(f"{readable_name} ({data['score']:.0%})")
        
        if strengths:
            reasoning_parts.append(f"Key strengths: {', '.join(strengths)}.")
        
        # Highlight weakest factors
        weak_factors = [
            name.replace("_", " ") 
            for name, data in sorted_factors[-2:] 
            if data["score"] < 0.5
        ]
        
        if weak_factors:
            reasoning_parts.append(f"Areas needing more evidence: {', '.join(weak_factors)}.")
        
        return " ".join(reasoning_parts)
    
    def _dedupe_evidence(
        self, 
        evidence: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Remove duplicate evidence items."""
        seen = set()
        deduped = []
        
        for e in evidence:
            key = (e.get("type"), e.get("finding", "")[:50])
            if key not in seen:
                seen.add(key)
                deduped.append(e)
        
        return deduped
