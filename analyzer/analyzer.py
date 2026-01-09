"""Analyzer for Splunk query results."""
from typing import Dict, Any, List, Optional
import structlog

logger = structlog.get_logger()

class ResultAnalyzer:
    """Analyzer for Splunk query results."""
    
    async def analyze(
        self,
        results: Dict[str, Any],
        hypothesis: str,
        question: str,
        intent: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Analyze query results and extract findings."""
        findings = []
        
        # Analyze result patterns
        result_count = results.get("total_count", 0)
        result_list = results.get("results", [])
        
        if result_count > 0:
            # Extract key patterns from results, prioritizing entities from intent
            patterns = self._extract_patterns(result_list, intent)
            findings.extend(patterns)
        
        summary = self._generate_summary(result_count, findings, hypothesis, intent)
        
        # Determine if we have sufficient evidence
        sufficient_evidence = result_count > 0 and len(findings) >= 2
        
        return {
            "summary": summary,
            "findings": findings,
            "result_count": result_count,
            "sufficient_evidence": sufficient_evidence
        }
    
    def _extract_patterns(self, results: List[Dict[str, Any]], intent: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Extract patterns from query results, prioritizing entities from intent."""
        patterns = []
        
        # Get entities and keywords from intent for prioritization
        entities = []
        symptom_keywords = []
        if intent:
            entities = [e.lower() for e in intent.get("entities", [])]
            symptom_keywords = [k.lower() for k in intent.get("symptom_keywords", [])]
        
        # Group by common fields
        field_counts = {}
        for result in results:
            for key, value in result.items():
                if key not in ['_time', '_raw']:
                    if key not in field_counts:
                        field_counts[key] = {}
                    field_counts[key][str(value)] = field_counts[key].get(str(value), 0) + 1
        
        # Identify top patterns, prioritizing fields/values that match entities or keywords
        for field, counts in field_counts.items():
            if len(counts) <= 5:  # Low cardinality fields
                top_value = max(counts.items(), key=lambda x: x[1])
                value_str = str(top_value[0]).lower()
                
                # Check if this pattern matches any entity or keyword
                matches_entity = any(entity in value_str or value_str in entity for entity in entities)
                matches_keyword = any(keyword in value_str or value_str in keyword for keyword in symptom_keywords)
                
                significance = "high"
                if matches_entity or matches_keyword:
                    significance = "high"
                elif top_value[1] > len(results) * 0.5:
                    significance = "high"
                else:
                    significance = "medium"
                
                patterns.append({
                    "field": field,
                    "pattern": top_value[0],
                    "count": top_value[1],
                    "significance": significance,
                    "matches_intent": matches_entity or matches_keyword
                })
        
        # Sort by significance and intent match
        patterns.sort(key=lambda x: (x.get("matches_intent", False), x.get("significance") == "high"), reverse=True)
        
        return patterns
    
    def _generate_summary(self, count: int, findings: List[Dict[str, Any]], hypothesis: str, intent: Optional[Dict[str, Any]] = None) -> str:
        """Generate summary of analysis."""
        if count == 0:
            return f"No results found for hypothesis: {hypothesis}"
        
        summary = f"Found {count} results. "
        if findings:
            top_finding = findings[0]
            intent_note = ""
            if top_finding.get("matches_intent", False):
                intent_note = " (matches extracted entities/keywords)"
            summary += f"Key pattern: {top_finding.get('field')}={top_finding.get('pattern')} (count: {top_finding.get('count')}){intent_note}"
        else:
            summary += "No clear patterns identified."
        
        return summary

