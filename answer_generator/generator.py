"""Final answer generator using Amazon Bedrock."""
from typing import List, Dict, Any
import structlog
import os
import json

from answer_generator.config import AnswerGeneratorConfig
from shared.bedrock_client import BedrockClient

logger = structlog.get_logger()

class AnswerGenerator:
    """Final answer generator using Amazon Bedrock."""
    
    def __init__(self):
        self.config = AnswerGeneratorConfig()
        self.bedrock_client = BedrockClient(
            region_name=self.config.aws_region or os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            model_id=self.config.llm_model
        )
        logger.info("Initialized Bedrock answer generator", model=self.config.llm_model)
    
    async def generate_answer(
        self,
        question: str,
        evidence: List[Dict[str, Any]],
        investigation_steps: List[Any],
        confidence_score: float,
        root_causes: List[Dict[str, Any]] = None,
        correlations: Dict[str, Any] = None
    ) -> str:
        """Generate final grounded explanation using Amazon Bedrock."""
        
        # Filter out non-meaningful evidence (generic metadata)
        meaningful_evidence = self._filter_meaningful_evidence(evidence)
        
        system_prompt = """You are a senior SRE providing root cause analysis. Be DIRECT and CONCISE.

STRICT RULES:
- Maximum 150 words for the main explanation
- NO filler phrases ("suggests that", "appears to be", "it seems")
- NO repeating the same information
- NO generic troubleshooting advice unless specifically relevant
- DO NOT interpret generic metadata (preview=False, init_offset=0, results=[]) as meaningful findings
- If evidence is weak or generic, SAY SO clearly
- Only mention services/errors that are ACTUALLY in the evidence
- Use bullet points, not paragraphs

FORMAT:
**Root Cause**: [One sentence - what failed and why]

**Evidence**:
- [Specific finding 1]
- [Specific finding 2]

**Confidence**: [X%] - [One sentence explaining why]

**Next Step**: [One specific action to take]"""

        # Build concise evidence summary - only meaningful findings
        evidence_bullets = []
        for e in meaningful_evidence[:5]:
            content = e.get('content', '')
            if content and not self._is_generic_finding(content):
                evidence_bullets.append(f"- {content}")
        
        evidence_text = "\n".join(evidence_bullets) if evidence_bullets else "- No specific error patterns found in logs"
        
        # Build root cause summary
        root_cause_text = ""
        if root_causes:
            meaningful_causes = [rc for rc in root_causes if rc.get("confidence", 0) > 0.3]
            if meaningful_causes:
                root_cause_text = "\n\nIdentified causes:\n"
                for rc in meaningful_causes[:3]:
                    desc = rc.get("description", "Unknown")
                    conf = rc.get("confidence", 0)
                    svc = rc.get("service", "")
                    root_cause_text += f"- {desc}"
                    if svc:
                        root_cause_text += f" (service: {svc})"
                    root_cause_text += f" [{conf:.0%} confidence]\n"
        
        # Historical context if available
        historical_text = ""
        if correlations and correlations.get("historical_matches"):
            matches = correlations["historical_matches"]
            if matches and matches[0].get("similarity", 0) > 0.5:
                best = matches[0]
                resolution = best.get("historical_resolution", "")
                if resolution:
                    historical_text = f"\n\nSimilar past incident ({best['similarity']:.0%} match): {resolution[:100]}"
        
        user_prompt = f"""Question: {question}

Evidence found:
{evidence_text}
{root_cause_text}
{historical_text}

Confidence: {confidence_score:.0%}

Provide a CONCISE root cause analysis. If the evidence is weak or generic, say "insufficient evidence" rather than over-interpreting."""

        try:
            answer = await self.bedrock_client.invoke(
                prompt=user_prompt,
                system_prompt=system_prompt,
                temperature=0.3,  # Lower temperature for more focused output
                max_tokens=500    # Limit output length
            )
            
            # Post-process to remove any remaining verbosity
            answer = self._clean_answer(answer)
            
            logger.info("Generated final answer using Bedrock", answer_length=len(answer))
            return answer
            
        except Exception as e:
            logger.error("Failed to generate answer using Bedrock", error=str(e))
            return self._generate_fallback_answer(question, meaningful_evidence, investigation_steps, confidence_score, root_causes)
    
    def _filter_meaningful_evidence(self, evidence: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter out generic/non-meaningful evidence."""
        meaningful = []
        
        # Generic patterns to skip
        generic_patterns = [
            "preview=", "init_offset=", "post_process_count=", 
            "messages=[]", "results=[]", "fields=[]",
            "is_preview=", "is_final=", "offset=0"
        ]
        
        for e in evidence:
            content = str(e.get("content", "")).lower()
            
            # Skip if it matches generic patterns
            if any(pattern.lower() in content for pattern in generic_patterns):
                continue
            
            # Skip if it's just empty arrays/objects
            if content in ["[]", "{}", "none", "null", ""]:
                continue
                
            meaningful.append(e)
        
        return meaningful
    
    def _is_generic_finding(self, content: str) -> bool:
        """Check if a finding is generic/non-meaningful."""
        content_lower = content.lower()
        
        generic_indicators = [
            "preview=false", "preview=true",
            "init_offset=0", "offset=0",
            "post_process_count=0",
            "messages=[]", "results=[]",
            "fields=[]", "count: 0",
            "is_preview=", "is_final="
        ]
        
        return any(indicator in content_lower for indicator in generic_indicators)
    
    def _clean_answer(self, answer: str) -> str:
        """Remove verbose filler phrases from the answer."""
        # Phrases to remove
        filler_phrases = [
            "Based on the investigation, ",
            "Based on the evidence provided, ",
            "The investigation reveals that ",
            "It appears that ",
            "It seems that ",
            "This suggests that ",
            "This indicates that ",
            "collectively indicate that ",
            "strongly suggest that ",
            "These findings suggest that ",
            "This strongly suggests that ",
        ]
        
        cleaned = answer
        for phrase in filler_phrases:
            cleaned = cleaned.replace(phrase, "")
            cleaned = cleaned.replace(phrase.lower(), "")
        
        # Remove excessive newlines
        while "\n\n\n" in cleaned:
            cleaned = cleaned.replace("\n\n\n", "\n\n")
        
        return cleaned.strip()
    
    def _generate_fallback_answer(
        self,
        question: str,
        evidence: List[Dict[str, Any]],
        investigation_steps: List[Any],
        confidence_score: float,
        root_causes: List[Dict[str, Any]] = None
    ) -> str:
        """Generate fallback answer if Bedrock fails."""
        parts = []
        
        # Root cause
        service = None
        error_type = None
        if root_causes and root_causes[0].get("confidence", 0) > 0.3:
            rc = root_causes[0]
            parts.append(f"**Root Cause**: {rc.get('description', 'Unknown')}")
            service = rc.get("service")
            error_type = rc.get("type")
        else:
            parts.append("**Root Cause**: Insufficient evidence to determine")
        
        # Evidence
        parts.append("\n**Evidence**:")
        if evidence:
            for e in evidence[:3]:
                content = e.get('content', '')
                if content and not self._is_generic_finding(content):
                    parts.append(f"- {content}")
        
        if len(parts) == 2:  # Only header added
            parts.append("- No specific error patterns found")
        
        # Confidence
        parts.append(f"\n**Confidence**: {confidence_score:.0%}")
        
        # Context-specific next step
        next_step = self._get_specific_next_step(service, error_type, root_causes, evidence)
        parts.append(f"\n**Next Step**: {next_step}")
        
        return "\n".join(parts)
    
    def _get_specific_next_step(
        self,
        service: str,
        error_type: str,
        root_causes: List[Dict[str, Any]],
        evidence: List[Dict[str, Any]]
    ) -> str:
        """Generate a specific next step based on the findings."""
        
        # Check for cascade pattern
        if root_causes:
            for rc in root_causes:
                if rc.get("type") == "cascade_origin":
                    origin = rc.get("service", "upstream service")
                    return f"Investigate {origin} - it's the origin of the cascade failure"
                
                if rc.get("type") == "upstream_failure":
                    upstream = rc.get("service", "upstream service")
                    return f"Check {upstream} health and connectivity"
        
        # Check for specific error patterns in evidence
        error_keywords = {
            "timeout": "Check network latency and increase timeout thresholds",
            "connection refused": "Verify the target service is running and port is accessible",
            "500": "Check application logs for stack traces",
            "503": "Service is overloaded - check resource utilization",
            "404": "Verify the endpoint URL and routing configuration",
            "auth": "Check authentication credentials and token expiration",
            "null": "Check for missing required fields in the request/data",
            "database": "Check database connectivity and query performance",
            "memory": "Check for memory leaks and increase heap size",
            "disk": "Check disk space and I/O performance",
        }
        
        for e in evidence:
            content = str(e.get("content", "")).lower()
            for keyword, action in error_keywords.items():
                if keyword in content:
                    return action
        
        # Service-specific fallback
        if service:
            return f"Check {service} application logs for detailed error messages"
        
        # Generic but slightly better fallback
        if evidence:
            return "Enable debug logging to capture more detailed error information"
        
        return "Add more specific error logging to identify the failure point"