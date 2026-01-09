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
        confidence_score: float
    ) -> str:
        """Generate final grounded explanation using Amazon Bedrock."""
        system_prompt = """You are an expert technical analyst specializing in root cause analysis and system troubleshooting. 
Your task is to provide clear, concise, and actionable explanations based on investigation evidence.

Guidelines:
- Provide a clear, structured explanation
- Reference specific evidence and findings
- Explain the root causes clearly
- Use professional but accessible language
- Include confidence assessment when relevant"""

        # Format investigation steps
        steps_summary = "\n".join([
            f"Step {step.get('step_number', i+1)}: {step.get('hypothesis', 'N/A')}\n"
            f"  Results: {step.get('results_summary', 'N/A')}\n"
            f"  Findings: {json.dumps(step.get('findings', []), indent=2)}"
            for i, step in enumerate(investigation_steps[:5])  # Top 5 steps
        ])
        
        # Format evidence
        evidence_summary = "\n".join([
            f"- {e.get('content', 'N/A')} (Relevance: {e.get('relevance_score', 0):.2f}, Source: {e.get('source', 'N/A')})"
            for e in evidence[:10]  # Top 10 evidence items
        ])
        
        user_prompt = f"""Based on the following investigation, provide a comprehensive explanation of the root cause analysis.

Original Question: {question}

Investigation Steps:
{steps_summary}

Key Evidence:
{evidence_summary}

Confidence Score: {confidence_score:.2f}

Please provide a clear, structured explanation that:
1. Summarizes the investigation findings
2. Identifies the root causes
3. Explains the evidence that supports these conclusions
4. Provides actionable insights"""

        try:
            answer = await self.bedrock_client.invoke(
                prompt=user_prompt,
                system_prompt=system_prompt,
                temperature=self.config.llm_temperature,
                max_tokens=2000
            )
            
            logger.info("Generated final answer using Bedrock", answer_length=len(answer))
            return answer
            
        except Exception as e:
            logger.error("Failed to generate answer using Bedrock", error=str(e))
            # Fallback to template-based answer
            return self._generate_fallback_answer(question, evidence, investigation_steps, confidence_score)
    
    def _generate_fallback_answer(
        self,
        question: str,
        evidence: List[Dict[str, Any]],
        investigation_steps: List[Any],
        confidence_score: float
    ) -> str:
        """Generate fallback answer if Bedrock fails."""
        answer_parts = [
            f"Based on the investigation of '{question}', I found the following:",
            "",
            "**Investigation Summary:**"
        ]
        
        for step in investigation_steps[:3]:  # Top 3 steps
            answer_parts.append(f"- {step.get('hypothesis')}: {step.get('results_summary')}")
        
        if evidence:
            answer_parts.append("")
            answer_parts.append("**Key Evidence:**")
            for e in evidence[:5]:  # Top 5 evidence items
                answer_parts.append(f"- {e['content']} (relevance: {e['relevance_score']:.2f})")
        
        answer_parts.append("")
        answer_parts.append(f"**Confidence Score:** {confidence_score:.2f}")
        
        answer = "\n".join(answer_parts)
        logger.info("Generated fallback answer", answer_length=len(answer))
        return answer
