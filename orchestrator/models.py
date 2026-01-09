"""Orchestrator data models."""
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime

class InvestigationHypothesis(BaseModel):
    """Investigation hypothesis model."""
    hypothesis: str
    priority: int
    query_template: Optional[str] = None

class InvestigationStep(BaseModel):
    """Investigation step model."""
    step_number: int
    hypothesis: str
    spl_query: str
    results_summary: str
    findings: List[Dict[str, Any]]
    timestamp: datetime

class InvestigationResult(BaseModel):
    """Final investigation result."""
    answer: str
    confidence_score: float
    evidence: List[Dict[str, Any]]
    investigation_steps: List[InvestigationStep]
    root_causes: List[str]

