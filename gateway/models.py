"""Gateway request/response models."""
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime


class QueryRequest(BaseModel):
    """User query request model."""
    question: str
    time_window: Optional[str] = None  # e.g., "24h", "7d", "1h"
    context: Optional[Dict[str, Any]] = None


class EvidenceItem(BaseModel):
    """Evidence item model."""
    source: str
    content: str
    relevance_score: float
    timestamp: Optional[datetime] = None
    significance: Optional[str] = None
    finding_type: Optional[str] = None


class SupportingEvidence(BaseModel):
    """Supporting evidence for confidence scoring."""
    type: str  # quality, quantity, consistency, service, temporal, historical
    finding: str
    impact: str  # positive, neutral, negative
    details: Optional[Dict[str, Any]] = None


class ConfidenceDetails(BaseModel):
    """Detailed breakdown of confidence scoring."""
    factors: Dict[str, Any]
    reasoning: str


class RootCause(BaseModel):
    """Root cause analysis result."""
    description: str
    confidence: float
    type: str
    service: Optional[str] = None
    evidence: Optional[Dict[str, Any]] = None


class QueryResponse(BaseModel):
    """Query response model with enhanced confidence details."""
    answer: str
    confidence_score: float
    confidence_level: str  # very_high, high, medium, low, very_low
    confidence_details: Optional[ConfidenceDetails] = None
    supporting_evidence: Optional[List[SupportingEvidence]] = None
    evidence: List[Dict[str, Any]]  # Changed from EvidenceItem for flexibility
    investigation_steps: List[Dict[str, Any]]
    root_causes: Optional[List[Dict[str, Any]]] = None
    correlations: Optional[Dict[str, Any]] = None
    processing_time_ms: float
    timestamp: datetime
    requires_user_input: Optional[bool] = False
    available_services: Optional[List[str]] = None
