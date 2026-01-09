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

class QueryResponse(BaseModel):
    """Query response model."""
    answer: str
    confidence_score: float
    evidence: List[EvidenceItem]
    investigation_steps: List[Dict[str, Any]]
    processing_time_ms: float
    timestamp: datetime

