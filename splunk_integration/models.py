"""Splunk integration models."""
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class SplunkSearchResult(BaseModel):
    """Splunk search result model."""
    results: List[Dict[str, Any]]
    total_count: int
    fields: List[str]
    preview: bool

class SplunkJob(BaseModel):
    """Splunk job model."""
    sid: str
    status: str
    is_done: bool

