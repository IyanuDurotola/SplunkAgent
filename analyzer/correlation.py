"""Enhanced pattern correlation with temporal and transaction-based analysis."""
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict
import structlog
import re

logger = structlog.get_logger()


class PatternCorrelation:
    """Enhanced pattern correlation engine with multi-dimensional analysis."""
    
    # Common correlation identifiers in logs (simplified to most common)
    CORRELATION_FIELDS = [
        "transactionId", "transaction_id",
        "traceId", "trace_id",
        "correlationId", "correlation_id"
    ]
    
    def correlate_by_time(
        self,
        events: List[Dict[str, Any]],
        time_window_seconds: int = 60
    ) -> List[Dict[str, Any]]:
        """Find events that occurred within a time window of each other."""
        correlations = []
        
        if len(events) < 2:
            return correlations
        
        # Sort by timestamp (Splunk commonly uses `_time`, but some environments emit `time`)
        sorted_events = sorted(events, key=lambda x: self._get_event_time_str(x) or "")
        
        for i, event in enumerate(sorted_events):
            event_time = self._parse_timestamp(self._get_event_time_str(event))
            if not event_time:
                continue
            
            related_events = []
            for j, other_event in enumerate(sorted_events):
                if i == j:
                    continue
                
                other_time = self._parse_timestamp(self._get_event_time_str(other_event))
                if not other_time:
                    continue
                
                time_diff = abs((event_time - other_time).total_seconds())
                if time_diff <= time_window_seconds:
                    related_events.append({
                        "event": other_event,
                        "time_diff_seconds": time_diff
                    })
            
            if related_events:
                correlations.append({
                    "anchor_event": event,
                    "related_events": related_events,
                    "correlation_type": "temporal"
                })
        
        return correlations
    
    def correlate_by_transaction(
        self,
        events: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Group events by transaction/trace/correlation ID."""
        transactions = defaultdict(list)
        
        for event in events:
            # Try to extract correlation ID (simplified)
            correlation_id = self._extract_correlation_id(event)
            
            if correlation_id:
                transactions[correlation_id].append({
                    "event": event,
                    "service": event.get("index", event.get("source", "unknown")),
                    "timestamp": self._get_event_time_str(event)
                })
        
        # Sort events within each transaction by timestamp
        for tx_id in transactions:
            transactions[tx_id].sort(key=lambda x: x.get("timestamp", ""))
        
        logger.info("Correlated by transaction", 
                   transaction_count=len(transactions),
                   total_events=sum(len(v) for v in transactions.values()))
        
        return dict(transactions)
    
    def find_recurring_patterns(
        self,
        current_events: List[Dict[str, Any]],
        historical_incidents: List[Dict[str, Any]],
        similarity_threshold: float = 0.6
    ) -> List[Dict[str, Any]]:
        """Find patterns in current events that match historical incidents."""
        recurring = []
        
        # Extract simplified error signatures (service + error keywords only)
        current_signatures = self._extract_error_signatures(current_events)
        
        for incident in historical_incidents:
            historical_signatures = self._extract_error_signatures(
                incident.get("events", [incident])
            )
            
            for curr_sig in current_signatures:
                for hist_sig in historical_signatures:
                    similarity = self._signature_similarity(curr_sig, hist_sig)
                    
                    if similarity >= similarity_threshold:
                        recurring.append({
                            "current_signature": curr_sig,
                            "historical_incident": incident,
                            "similarity": similarity,
                            "historical_resolution": incident.get("resolution", incident.get("answer", ""))
                        })
        
        # Sort by similarity
        recurring.sort(key=lambda x: x["similarity"], reverse=True)
        
        logger.info("Found recurring patterns", count=len(recurring))
        return recurring
    
    # Helper methods
    def _get_event_time_str(self, event: Dict[str, Any]) -> Optional[str]:
        """Extract a timestamp string from an event across common field names."""
        return event.get("time") or event.get("_time") or event.get("timestamp")

    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse Splunk timestamp (ISO format)."""
        if not timestamp_str:
            return None
        
        # Splunk typically uses ISO format: 2024-01-01T12:00:00.000+00:00 or 2024-01-01T12:00:00
        try:
            # Try ISO format with timezone
            if '+' in timestamp_str or timestamp_str.endswith('Z'):
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            # Try ISO format without timezone
            return datetime.fromisoformat(timestamp_str)
        except (ValueError, AttributeError):
            # Fallback: try common format
            try:
                return datetime.strptime(timestamp_str[:19], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                return None
    
    def _extract_correlation_id(self, event: Dict[str, Any]) -> Optional[str]:
        """Extract correlation ID from an event (simplified)."""
        # Check direct fields first (most common case)
        for field in self.CORRELATION_FIELDS:
            if field in event and event[field]:
                return str(event[field])
        
        # Check in raw message (simplified regex)
        raw = str(event.get("_raw", ""))
        if raw:
            # Look for common patterns: transactionId="xxx" or transactionId=xxx
            for field in self.CORRELATION_FIELDS:
                pattern = rf'{field}[=:]\s*["\']?([a-zA-Z0-9\-_]+)["\']?'
                match = re.search(pattern, raw, re.IGNORECASE)
                if match:
                    return match.group(1)
        
        return None
    
    def _is_error_event(self, event: Dict[str, Any]) -> bool:
        """Check if event represents an error."""
        level = str(event.get("level", event.get("log_level", ""))).lower()
        raw = str(event.get("_raw", "")).lower()
        
        return (
            level in ["error", "fatal", "critical"] or
            any(x in raw for x in ["error", "exception", "failed", "failure", "timeout"])
        )
    
    def _extract_error_signatures(
        self, 
        events: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Extract simplified error signatures (service + error keywords only)."""
        signatures = []
        
        for event in events:
            if not self._is_error_event(event):
                continue
            
            raw = event.get("_raw", event.get("message", ""))
            
            # Simplified signature: service + error keywords
            signature = {
                "service": event.get("index", event.get("source", "unknown")),
                "error_keywords": self._extract_error_keywords(raw),
                "error_codes": self._extract_error_codes(raw)
            }
            signatures.append(signature)
        
        return signatures
    
    def _extract_error_keywords(self, text: str) -> List[str]:
        """Extract error-related keywords from text."""
        keywords = []
        text_lower = text.lower()
        
        error_terms = [
            "timeout", "connection refused", "null pointer", "out of memory",
            "permission denied", "not found", "invalid", "failed", "exception",
            "error", "fatal", "critical", "unauthorized", "forbidden"
        ]
        
        for term in error_terms:
            if term in text_lower:
                keywords.append(term)
        
        return keywords
    
    def _extract_error_codes(self, text: str) -> List[str]:
        """Extract error codes from text."""
        codes = []
        
        # HTTP status codes
        http_codes = re.findall(r'\b[45]\d{2}\b', text)
        codes.extend(http_codes)
        
        return list(set(codes))
    
    def _signature_similarity(
        self, 
        sig1: Dict[str, Any], 
        sig2: Dict[str, Any]
    ) -> float:
        """Calculate similarity between two error signatures (simplified)."""
        score = 0.0
        
        # Service match (weight: 0.4)
        if sig1.get("service") == sig2.get("service"):
            score += 0.4
        
        # Error keywords overlap (weight: 0.4)
        keywords1 = set(sig1.get("error_keywords", []))
        keywords2 = set(sig2.get("error_keywords", []))
        if keywords1 and keywords2:
            overlap = len(keywords1 & keywords2) / len(keywords1 | keywords2)
            score += 0.4 * overlap
        
        # Error codes match (weight: 0.2)
        codes1 = set(sig1.get("error_codes", []))
        codes2 = set(sig2.get("error_codes", []))
        if codes1 and codes2:
            overlap = len(codes1 & codes2) / len(codes1 | codes2)
            score += 0.2 * overlap
        
        return score
