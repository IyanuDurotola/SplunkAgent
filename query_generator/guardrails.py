"""Guardrails for validating and constraining SPL queries."""
from typing import List, Dict
import re
import structlog

from shared.exceptions import ValidationError

logger = structlog.get_logger()

class QueryGuardrails:
    """Guardrails for validating and constraining SPL queries."""
    
    DANGEROUS_COMMANDS = ['delete', 'eval.*delete', 'outputlookup.*append']
    MAX_TIME_RANGE_DAYS = 30
    
    def validate_query(self, query: str) -> bool:
        """Validate that the query is safe to execute."""
        query_lower = query.lower()
        
        # Check for dangerous commands
        for dangerous in self.DANGEROUS_COMMANDS:
            if re.search(dangerous, query_lower):
                raise ValidationError(f"Query contains dangerous command: {dangerous}")
        
        # Check query length
        if len(query) > 10000:
            raise ValidationError("Query exceeds maximum length")
        
        # Check for basic SPL structure
        if not any(keyword in query_lower for keyword in ['index=', 'search', '|']):
            logger.warning("Query may not be valid SPL", query=query)
        
        return True
    
    def constrain_query(self, query: str, time_window: tuple) -> str:
        """Constrain query with time window and other limits."""
        # Ensure time window is within limits
        from datetime import datetime, timedelta
        start, end = time_window
        days_diff = (end - start).days
        
        if days_diff > self.MAX_TIME_RANGE_DAYS:
            logger.warning("Time window exceeds max, constraining", days=days_diff)
            end = start + timedelta(days=self.MAX_TIME_RANGE_DAYS)
        
        # Add time constraints if not present
        if 'earliest=' not in query.lower() and 'latest=' not in query.lower():
            time_constraint = f'earliest="{start.strftime("%Y-%m-%dT%H:%M:%S")}" latest="{end.strftime("%Y-%m-%dT%H:%M:%S")}"'
            if 'index=' in query:
                query = query.replace('index=', f'{time_constraint} index=', 1)
            else:
                query = f'{time_constraint} {query}'
        
        return query

