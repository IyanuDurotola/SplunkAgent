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
    
    def constrain_query(self, query: str, time_window: tuple) -> tuple:
        """Constrain query with time window and other limits.
        
        Returns:
            tuple: (cleaned_query, time_params_dict) where time_params_dict contains
                   'earliest' and 'latest' as Unix epoch timestamps for Splunk API kwargs
        """
        # Ensure time window is within limits
        from datetime import datetime, timedelta
        start, end = time_window
        
        # Normalize time window ordering (Splunk requires latest_time > earliest_time)
        if start > end:
            logger.warning("Start time is after end time; swapping for Splunk", start_time=start, end_time=end)
            start, end = end, start
        elif start == end:
            # Splunk rejects latest_time == earliest_time
            end = start + timedelta(seconds=1)
            logger.warning("Start time equals end time; expanding by 1s for Splunk", start_time=start, end_time=end)
        days_diff = (end - start).days
        
        if days_diff > self.MAX_TIME_RANGE_DAYS:
            logger.warning("Time window exceeds max, constraining", days=days_diff)
            end = start + timedelta(days=self.MAX_TIME_RANGE_DAYS)
        
        # Convert datetime to Unix epoch timestamp (seconds) for Splunk API
        earliest_timestamp = int(start.timestamp())
        latest_timestamp = int(end.timestamp())
        
        # Remove any existing earliest/latest from query string (they'll be passed as kwargs)
        # Clean the query to remove time constraints that might have been added
        # query_cleaned = query
        # # Remove earliest= and latest= patterns from query
        # query_cleaned = re.sub(r'earliest\s*=\s*[^\s"]+\s*', '', query_cleaned, flags=re.IGNORECASE)
        # query_cleaned = re.sub(r'latest\s*=\s*[^\s"]+\s*', '', query_cleaned, flags=re.IGNORECASE)
        # query_cleaned = re.sub(r'\s+', ' ', query_cleaned).strip()  # Clean up extra spaces
        
        time_params = {
            'earliest_time': earliest_timestamp,
            'latest_time': latest_timestamp
        }
        
        return query, time_params

