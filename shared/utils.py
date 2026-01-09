"""Shared utility functions."""
from datetime import datetime, timedelta
from typing import Optional, Tuple

def parse_time_window(time_window: Optional[str]) -> Tuple[datetime, datetime]:
    """Parse time window string into start and end datetime."""
    if time_window is None:
        # Default to last 24 hours
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
        return start_time, end_time
    
    # Handle relative time windows like "1h", "24h", "7d"
    if time_window.endswith('h'):
        hours = int(time_window[:-1])
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
    elif time_window.endswith('d'):
        days = int(time_window[:-1])
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days)
    else:
        # Default to 24 hours if format is unknown
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
    
    return start_time, end_time

