"""Splunk HEC (HTTP Event Collector) API client."""
import httpx
import structlog
from typing import Dict, Any, Optional

from splunk_integration.config import SplunkConfig

logger = structlog.get_logger()

class HECClient:
    """Splunk HEC API client for sending events."""
    
    def __init__(self, config: Optional[SplunkConfig] = None):
        self.config = config or SplunkConfig()
        self.base_url = f"{self.config.scheme}://{self.config.host}:{self.config.hec_port}/services/collector"
        self.headers = {
            "Authorization": f"Splunk {self.config.hec_token}",
            "Content-Type": "application/json"
        }
    
    async def send_event(self, event: Dict[str, Any], source: Optional[str] = None):
        """Send a single event to Splunk via HEC."""
        if not self.config.hec_token:
            logger.warning("HEC token not configured, cannot send event")
            return
        
        payload = {
            "event": event,
            "sourcetype": "_json"
        }
        if source:
            payload["source"] = source
        
        async with httpx.AsyncClient(verify=self.config.verify) as client:
            try:
                response = await client.post(self.base_url, json=payload, headers=self.headers)
                response.raise_for_status()
                logger.info("Sent event to Splunk via HEC", status_code=response.status_code)
            except Exception as e:
                logger.error("Failed to send event to Splunk", error=str(e))
                raise

