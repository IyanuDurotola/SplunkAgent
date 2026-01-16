"""Splunk REST API client."""
import splunklib.client as client
from typing import Dict, Any, Optional
import structlog
import asyncio
import os
import urllib3
import json

from splunk_integration.config import SplunkConfig
from splunk_integration.models import SplunkSearchResult

# Disable SSL warnings if verification is disabled
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = structlog.get_logger()

class SplunkClient:
    """Splunk REST API client."""
    
    def __init__(self):
        self.config = SplunkConfig()
        self.service = None
        self._connected = False
    
    def _ensure_connected(self):
        """Ensure connection to Splunk is established (lazy connection)."""
        if self._connected and self.service:
            return
        
        # Check if Splunk is configured
        if not self.config.is_configured():
            raise ConnectionError(
                f"Splunk not configured. Required: SPLUNK_HOST, SPLUNK_USERNAME, SPLUNK_PASSWORD. "
                f"Current: host={self.config.host}, username={'*' if self.config.username else None}"
            )
        
        try:
            logger.info("Attempting to connect to Splunk", 
                       host=self.config.host, 
                       port=self.config.port, 
                       scheme=self.config.scheme,
                       verify_ssl=self.config.verify)
            
            # Prepare connection kwargs
            connect_kwargs = {
                "host": self.config.host,
                "port": self.config.port,
                "username": self.config.username,
                "password": self.config.password,
                "scheme": self.config.scheme,
            }
            
            # Handle SSL verification
            if not self.config.verify:
                # Disable SSL verification for self-signed certificates
                connect_kwargs["verify"] = False
                # Also set environment variable for underlying HTTP library
                # This helps with libraries that don't respect the verify parameter
                os.environ['PYTHONHTTPSVERIFY'] = '0'
                logger.warning("SSL verification disabled - using self-signed certificates", 
                             host=self.config.host)
            else:
                connect_kwargs["verify"] = True
                # Ensure SSL verification is enabled if verify=True
                if 'PYTHONHTTPSVERIFY' in os.environ:
                    del os.environ['PYTHONHTTPSVERIFY']
            
            self.service = client.connect(**connect_kwargs)
            self._connected = True
            logger.info("Successfully connected to Splunk", host=self.config.host, port=self.config.port)
        except ConnectionRefusedError as e:
            self._connected = False
            logger.error(
                "Connection refused to Splunk. Check:",
                error=str(e),
                host=self.config.host,
                port=self.config.port,
                scheme=self.config.scheme,
                troubleshooting="Verify Splunk is running and accessible from this network"
            )
            raise
        except Exception as e:
            self._connected = False
            logger.error(
                "Failed to connect to Splunk",
                error=str(e),
                error_type=type(e).__name__,
                host=self.config.host,
                port=self.config.port,
                scheme=self.config.scheme,
                verify_ssl=self.config.verify
            )
            raise
    
    async def search(
        self,
        query: str,
        output_mode: str = "json",
        count: int = 1000,
        **kwargs
    ) -> Dict[str, Any]:
        """Execute a Splunk search query."""
        # Try to ensure connection, but handle failures gracefully
        try:
            self._ensure_connected()
        except ConnectionError as e:
            # Configuration error - return helpful message
            logger.warning("Splunk not configured or connection failed", error=str(e))
            return {
                "results": [],
                "total_count": 0,
                "fields": [],
                "error": str(e),
                "warning": "Splunk queries will return empty results. Check SPLUNK_HOST, SPLUNK_USERNAME, SPLUNK_PASSWORD in .env"
            }
        except Exception as e:
            # Connection error - log details but continue
            logger.warning("Cannot connect to Splunk - continuing with empty results", 
                         error=str(e), 
                         host=self.config.host,
                         port=self.config.port)
            return {
                "results": [],
                "total_count": 0,
                "fields": [],
                "error": f"Connection failed: {str(e)}",
                "warning": "Verify Splunk is running and accessible"
            }
        
        def _search_sync():
            try:
                job = self.service.jobs.oneshot(query, output_mode=output_mode, count=count, **kwargs)

                # Important: oneshot returns a response stream. Iterating it yields raw chunks,
                # not "event dicts" like the UI. Read the full payload and parse JSON properly.
                raw_bytes = b""
                try:
                    # ResponseReader supports .read()
                    raw_bytes = job.read()  # type: ignore[attr-defined]
                except Exception:
                    # Fallback: join chunks
                    raw_bytes = b"".join(chunk if isinstance(chunk, (bytes, bytearray)) else str(chunk).encode("utf-8") for chunk in job)

                text = raw_bytes.decode("utf-8", errors="ignore").strip()
                if not text:
                    return {"results": [], "total_count": 0, "fields": [], "messages": []}

                results: list = []
                messages: list = []

                # Splunk oneshot JSON can be:
                # - a single JSON object (possibly pretty-printed with newlines)
                # - newline-delimited JSON objects (NDJSON)
                parsed_any = False

                # First try: parse the entire payload as JSON.
                try:
                    obj = json.loads(text)
                    parsed_any = True
                    if isinstance(obj, dict):
                        if isinstance(obj.get("results"), list):
                            results.extend(obj.get("results", []))
                        if isinstance(obj.get("messages"), list):
                            messages.extend(obj.get("messages", []))
                    elif isinstance(obj, list):
                        results.extend(obj)
                except json.JSONDecodeError:
                    # Second try: NDJSON line-by-line.
                    lines = [ln for ln in text.splitlines() if ln.strip()]
                    for ln in lines:
                        try:
                            obj = json.loads(ln)
                            parsed_any = True
                        except json.JSONDecodeError:
                            continue

                        if isinstance(obj, dict):
                            if isinstance(obj.get("results"), list):
                                results.extend(obj.get("results", []))
                            if isinstance(obj.get("messages"), list):
                                messages.extend(obj.get("messages", []))
                        elif isinstance(obj, list):
                            results.extend(obj)

                if not parsed_any:
                    # Fallback: return raw text as a single event (so we can still debug what Splunk returned)
                    results = [{"_raw": text}]

                fields = []
                if results and isinstance(results[0], dict):
                    fields = list(results[0].keys())

                return {
                    "results": results,
                    "total_count": len(results),
                    "fields": fields,
                    "messages": messages,
                }
            except Exception as e:
                # Reset connection on error
                self._connected = False
                self.service = None
                raise
        
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, _search_sync)
            logger.info("Splunk search completed", query=query[:100], results_count=result["total_count"])
            return result
        except Exception as e:
            logger.error("Splunk search failed", error=str(e), query=query[:100])
            # Return empty results instead of crashing
            return {
                "results": [],
                "total_count": 0,
                "fields": [],
                "error": str(e)
            }
    
    async def create_job(self, query: str, **kwargs) -> str:
        """Create a Splunk search job."""
        # Ensure connection before creating job
        self._ensure_connected()
        
        def _create_job_sync():
            try:
                job = self.service.jobs.create(query, **kwargs)
                return job.sid
            except Exception as e:
                # Reset connection on error
                self._connected = False
                self.service = None
                raise
        
        try:
            loop = asyncio.get_event_loop()
            job_id = await loop.run_in_executor(None, _create_job_sync)
            logger.info("Created Splunk job", job_id=job_id)
            return job_id
        except Exception as e:
            logger.error("Failed to create Splunk job", error=str(e), query=query[:100])
            raise

