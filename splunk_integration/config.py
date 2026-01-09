"""Splunk configuration."""
from pydantic_settings import BaseSettings
from typing import Optional

class SplunkConfig(BaseSettings):
    """Splunk configuration."""
    host: Optional[str] = None
    port: int = 8089
    username: Optional[str] = None
    password: Optional[str] = None
    scheme: str = "https"
    verify: bool = False
    hec_token: Optional[str] = None
    hec_port: int = 8088
    
    class Config:
        env_file = ".env"
        env_prefix = "SPLUNK_"
    
    def is_configured(self) -> bool:
        """Check if Splunk is properly configured."""
        return all([self.host, self.username, self.password])

