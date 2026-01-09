"""Gateway configuration."""
from pydantic_settings import BaseSettings

class GatewayConfig(BaseSettings):
    """Gateway configuration."""
    api_title: str = "Splunk AI Query Gateway"
    api_version: str = "1.0.0"
    api_prefix: str = "/api/v1"
    host: str = "0.0.0.0"
    port: int = 8082
    
    class Config:
        env_file = ".env"
        env_prefix = "GATEWAY_"

