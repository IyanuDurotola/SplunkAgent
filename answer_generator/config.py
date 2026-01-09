"""Answer generator configuration."""
from pydantic_settings import BaseSettings
from typing import Optional

class AnswerGeneratorConfig(BaseSettings):
    """Answer generator configuration."""
    llm_provider: str = "bedrock"
    llm_model: str = "claude-3-sonnet"  # claude-3-sonnet, claude-3-haiku, claude-3-opus, etc.
    llm_temperature: float = 0.7
    # AWS Bedrock configuration (uses shared AWS credentials from AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    aws_region: str = "us-east-1"
    
    class Config:
        env_file = ".env"
        env_prefix = "ANSWER_GEN_"

