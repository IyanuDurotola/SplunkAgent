"""Memory/Vector DB configuration."""
from pydantic_settings import BaseSettings
from typing import Optional

class MemoryConfig(BaseSettings):
    """Memory/Vector DB configuration."""
    vector_db_type: str = "postgresql"
    # PostgreSQL connection settings
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "splunkprocessor"
    db_user: str = "postgres"
    db_password: str = "postgres"
    db_pool_size: int = 10
    db_max_overflow: int = 20
    # Embedding settings (Amazon Bedrock Titan)
    embedding_model: str = "titan-embed-v1"  # titan-embed-v1 (1536 dim) or titan-embed-v2 (1024 dim)
    embedding_dimension: int = 1536  # Dimension for titan-embed-v1 (v2 is 1024)
    # AWS Bedrock configuration (uses shared AWS credentials from AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    aws_region: str = "us-east-1"
    table_name: str = "incidents"
    top_k_results: int = 5
    
    class Config:
        env_file = ".env"
        env_prefix = "MEMORY_"
    
    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL."""
        return f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    @property
    def sync_database_url(self) -> str:
        """Get synchronous PostgreSQL connection URL."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

