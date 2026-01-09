"""Service for generating embeddings using Amazon Bedrock Titan."""
from typing import List
import structlog
import os
import json
import asyncio
import boto3
from botocore.exceptions import ClientError

logger = structlog.get_logger()

class EmbeddingService:
    """Service for generating embeddings using Amazon Bedrock Titan."""
    
    # Titan embedding model IDs
    TITAN_EMBEDDING_MODELS = {
        "titan-embed-v1": "amazon.titan-embed-text-v1",
        "titan-embed-v2": "amazon.titan-embed-text-v2:0",
    }
    
    # Embedding dimensions for Titan models
    TITAN_DIMENSIONS = {
        "titan-embed-v1": 1536,
        "titan-embed-v2": 1024,
    }
    
    def __init__(
        self,
        model_name: str = "titan-embed-v1",
        region_name: str = None
    ):
        """Initialize Bedrock embedding service (uses shared AWS credentials from environment)."""
        self.model_name = model_name
        self.model_id = self.TITAN_EMBEDDING_MODELS.get(model_name, model_name)
        self.embedding_dimension = self.TITAN_DIMENSIONS.get(model_name, 1536)
        
        # Initialize Bedrock client (uses shared AWS credentials from environment)
        region = region_name or os.getenv("AWS_REGION", "us-east-1")
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        client_kwargs = {"service_name": "bedrock-runtime", "region_name": region}
        if access_key and secret_key:
            client_kwargs.update({
                "aws_access_key_id": access_key,
                "aws_secret_access_key": secret_key
            })
        
        self.client = boto3.client(**client_kwargs)
        logger.info("Initialized Bedrock embedding service", model=self.model_id, dimension=self.embedding_dimension)
    
    async def encode(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for texts using Bedrock Titan."""
        embeddings = []
        
        # Bedrock Titan can handle batch requests, but we'll process one at a time for simplicity
        # You can optimize this to batch if needed
        for text in texts:
            try:
                embedding = await self._get_embedding(text)
                embeddings.append(embedding)
            except Exception as e:
                logger.error("Failed to generate embedding", text=text[:50], error=str(e))
                # Return zero vector as fallback
                embeddings.append([0.0] * self.embedding_dimension)
        
        return embeddings
    
    async def _get_embedding(self, text: str) -> List[float]:
        """Get embedding for a single text using Bedrock Titan."""
        body = {
            "inputText": text
        }
        
        # Run synchronous boto3 call in executor
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: self.client.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body)
            )
        )
        
        response_body = json.loads(response['body'].read())
        embedding = response_body['embedding']
        
        return embedding
    
    def encode_sync(self, texts: List[str]) -> List[List[float]]:
        """Synchronous version for backward compatibility."""
        # Use asyncio to run async method
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        return loop.run_until_complete(self.encode(texts))
