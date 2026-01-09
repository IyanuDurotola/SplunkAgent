"""Amazon Bedrock client for LLM operations."""
import boto3
import json
import asyncio
from typing import Optional, Dict, Any, List
import structlog
from botocore.exceptions import ClientError

logger = structlog.get_logger()

class BedrockClient:
    """Amazon Bedrock client for LLM operations."""
    
    # Model IDs for different Bedrock models
    MODELS = {
        "claude-3-sonnet": "anthropic.claude-3-sonnet-20240229-v1:0",
        "claude-3-haiku": "anthropic.claude-3-haiku-20240307-v1:0",
        "claude-3-opus": "anthropic.claude-3-opus-20240229-v1:0",
        "claude-2": "anthropic.claude-v2:1",
        "llama2-70b": "meta.llama2-70b-chat-v1",
        "llama2-13b": "meta.llama2-13b-chat-v1",
        "titan-text": "amazon.titan-text-express-v1",
        "titan-text-lite": "amazon.titan-text-lite-v1",
    }
    
    def __init__(
        self,
        region_name: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        model_id: str = "claude-3-sonnet"
    ):
        """Initialize Bedrock client."""
        self.region_name = region_name
        self.model_id = self.MODELS.get(model_id, model_id)  # Use provided or lookup
        
        # Initialize boto3 client
        client_kwargs = {"service_name": "bedrock-runtime", "region_name": region_name}
        if aws_access_key_id and aws_secret_access_key:
            client_kwargs.update({
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key
            })
        
        self.client = boto3.client(**client_kwargs)
        logger.info("Initialized Bedrock client", region=region_name, model=model_id)
    
    async def invoke(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        stop_sequences: Optional[List[str]] = None
    ) -> str:
        """Invoke Bedrock model and return response."""
        try:
            # Determine model provider
            if "anthropic" in self.model_id.lower() or "claude" in self.model_id.lower():
                return await self._invoke_anthropic(
                    prompt=prompt,
                    system_prompt=system_prompt,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    stop_sequences=stop_sequences
                )
            elif "meta" in self.model_id.lower() or "llama" in self.model_id.lower():
                return await self._invoke_llama(
                    prompt=prompt,
                    temperature=temperature,
                    max_tokens=max_tokens
                )
            elif "titan" in self.model_id.lower() or "amazon" in self.model_id.lower():
                return await self._invoke_titan(
                    prompt=prompt,
                    temperature=temperature,
                    max_tokens=max_tokens
                )
            else:
                # Default to Anthropic format
                return await self._invoke_anthropic(
                    prompt=prompt,
                    system_prompt=system_prompt,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    stop_sequences=stop_sequences
                )
        except ClientError as e:
            logger.error("Bedrock API error", error=str(e), error_code=e.response.get('Error', {}).get('Code'))
            raise
        except Exception as e:
            logger.error("Failed to invoke Bedrock", error=str(e))
            raise
    
    async def _invoke_anthropic(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        stop_sequences: Optional[List[str]] = None
    ) -> str:
        """Invoke Anthropic Claude model."""
        # Claude uses messages format with user/assistant roles
        messages = [{"role": "user", "content": prompt}]
        
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": messages
        }
        
        # Add system prompt if provided (Claude 3 supports system prompts)
        if system_prompt:
            body["system"] = system_prompt
        
        if stop_sequences:
            body["stop_sequences"] = stop_sequences
        
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
        return response_body['content'][0]['text']
    
    async def _invoke_llama(
        self,
        prompt: str,
        temperature: float = 0.7,
        max_tokens: int = 4096
    ) -> str:
        """Invoke Meta Llama model."""
        body = {
            "prompt": prompt,
            "max_gen_len": max_tokens,
            "temperature": temperature
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
        return response_body['generation']
    
    async def _invoke_titan(
        self,
        prompt: str,
        temperature: float = 0.7,
        max_tokens: int = 4096
    ) -> str:
        """Invoke Amazon Titan model."""
        body = {
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": max_tokens,
                "temperature": temperature
            }
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
        return response_body['results'][0]['outputText']
    
    async def chat_completion(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 4096
    ) -> str:
        """Chat completion interface compatible with OpenAI-style API."""
        # Convert messages to prompt
        system_prompt = None
        user_messages = []
        
        for msg in messages:
            if msg["role"] == "system":
                system_prompt = msg["content"]
            elif msg["role"] == "user":
                user_messages.append(msg["content"])
        
        prompt = "\n\n".join(user_messages)
        
        return await self.invoke(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=temperature,
            max_tokens=max_tokens
        )
