"""Vector database for storing and retrieving incidents using PostgreSQL with pgvector."""
import asyncpg
from typing import List, Dict, Any, Optional
from datetime import datetime
import structlog
import json

from memory.config import MemoryConfig
from memory.embeddings import EmbeddingService

logger = structlog.get_logger()

class VectorStore:
    """Vector database for storing and retrieving incidents using PostgreSQL with pgvector."""
    
    def __init__(self, config: MemoryConfig):
        self.config = config
        # EmbeddingService will use shared AWS credentials from environment variables
        self.embedding_service = EmbeddingService(
            model_name=config.embedding_model,
            region_name=config.aws_region
        )
        self.pool: Optional[asyncpg.Pool] = None
        logger.info("Initialized PostgreSQL vector store", table=config.table_name)
    
    async def initialize(self):
        """Initialize database connection pool and create tables if needed."""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.db_host,
                port=self.config.db_port,
                user=self.config.db_user,
                password=self.config.db_password,
                database=self.config.db_name,
                min_size=5,
                max_size=self.config.db_pool_size
            )
            await self._create_tables()
            logger.info("Connected to PostgreSQL and initialized tables")
        except Exception as e:
            logger.error("Failed to connect to PostgreSQL", error=str(e))
            raise
    
    async def close(self):
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Closed PostgreSQL connection pool")
    
    async def _create_tables(self):
        """Create tables and extensions if they don't exist."""
        async with self.pool.acquire() as conn:
            # Enable pgvector extension
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            
            # Check if table exists and get current dimension
            table_exists = await conn.fetchval(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{self.config.table_name}'
                )
            """)
            
            # Create incidents table with vector column
            # If table exists with wrong dimensions, drop and recreate
            if table_exists:
                # Check current embedding dimension
                try:
                    current_dim = await conn.fetchval(f"""
                        SELECT atttypmod - 4 
                        FROM pg_attribute 
                        WHERE attrelid = '{self.config.table_name}'::regclass 
                        AND attname = 'embedding'
                    """)
                    
                    if current_dim and current_dim != self.config.embedding_dimension:
                        logger.warning(
                            "Embedding dimension mismatch - dropping and recreating table",
                            current=current_dim,
                            expected=self.config.embedding_dimension
                        )
                        # Drop indexes and table
                        await conn.execute(f"DROP INDEX IF EXISTS {self.config.table_name}_embedding_idx")
                        await conn.execute(f"DROP INDEX IF EXISTS {self.config.table_name}_metadata_idx")
                        await conn.execute(f"DROP INDEX IF EXISTS {self.config.table_name}_created_at_idx")
                        await conn.execute(f"DROP TABLE IF EXISTS {self.config.table_name}")
                        logger.info("Dropped old table with incorrect dimensions")
                except Exception as e:
                    logger.warning("Could not check embedding dimension, will attempt to create table", error=str(e))
            
            # Create incidents table with vector column
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.config.table_name} (
                    id SERIAL PRIMARY KEY,
                    doc_id VARCHAR(255) UNIQUE NOT NULL,
                    question TEXT NOT NULL,
                    answer TEXT NOT NULL,
                    document_text TEXT NOT NULL,
                    embedding vector({self.config.embedding_dimension}),
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create index for vector similarity search
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS {self.config.table_name}_embedding_idx 
                ON {self.config.table_name} 
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100)
            """)
            
            # Create index for metadata queries
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS {self.config.table_name}_metadata_idx 
                ON {self.config.table_name} 
                USING GIN (metadata)
            """)
            
            # Create index for timestamp queries
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS {self.config.table_name}_created_at_idx 
                ON {self.config.table_name} (created_at)
            """)
            
            logger.info("Created/verified database tables and indexes")
    
    async def add_incident(
        self,
        question: str,
        answer: str,
        evidence: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Add an incident to the vector store."""
        if not self.pool:
            await self.initialize()
        
        text = f"Question: {question}\nAnswer: {answer}\nEvidence: {str(evidence)}"
        embeddings = await self.embedding_service.encode([text])
        embedding = embeddings[0]
        
        doc_id = f"incident_{datetime.utcnow().timestamp()}"
        metadatas = metadata or {}
        metadatas.update({
            "question": question,
            "timestamp": datetime.utcnow().isoformat(),
            "evidence_count": len(evidence)
        })
        
        # Convert embedding to PostgreSQL vector format
        embedding_str = "[" + ",".join(map(str, embedding)) + "]"
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self.config.table_name} 
                (doc_id, question, answer, document_text, embedding, metadata)
                VALUES ($1, $2, $3, $4, $5::vector, $6::jsonb)
                ON CONFLICT (doc_id) DO UPDATE SET
                    question = EXCLUDED.question,
                    answer = EXCLUDED.answer,
                    document_text = EXCLUDED.document_text,
                    embedding = EXCLUDED.embedding,
                    metadata = EXCLUDED.metadata,
                    updated_at = CURRENT_TIMESTAMP
                """,
                doc_id,
                question,
                answer,
                text,
                embedding_str,
                json.dumps(metadatas)
            )
        
        logger.info("Added incident to vector store", doc_id=doc_id)
    
    async def search_similar(
        self,
        query: str,
        top_k: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Search for similar incidents using cosine similarity."""
        if not self.pool:
            await self.initialize()
        
        top_k = top_k or self.config.top_k_results
        embeddings = await self.embedding_service.encode([query])
        query_embedding = embeddings[0]
        
        # Convert embedding to PostgreSQL vector format
        embedding_str = "[" + ",".join(map(str, query_embedding)) + "]"
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT 
                    doc_id,
                    question,
                    answer,
                    document_text,
                    metadata,
                    created_at,
                    1 - (embedding <=> $1::vector) as similarity
                FROM {self.config.table_name}
                ORDER BY embedding <=> $1::vector
                LIMIT $2
                """,
                embedding_str,
                top_k
            )
        
        incidents = []
        for row in rows:
            incidents.append({
                "id": row["doc_id"],
                "document": row["document_text"],
                "metadata": row["metadata"] if isinstance(row["metadata"], dict) else json.loads(row["metadata"]) if row["metadata"] else {},
                "distance": 1 - float(row["similarity"]),  # Convert similarity to distance
                "question": row["question"],
                "answer": row["answer"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None
            })
        
        logger.info("Searched vector store", query=query, results_count=len(incidents))
        return incidents
