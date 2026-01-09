"""Migration script to update embedding dimensions in PostgreSQL."""
import asyncio
import asyncpg
import structlog
from memory.config import MemoryConfig

logger = structlog.get_logger()

async def migrate_embedding_dimensions():
    """Migrate embedding column from old dimension to new dimension."""
    config = MemoryConfig()
    
    try:
        conn = await asyncpg.connect(
            host=config.db_host,
            port=config.db_port,
            user=config.db_user,
            password=config.db_password,
            database=config.db_name
        )
        
        # Check current dimension
        dimension_info = await conn.fetchval(f"""
            SELECT atttypmod - 4 
            FROM pg_attribute 
            WHERE attrelid = '{config.table_name}'::regclass 
            AND attname = 'embedding'
        """)
        
        if dimension_info is None:
            logger.info("Table or embedding column does not exist, will be created with correct dimensions")
            await conn.close()
            return
        
        current_dimension = dimension_info
        target_dimension = config.embedding_dimension
        
        logger.info("Current embedding dimension", current=current_dimension, target=target_dimension)
        
        if current_dimension == target_dimension:
            logger.info("Embedding dimension is already correct", dimension=current_dimension)
            await conn.close()
            return
        
        logger.warning("Embedding dimension mismatch detected", 
                      current=current_dimension, 
                      target=target_dimension)
        
        # Drop the table and recreate with correct dimensions
        # Note: This will delete all existing data
        logger.warning("Dropping and recreating table with correct dimensions. All existing data will be lost!")
        
        # Drop indexes first
        await conn.execute(f"DROP INDEX IF EXISTS {config.table_name}_embedding_idx")
        await conn.execute(f"DROP INDEX IF EXISTS {config.table_name}_metadata_idx")
        await conn.execute(f"DROP INDEX IF EXISTS {config.table_name}_created_at_idx")
        
        # Drop table
        await conn.execute(f"DROP TABLE IF EXISTS {config.table_name}")
        logger.info("Dropped old table")
        
        # Recreate table with correct dimensions
        await conn.execute(f"""
            CREATE TABLE {config.table_name} (
                id SERIAL PRIMARY KEY,
                doc_id VARCHAR(255) UNIQUE NOT NULL,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                document_text TEXT NOT NULL,
                embedding vector({target_dimension}),
                metadata JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("Created table with correct dimensions", dimension=target_dimension)
        
        # Recreate indexes
        await conn.execute(f"""
            CREATE INDEX {config.table_name}_embedding_idx 
            ON {config.table_name} 
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100)
        """)
        
        await conn.execute(f"""
            CREATE INDEX {config.table_name}_metadata_idx 
            ON {config.table_name} 
            USING GIN (metadata)
        """)
        
        await conn.execute(f"""
            CREATE INDEX {config.table_name}_created_at_idx 
            ON {config.table_name} (created_at)
        """)
        
        logger.info("Recreated indexes")
        
        await conn.close()
        logger.info("Migration completed successfully")
        
    except Exception as e:
        logger.error("Migration failed", error=str(e))
        raise

if __name__ == "__main__":
    asyncio.run(migrate_embedding_dimensions())
