"""Database initialization script for PostgreSQL with pgvector."""
import asyncio
import asyncpg
import structlog
from memory.config import MemoryConfig

logger = structlog.get_logger()

async def init_database():
    """Initialize PostgreSQL database with pgvector extension."""
    config = MemoryConfig()
    
    try:
        # Connect to PostgreSQL (to default postgres database first to create our database)
        conn = await asyncpg.connect(
            host=config.db_host,
            port=config.db_port,
            user=config.db_user,
            password=config.db_password,
            database="postgres"  # Connect to default database
        )
        
        # Create database if it doesn't exist
        await conn.execute(f"""
            SELECT 1 FROM pg_database WHERE datname = '{config.db_name}'
        """)
        
        db_exists = await conn.fetchval(f"""
            SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = '{config.db_name}')
        """)
        
        if not db_exists:
            await conn.execute(f'CREATE DATABASE {config.db_name}')
            logger.info("Created database", database=config.db_name)
        else:
            logger.info("Database already exists", database=config.db_name)
        
        await conn.close()
        
        # Now connect to our database and set up extensions/tables
        conn = await asyncpg.connect(
            host=config.db_host,
            port=config.db_port,
            user=config.db_user,
            password=config.db_password,
            database=config.db_name
        )
        
        # Enable pgvector extension
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        logger.info("Enabled pgvector extension")
        
        # Check if table exists and has correct dimensions
        table_exists = await conn.fetchval(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{config.table_name}'
            )
        """)
        
        if table_exists:
            # Check current embedding dimension
            try:
                current_dim = await conn.fetchval(f"""
                    SELECT atttypmod - 4 
                    FROM pg_attribute 
                    WHERE attrelid = '{config.table_name}'::regclass 
                    AND attname = 'embedding'
                """)
                
                if current_dim and current_dim != config.embedding_dimension:
                    logger.warning(
                        "Table exists with wrong dimensions - dropping and recreating",
                        current=current_dim,
                        expected=config.embedding_dimension
                    )
                    # Drop indexes and table (safe for fresh project)
                    await conn.execute(f"DROP INDEX IF EXISTS {config.table_name}_embedding_idx")
                    await conn.execute(f"DROP INDEX IF EXISTS {config.table_name}_metadata_idx")
                    await conn.execute(f"DROP INDEX IF EXISTS {config.table_name}_created_at_idx")
                    await conn.execute(f"DROP TABLE IF EXISTS {config.table_name}")
                    logger.info("Dropped old table with incorrect dimensions")
            except Exception as e:
                logger.warning("Could not check embedding dimension", error=str(e))
        
        # Create incidents table with correct dimensions
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {config.table_name} (
                id SERIAL PRIMARY KEY,
                doc_id VARCHAR(255) UNIQUE NOT NULL,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                document_text TEXT NOT NULL,
                embedding vector({config.embedding_dimension}),
                metadata JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("Created incidents table", dimension=config.embedding_dimension)
        
        # Create indexes
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS {config.table_name}_embedding_idx 
            ON {config.table_name} 
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100)
        """)
        
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS {config.table_name}_metadata_idx 
            ON {config.table_name} 
            USING GIN (metadata)
        """)
        
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS {config.table_name}_created_at_idx 
            ON {config.table_name} (created_at)
        """)
        
        logger.info("Created indexes")
        
        await conn.close()
        logger.info("Database initialization complete")
        
    except Exception as e:
        logger.error("Failed to initialize database", error=str(e))
        raise

if __name__ == "__main__":
    asyncio.run(init_database())

