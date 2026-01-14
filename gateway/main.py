"""FastAPI application entry point."""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import time
import structlog

from gateway.config import GatewayConfig
from gateway.models import QueryRequest, QueryResponse
from orchestrator.orchestrator import InvestigationOrchestrator
from shared.logger import setup_logging

config = GatewayConfig()
logger = setup_logging()
app = FastAPI(title=config.api_title, version=config.api_version)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    """Initialize orchestrator on startup."""
    logger.info("Starting AI Query Gateway")
    # Initialize database connections
    try:
        from memory.retrieval import MemoryRetrieval
        memory = MemoryRetrieval()
        await memory._ensure_initialized()
        logger.info("Database connections initialized")
    except Exception as e:
        logger.warning("Failed to initialize database connections", error=str(e))
        logger.warning("Application will continue but memory features may not work")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("Shutting down AI Query Gateway")
    # Close database connections
    try:
        from memory.retrieval import MemoryRetrieval
        memory = MemoryRetrieval()
        if hasattr(memory.vector_store, 'pool') and memory.vector_store.pool:
            await memory.vector_store.close()
    except Exception as e:
        logger.warning("Error closing database connections", error=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.post(f"{config.api_prefix}/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    """Process natural language query about system bugs."""
    start_time = time.time()
    logger.info("Received query", question=request.question)
    
    try:
        orchestrator = InvestigationOrchestrator()
        result = await orchestrator.investigate(
            question=request.question,
            time_window=request.time_window,
            context=request.context
        )
        
        processing_time = (time.time() - start_time) * 1000
        
        # Build response with enhanced confidence details
        response = QueryResponse(
            answer=result["answer"],
            confidence_score=result["confidence_score"],
            confidence_level=result.get("confidence_level", "unknown"),
            confidence_details=result.get("confidence_details"),
            supporting_evidence=result.get("supporting_evidence", []),
            evidence=result["evidence"],
            investigation_steps=result["investigation_steps"],
            root_causes=result.get("root_causes", []),
            correlations=result.get("correlations"),
            processing_time_ms=processing_time,
            timestamp=datetime.utcnow(),
            requires_user_input=result.get("requires_user_input", False),
            available_services=result.get("available_services")
        )
        
        logger.info(
            "Query processed successfully", 
            processing_time_ms=processing_time,
            confidence_level=result.get("confidence_level"),
            evidence_count=len(result.get("evidence", []))
        )
        return response
        
    except Exception as e:
        logger.error("Query processing failed", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.host, port=config.port)

