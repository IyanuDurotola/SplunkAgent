"""RAG retrieval logic for past incidents."""
from typing import List, Dict, Any, Tuple
from datetime import datetime
import structlog

from memory.vector_store import VectorStore
from memory.config import MemoryConfig

logger = structlog.get_logger()

class MemoryRetrieval:
    """RAG retrieval logic for past incidents."""
    
    def __init__(self):
        self.config = MemoryConfig()
        self.vector_store = VectorStore(self.config)
        self._initialized = False
    
    async def _ensure_initialized(self):
        """Ensure vector store is initialized."""
        if not self._initialized:
            await self.vector_store.initialize()
            self._initialized = True
    
    async def retrieve_relevant_incidents(
        self,
        question: str,
        time_window: Tuple[datetime, datetime]
    ) -> List[Dict[str, Any]]:
        """Retrieve relevant past incidents for the question."""
        await self._ensure_initialized()
        
        # Search vector store for similar incidents
        similar_incidents = await self.vector_store.search_similar(
            query=question,
            top_k=self.config.top_k_results
        )
        
        # Filter by time window if needed
        filtered_incidents = []
        for incident in similar_incidents:
            # Check metadata timestamp or created_at
            incident_time = None
            if 'timestamp' in incident.get('metadata', {}):
                incident_time = datetime.fromisoformat(incident['metadata']['timestamp'])
            elif incident.get('created_at'):
                incident_time = datetime.fromisoformat(incident['created_at'])
            
            if incident_time:
                if time_window[0] <= incident_time <= time_window[1]:
                    filtered_incidents.append(incident)
            else:
                # Include if no timestamp available
                filtered_incidents.append(incident)
        
        logger.info("Retrieved relevant incidents", count=len(filtered_incidents))
        return filtered_incidents
    
    async def store_investigation(
        self,
        question: str,
        answer: str,
        evidence: List[Dict[str, Any]],
        investigation_steps: List[Any]
    ):
        """Store completed investigation in memory."""
        await self._ensure_initialized()
        
        metadata = {
            "investigation_steps_count": len(investigation_steps)
        }
        
        await self.vector_store.add_incident(
            question=question,
            answer=answer,
            evidence=evidence,
            metadata=metadata
        )
        logger.info("Stored investigation in memory")

