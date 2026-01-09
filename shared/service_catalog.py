"""Service catalog for understanding service relationships and observability."""
import json
import os
from typing import Dict, Any, List, Optional, Set
from pathlib import Path
import structlog

logger = structlog.get_logger()

class ServiceCatalog:
    """Service catalog for understanding service relationships and observability."""
    
    def __init__(self, catalog_path: Optional[str] = None):
        """Initialize service catalog from JSON file."""
        if catalog_path is None:
            # Default to service-catalog.json in the same directory
            current_dir = Path(__file__).parent
            catalog_path = current_dir / "service-catalog.json"
        
        self.catalog_path = catalog_path
        self.catalog_data: Dict[str, Any] = {}
        self.services: Dict[str, Dict[str, Any]] = {}
        self._load_catalog()
    
    def _load_catalog(self):
        """Load service catalog from JSON file."""
        try:
            with open(self.catalog_path, 'r') as f:
                self.catalog_data = json.load(f)
                self.services = self.catalog_data.get("services", {})
                logger.info("Loaded service catalog", services_count=len(self.services))
        except FileNotFoundError:
            logger.warning("Service catalog file not found", path=self.catalog_path)
            self.services = {}
        except json.JSONDecodeError as e:
            logger.error("Failed to parse service catalog JSON", error=str(e))
            self.services = {}
    
    def find_service(self, service_name: str) -> Optional[Dict[str, Any]]:
        """Find a service by name (case-insensitive, partial match)."""
        service_name_lower = service_name.lower()
        
        # Exact match
        if service_name in self.services:
            return self.services[service_name]
        
        # Case-insensitive match
        for service_id, service_data in self.services.items():
            if service_id.lower() == service_name_lower:
                return service_data
        
        # Partial match (service name contains the search term)
        for service_id, service_data in self.services.items():
            if service_name_lower in service_id.lower() or service_id.lower() in service_name_lower:
                return service_data
        
        return None
    
    def find_services_by_entities(self, entities: List[str]) -> List[Dict[str, Any]]:
        """Find services matching a list of entity names."""
        matched_services = []
        for entity in entities:
            service = self.find_service(entity)
            if service:
                matched_services.append(service)
        return matched_services
    
    def get_splunk_indexes(self, service_id: str) -> List[str]:
        """Get Splunk primary indexes for a service."""
        service = self.find_service(service_id)
        if not service:
            return []
        
        observability = service.get("observability", {})
        splunk = observability.get("splunk", {})
        return splunk.get("primary_indexes", [])
    
    def get_upstream_dependencies(self, service_id: str) -> List[Dict[str, Any]]:
        """Get upstream dependencies for a service."""
        service = self.find_service(service_id)
        if not service:
            return []
        
        dependencies = service.get("dependencies", {})
        return dependencies.get("upstream", [])
    
    def get_downstream_dependencies(self, service_id: str) -> List[str]:
        """Get downstream dependencies (services that depend on this service)."""
        downstream = []
        for service_id_check, service_data in self.services.items():
            dependencies = service_data.get("dependencies", {})
            upstream = dependencies.get("upstream", [])
            for dep in upstream:
                if isinstance(dep, dict):
                    dep_service = dep.get("service")
                else:
                    dep_service = dep
                
                if dep_service == service_id:
                    downstream.append(service_id_check)
        
        return downstream
    
    def get_dependency_chain(self, service_id: str, direction: str = "upstream") -> List[str]:
        """Get full dependency chain (upstream or downstream) for a service."""
        visited: Set[str] = set()
        chain: List[str] = []
        
        def traverse(current_service: str):
            if current_service in visited:
                return
            visited.add(current_service)
            chain.append(current_service)
            
            if direction == "upstream":
                deps = self.get_upstream_dependencies(current_service)
                for dep in deps:
                    if isinstance(dep, dict):
                        dep_service = dep.get("service")
                    else:
                        dep_service = dep
                    if dep_service:
                        traverse(dep_service)
            else:  # downstream
                deps = self.get_downstream_dependencies(current_service)
                for dep_service in deps:
                    traverse(dep_service)
        
        traverse(service_id)
        return chain
    
    def get_failure_modes(self, service_id: str, upstream_service: str) -> List[str]:
        """Get failure modes for a dependency relationship."""
        service = self.find_service(service_id)
        if not service:
            return []
        
        dependencies = service.get("dependencies", {})
        upstream = dependencies.get("upstream", [])
        
        for dep in upstream:
            if isinstance(dep, dict):
                dep_service = dep.get("service")
                if dep_service == upstream_service:
                    return dep.get("failure_modes", [])
        
        return []
    
    def get_criticality(self, service_id: str) -> Optional[str]:
        """Get criticality level of a service."""
        service = self.find_service(service_id)
        if not service:
            return None
        return service.get("criticality")
    
    def get_service_info(self, service_id: str) -> Dict[str, Any]:
        """Get comprehensive service information."""
        service = self.find_service(service_id)
        if not service:
            return {}
        
        return {
            "service_id": service.get("service_id"),
            "domain": service.get("domain"),
            "tier": service.get("tier"),
            "criticality": service.get("criticality"),
            "splunk_indexes": self.get_splunk_indexes(service_id),
            "upstream_dependencies": self.get_upstream_dependencies(service_id),
            "downstream_dependencies": self.get_downstream_dependencies(service_id),
            "dependency_chain_upstream": self.get_dependency_chain(service_id, "upstream"),
            "dependency_chain_downstream": self.get_dependency_chain(service_id, "downstream")
        }
