"""Enhanced Root Cause Analysis engine with dependency-aware tracing."""
from typing import List, Dict, Any, Optional
from datetime import datetime
import structlog

from shared.service_catalog import ServiceCatalog

logger = structlog.get_logger()


class RCAEngine:
    """Enhanced Root Cause Analysis engine with multi-hop dependency tracing."""
    
    def __init__(self, service_catalog: Optional[ServiceCatalog] = None):
        self.service_catalog = service_catalog or ServiceCatalog()
    
    async def identify_root_causes(
        self,
        investigation_steps: List[Any],
        evidence: List[Dict[str, Any]],
        intent: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Identify root causes using multi-factor analysis."""
        # Step 1: Extract error patterns
        error_patterns = self._extract_error_patterns(investigation_steps)
        
        # Step 2: Build simplified timeline (service + timestamp only)
        timeline = self._build_event_timeline(investigation_steps)
        
        # Step 3: Identify cascade patterns
        cascade_analysis = self._analyze_cascade_patterns(timeline)
        
        # Step 4: Find the earliest error (potential origin)
        origin_analysis = self._find_error_origin(timeline, intent)
        
        # Step 5: Correlate with service dependencies
        dependency_analysis = self._analyze_dependency_chain(error_patterns, cascade_analysis)
        
        # Step 6: Rank and build root causes
        root_causes = self._rank_root_causes(
            error_patterns,
            cascade_analysis,
            origin_analysis,
            dependency_analysis
        )
        
        logger.info("Identified root causes", 
                   count=len(root_causes),
                   top_cause=root_causes[0] if root_causes else None)
        return root_causes
    
    def _extract_error_patterns(self, investigation_steps: List[Any]) -> List[Dict[str, Any]]:
        """Extract error patterns from investigation steps."""
        patterns = []
        
        for step in investigation_steps:
            findings = step.get("findings", [])
            results = step.get("results", {})
            
            # Extract from findings
            for finding in findings:
                if finding.get("significance") in ["high", "medium"]:
                    pattern = {
                        "type": finding.get("field", "unknown"),
                        "value": finding.get("pattern", ""),
                        "count": finding.get("count", 0),
                        "significance": finding.get("significance"),
                        "service": self._extract_service(finding),
                        "timestamp": finding.get("timestamp"),
                        "error_category": self._categorize_error(finding)
                    }
                    patterns.append(pattern)
            
            # Extract from raw results
            if results and isinstance(results, dict):
                for result in results.get("results", []):
                    if self._is_error_result(result):
                        patterns.append({
                            "type": "error_log",
                            "value": result.get("_raw", result.get("message", "")),
                            "count": 1,
                            "significance": "high",
                            "service": result.get("index", result.get("source", "")),
                            "timestamp": result.get("_time"),
                            "error_category": self._categorize_error(result)
                        })
        
        return patterns
    
    def _build_event_timeline(self, investigation_steps: List[Any]) -> List[Dict[str, Any]]:
        """Build simplified timeline (service + timestamp only)."""
        events = []
        
        for step in investigation_steps:
            results = step.get("results", {})
            if isinstance(results, dict):
                for result in results.get("results", []):
                    timestamp = result.get("_time")
                    if timestamp and self._is_error_result(result):
                        events.append({
                            "timestamp": timestamp,
                            "service": self._extract_service(result)
                        })
        
        # Sort by timestamp
        events.sort(key=lambda x: x.get("timestamp", ""), reverse=False)
        return events
    
    def _analyze_cascade_patterns(self, timeline: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze if errors cascaded through service dependencies (simplified)."""
        cascade_info = {
            "detected": False,
            "chain": [],
            "origin_service": None
        }
        
        if not timeline:
            return cascade_info
        
        # Group errors by service
        service_errors = {}
        for event in timeline:
            service = event.get("service", "unknown")
            if service not in service_errors:
                service_errors[service] = []
            service_errors[service].append(event)
        
        # Check if errors follow dependency chain
        for service, errors in service_errors.items():
            if not errors:
                continue
            
            first_error = errors[0]
            downstream_deps = self.service_catalog.get_downstream_dependencies(service)
            
            # Check if downstream services had errors after this service
            for dep_service in downstream_deps:
                if dep_service in service_errors:
                    dep_errors = service_errors[dep_service]
                    for dep_error in dep_errors:
                        if dep_error.get("timestamp", "") > first_error.get("timestamp", ""):
                            cascade_info["detected"] = True
                            cascade_info["origin_service"] = service
                            cascade_info["chain"].append({
                                "from": service,
                                "to": dep_service
                            })
                            break
        
        return cascade_info
    
    def _find_error_origin(
        self, 
        timeline: List[Dict[str, Any]],
        intent: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Find the earliest error that could be the origin."""
        origin = {
            "found": False,
            "service": None,
            "timestamp": None,
            "confidence": 0.0
        }
        
        if not timeline:
            return origin
        
        # Find earliest error event
        first_event = timeline[0]
        origin = {
            "found": True,
            "service": first_event.get("service"),
            "timestamp": first_event.get("timestamp"),
            "confidence": 0.7
        }
        
        # Boost confidence if it's from an upstream service
        if self._is_upstream_service(origin["service"], intent):
            origin["confidence"] = 0.85
        
        return origin
    
    def _analyze_dependency_chain(
        self,
        error_patterns: List[Dict[str, Any]],
        cascade_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze which dependencies are involved in the error."""
        dependency_info = {
            "affected_services": [],
            "upstream_failures": [],
            "downstream_impact": []
        }
        
        # Extract unique services from error patterns
        services_with_errors = set()
        for pattern in error_patterns:
            service = pattern.get("service")
            if service:
                services_with_errors.add(service)
        
        for service in services_with_errors:
            dependency_info["affected_services"].append(service)
            
            # Check upstream
            upstream = self.service_catalog.get_upstream_dependencies(service)
            for dep in upstream:
                dep_service = dep.get("service") if isinstance(dep, dict) else dep
                if dep_service in services_with_errors:
                    dependency_info["upstream_failures"].append({
                        "service": dep_service,
                        "affected": service,
                        "failure_modes": dep.get("failure_modes", []) if isinstance(dep, dict) else []
                    })
            
            # Check downstream
            downstream = self.service_catalog.get_downstream_dependencies(service)
            for dep_service in downstream:
                if dep_service in services_with_errors:
                    dependency_info["downstream_impact"].append({
                        "origin": service,
                        "affected": dep_service
                    })
        
        return dependency_info
    
    def _rank_root_causes(
        self,
        error_patterns: List[Dict[str, Any]],
        cascade_analysis: Dict[str, Any],
        origin_analysis: Dict[str, Any],
        dependency_analysis: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Rank and build final root cause list with confidence scores."""
        root_causes = []
        
        # Highest priority: Cascade origin
        if cascade_analysis.get("detected") and cascade_analysis.get("origin_service"):
            origin_service = cascade_analysis["origin_service"]
            root_causes.append({
                "description": f"Error cascade originated from {origin_service}",
                "service": origin_service,
                "confidence": 0.9,
                "type": "cascade_origin",
                "evidence": {
                    "cascade_chain": cascade_analysis.get("chain", []),
                    "affected_services": [c.get("to") for c in cascade_analysis.get("chain", [])]
                }
            })
        
        # Second priority: Upstream failures
        for failure in dependency_analysis.get("upstream_failures", []):
            root_causes.append({
                "description": f"Upstream service {failure['service']} failed, affecting {failure['affected']}",
                "service": failure["service"],
                "confidence": 0.85,
                "type": "upstream_failure",
                "evidence": {
                    "failure_modes": failure.get("failure_modes", []),
                    "downstream_affected": failure["affected"]
                }
            })
        
        # Third priority: Origin analysis
        if origin_analysis.get("found") and not cascade_analysis.get("detected"):
            root_causes.append({
                "description": f"Earliest error detected in {origin_analysis['service']}",
                "service": origin_analysis["service"],
                "confidence": origin_analysis["confidence"],
                "type": "earliest_error",
                "evidence": {
                    "timestamp": origin_analysis["timestamp"]
                }
            })
        
        # Fourth priority: High-frequency error patterns
        pattern_counts = {}
        for pattern in error_patterns:
            key = f"{pattern.get('service', 'unknown')}:{pattern.get('error_category', 'unknown')}"
            if key not in pattern_counts:
                pattern_counts[key] = {
                    "service": pattern.get("service"),
                    "category": pattern.get("error_category"),
                    "count": 0,
                    "samples": []
                }
            pattern_counts[key]["count"] += pattern.get("count", 1)
            if len(pattern_counts[key]["samples"]) < 3:
                pattern_counts[key]["samples"].append(pattern.get("value", "")[:200])
        
        # Sort by count and add top patterns
        sorted_patterns = sorted(pattern_counts.items(), key=lambda x: x[1]["count"], reverse=True)
        for key, info in sorted_patterns[:3]:
            if info["count"] > 0:
                root_causes.append({
                    "description": f"Frequent {info['category']} errors in {info['service']} ({info['count']} occurrences)",
                    "service": info["service"],
                    "confidence": min(0.5 + (info["count"] * 0.05), 0.8),
                    "type": "frequent_error",
                    "evidence": {
                        "error_count": info["count"],
                        "samples": info["samples"]
                    }
                })
        
        # Sort by confidence and deduplicate
        root_causes.sort(key=lambda x: x["confidence"], reverse=True)
        seen_services = set()
        unique_causes = []
        for cause in root_causes:
            service = cause.get("service")
            if service not in seen_services:
                seen_services.add(service)
                unique_causes.append(cause)
        
        return unique_causes[:5]
    
    # Helper methods (consolidated)
    def _extract_service(self, item: Dict[str, Any]) -> str:
        """Extract service name from finding or result."""
        index = item.get("index", item.get("source", ""))
        return self._index_to_service(index)
    
    def _index_to_service(self, index: str) -> str:
        """Map Splunk index to service name using catalog."""
        for service_id, service_data in self.service_catalog.services.items():
            indexes = self.service_catalog.get_splunk_indexes(service_id)
            if index.lower() in [idx.lower() for idx in indexes]:
                return service_id
        return index
    
    def _categorize_error(self, item: Dict[str, Any]) -> str:
        """Categorize error from finding or result (consolidated)."""
        # Extract text from either finding or result
        text = str(item.get("pattern", item.get("value", item.get("_raw", item.get("message", ""))))).lower()
        
        if any(x in text for x in ["timeout", "timed out", "deadline exceeded"]):
            return "timeout"
        if any(x in text for x in ["connection refused", "connect error", "network"]):
            return "connection_error"
        if any(x in text for x in ["500", "502", "503", "504", "5xx", "internal server"]):
            return "server_error_5xx"
        if any(x in text for x in ["404", "not found"]):
            return "not_found"
        if any(x in text for x in ["401", "403", "unauthorized", "forbidden", "auth"]):
            return "auth_error"
        if any(x in text for x in ["null", "undefined", "none", "nullpointer"]):
            return "null_reference"
        if any(x in text for x in ["exception", "error", "failed", "failure"]):
            return "general_error"
        
        return "unknown"
    
    def _is_error_result(self, result: Dict[str, Any]) -> bool:
        """Check if a result represents an error."""
        raw = str(result.get("_raw", "")).lower()
        level = str(result.get("level", result.get("log_level", ""))).lower()
        
        return (
            level in ["error", "fatal", "critical"] or
            any(x in raw for x in ["error", "exception", "failed", "failure", "timeout"])
        )
    
    def _is_upstream_service(self, service: str, intent: Optional[Dict[str, Any]]) -> bool:
        """Check if service is upstream of the entities in intent."""
        if not intent or not service:
            return False
        
        entities = intent.get("entities", [])
        for entity in entities:
            upstream = self.service_catalog.get_upstream_dependencies(entity)
            upstream_names = [
                dep.get("service") if isinstance(dep, dict) else dep 
                for dep in upstream
            ]
            if service in upstream_names:
                return True
        return False
