"""Root Cause Analysis engine."""
from typing import List, Dict, Any
import structlog

logger = structlog.get_logger()

class RCAEngine:
    """Root Cause Analysis engine."""
    
    async def identify_root_causes(
        self,
        investigation_steps: List[Any],
        evidence: List[Dict[str, Any]]
    ) -> List[str]:
        """Identify root causes from investigation steps and evidence."""
        root_causes = []
        
        # Analyze patterns across investigation steps
        error_patterns = []
        for step in investigation_steps:
            findings = step.get("findings", [])
            for finding in findings:
                if finding.get("significance") == "high":
                    error_patterns.append(finding)
        
        # Identify most common patterns as root causes
        if error_patterns:
            # Group by pattern type
            pattern_groups = {}
            for pattern in error_patterns:
                key = f"{pattern.get('field')}={pattern.get('pattern')}"
                if key not in pattern_groups:
                    pattern_groups[key] = 0
                pattern_groups[key] += pattern.get("count", 0)
            
            # Sort by frequency
            sorted_patterns = sorted(pattern_groups.items(), key=lambda x: x[1], reverse=True)
            root_causes = [pattern[0] for pattern in sorted_patterns[:3]]
        
        logger.info("Identified root causes", count=len(root_causes))
        return root_causes

