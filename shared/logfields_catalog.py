"""Log fields catalog to ground SPL generation and avoid hallucinated field names."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger()


class LogFieldsCatalog:
    """Loads `shared/logfields-catalog.json` and exposes known field names/descriptions."""

    def __init__(self, catalog_path: Optional[str] = None):
        if catalog_path is None:
            current_dir = Path(__file__).parent
            catalog_path = current_dir / "logfields-catalog.json"
        self.catalog_path = catalog_path
        self.catalog_data: Dict[str, Any] = {}
        self.logfields: Dict[str, str] = {}
        self._load_catalog()

    def _load_catalog(self) -> None:
        try:
            with open(self.catalog_path, "r") as f:
                self.catalog_data = json.load(f)
                self.logfields = self.catalog_data.get("logfields", {}) or {}
                logger.info("Loaded logfields catalog", fields_count=len(self.logfields))
        except FileNotFoundError:
            logger.warning("Logfields catalog file not found", path=self.catalog_path)
            self.logfields = {}
        except json.JSONDecodeError as e:
            logger.error("Failed to parse logfields catalog JSON", error=str(e))
            self.logfields = {}

    def field_names(self) -> List[str]:
        return sorted(self.logfields.keys())

    def description(self, field_name: str) -> Optional[str]:
        if not field_name:
            return None
        return self.logfields.get(field_name)

    def as_prompt_block(self) -> str:
        """Format as a compact text block for LLM prompts."""
        if not self.logfields:
            return ""
        lines = ["Known Log Fields (ground truth):"]
        for name in self.field_names():
            desc = self.logfields.get(name, "")
            if desc:
                lines.append(f"- {name}: {desc}")
            else:
                lines.append(f"- {name}")
        return "\n".join(lines) + "\n"

