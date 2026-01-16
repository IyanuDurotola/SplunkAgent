"""Splunk query generator with LLM and guardrails."""
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
import structlog
import re

from query_generator.config import QueryGeneratorConfig
from query_generator.guardrails import QueryGuardrails
from query_generator.llm_client import LLMClient
from splunk_integration.client import SplunkClient

logger = structlog.get_logger()

class SplunkQueryGenerator:
    """Splunk query generator with LLM and guardrails."""
    
    def __init__(self):
        self.config = QueryGeneratorConfig()
        self.guardrails = QueryGuardrails()
        self.llm_client = LLMClient(self.config)
        self.splunk_client = SplunkClient()
    
    async def generate_query(
        self,
        hypothesis: str,
        question: str,
        time_window: Tuple[datetime, datetime],
        historical_context: Optional[list] = None,
        intent: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate and validate SPL query for a hypothesis."""
        # Format historical context
        historical_examples = None
        if historical_context:
            historical_examples = "\n".join([
                f"- {inc.get('document', '')[:200]}" for inc in historical_context[:3]
            ])
        
        # Generate query using LLM
        query = await self.llm_client.generate_spl_query(
            hypothesis=hypothesis,
            question=question,
            historical_examples=historical_examples,
            intent=intent
        )
        
        # Validate query
        time_params = {}
        if self.config.enable_guardrails:
            self.guardrails.validate_query(query)
            query, time_params = self.guardrails.constrain_query(query, time_window)

        # Remove hallucinated/ungrounded filters that frequently cause misses (e.g., source="main_app_log.log").
        query = self._remove_ungrounded_source_filters(query, question)

        # If the user mentioned a known catalog app, enforce an `app=...` filter in the base search clause.
        if intent and isinstance(intent, dict):
            apps = intent.get("apps", []) or []
            if apps:
                query = self._enforce_app_filters(query, apps)

        # Deterministic enforcement: if user asks for "origin"/"first occurrence", ensure we return the earliest event.
        if intent and isinstance(intent, dict):
            query_patterns = intent.get("query_patterns", []) or []
            if any(p in query_patterns for p in ["origin", "first_occurrence"]):
                query = self._enforce_first_occurrence(query)
        
        logger.info("Generated validated SPL query", query=query[:100])
        return query, time_params

    def _remove_ungrounded_source_filters(self, query: str, question: str) -> str:
        """
        Remove `source=...` / `sourcetype=...` filters unless the user explicitly mentioned the value.

        Rationale: these fields are easy for an LLM to hallucinate and they often exclude all real events.
        """
        q = (query or "").strip()
        if not q:
            return q

        question_lower = (question or "").lower()

        head, sep, tail = q.partition("|")
        head_str = head.strip()
        if not head_str:
            return q

        # Only clean the pre-pipe search clause; commands later in the pipeline are less likely to be literal filters.
        pattern = re.compile(
            r"(?P<lead>\s*)\b(?P<field>source|sourcetype)\s*=\s*(?P<val>\"[^\"]*\"|'[^']*'|[^\s|]+)",
            re.IGNORECASE,
        )

        def _repl(m: re.Match) -> str:
            val = (m.group("val") or "").strip().strip("\"'").strip()
            if val and val.lower() in question_lower:
                return m.group(0)
            # drop the entire filter (including leading whitespace)
            return ""

        cleaned_head = pattern.sub(_repl, head_str)
        cleaned_head = self._cleanup_dangling_boolean_operators(cleaned_head)
        cleaned_head = re.sub(r"\s+", " ", cleaned_head).strip()

        if not cleaned_head:
            cleaned_head = "index=*"

        if sep:
            # Preserve pipeline portion (tail already excludes the first pipe)
            return f"{cleaned_head} | {tail.strip()}"
        return cleaned_head

    def _cleanup_dangling_boolean_operators(self, search_clause: str) -> str:
        """
        Clean up common SPL parse breakers like:
        - '( OR)' / '(AND )' after filters are removed
        - leading 'OR'/'AND'
        - trailing 'OR'/'AND'
        - empty parentheses
        """
        s = (search_clause or "").strip()
        if not s:
            return s

        # Remove "( OR )" / "( AND )" and empty parens
        s = re.sub(r"\(\s*(OR|AND)\s*\)", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\(\s*\)", "", s)

        # Remove leading dangling operator(s)
        s = re.sub(r"^\s*(OR|AND)\b", "", s, flags=re.IGNORECASE).strip()
        # Remove trailing dangling operator(s)
        s = re.sub(r"\b(OR|AND)\s*$", "", s, flags=re.IGNORECASE).strip()

        # If we end up with a lone 'search' keyword, drop it (Splunk doesn't need it here)
        if s.lower() == "search":
            return ""

        return s

    def _enforce_app_filters(self, query: str, apps: list) -> str:
        """Ensure base search clause includes an app filter for catalog-grounded apps."""
        q = (query or "").strip()
        if not q:
            return q

        # If query already filters on app, don't add another.
        if re.search(r"\bapp\s*=", q, flags=re.IGNORECASE):
            return q

        # Only allow simple app tokens (from catalog).
        clean_apps = []
        for a in apps:
            a_str = str(a).strip()
            if re.fullmatch(r"[A-Za-z0-9_.:-]+", a_str):
                clean_apps.append(a_str)
        if not clean_apps:
            return q

        app_filter = ""
        if len(clean_apps) == 1:
            app_filter = f'app="{clean_apps[0]}"'
        else:
            ors = " OR ".join(f'app="{a}"' for a in clean_apps)
            app_filter = f"({ors})"

        head, sep, tail = q.partition("|")
        head_str = head.strip()

        # Avoid "search" keyword duplication in base clause
        if head_str.lower().startswith("search "):
            head_str = head_str[7:].strip()

        # Append app filter to the base clause.
        if head_str:
            head_str = f"{head_str} {app_filter}"
        else:
            head_str = app_filter

        head_str = self._cleanup_dangling_boolean_operators(head_str)
        head_str = re.sub(r"\s+", " ", head_str).strip()

        if sep:
            return f"{head_str} | {tail.strip()}"
        return head_str

    def _enforce_first_occurrence(self, query: str) -> str:
        """Ensure SPL query returns earliest matching event.

        Prefer the log field `time` (per logfields catalog). Avoid forcing `eval coalesce(...)`
        because it can be brittle across deployments/field types.
        """
        q = (query or "").strip()
        if not q:
            return q

        q_lower = q.lower()
        # If the query aggregates, "first occurrence" semantics are unclear; don't force head(1).
        if any(x in q_lower for x in ["| stats", "| timechart", "| chart", "| top", "| rare", "| dedup"]):
            return q

        # Avoid duplicate enforcement if query already has head(1).
        if "| head 1" in q_lower or "| head 0" in q_lower:
            return q

        # Prefer the log payload field `time` for ordering.
        return f"{q} | sort 0 time | head 1"
    
    async def execute_query(self, query: str, time_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute SPL query through Splunk API.
        
        Args:
            query: SPL query string (without earliest/latest - those are passed as kwargs)
            time_params: Optional dict with 'earliest_time' and 'latest_time' for Splunk API
        """
        # Pass time parameters as kwargs to Splunk API
        kwargs = time_params or {}
        q = (query or "").strip()
        # Avoid "search search ..." when the LLM already returns a leading `search`
        if q.lower().startswith("search "):
            results = await self.splunk_client.search(q, **kwargs)
        else:
            results = await self.splunk_client.search(f"search {q}", **kwargs)
        logger.info(
            "Executed SPL query",
            results_count=len(results.get("results", [])),
            earliest_time=kwargs.get("earliest_time"),
            latest_time=kwargs.get("latest_time"),
        )
        return results

