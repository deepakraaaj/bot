import re
from typing import Any, Dict

from app.assistant.services.manifest_catalog import ManifestCatalog


class MutationUnderstandingService:
    def __init__(self):
        self.catalog = ManifestCatalog()

    def resolve_table(self, query: str, intent: Dict[str, Any]) -> str:
        q = (query or "").strip().lower()
        intent_table = str((intent or {}).get("table", "") or "").strip()

        if intent_table and intent_table in self.catalog.table_names():
            return intent_table

        # High-confidence disambiguation rules for schedule/task language.
        if re.search(r"\b(schedule|scheduler|scheduled)\b", q) and re.search(r"\btask\b", q):
            if "scheduler_task_details" in self.catalog.table_names():
                return "scheduler_task_details"

        if re.search(r"\b(schedule|scheduler|scheduled)\b", q):
            if "scheduler_details" in self.catalog.table_names():
                return "scheduler_details"

        # Fallback to manifest alias resolver.
        return self.catalog.resolve_table_from_query(q)
