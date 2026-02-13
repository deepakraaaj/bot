import os
import re
from typing import Dict

from langchain_core.messages import AIMessage
from langchain_openai import ChatOpenAI

from app.config import get_settings

settings = get_settings()


class ChatNode:
    DOMAIN_HINTS = (
        "task",
        "tasks",
        "asset",
        "assets",
        "facility",
        "facilities",
        "user",
        "users",
        "company",
        "checklist",
        "scheduler",
        "sql",
        "database",
        "tag",
        "session",
        "query",
        "insert",
        "update",
        "select",
        "count",
    )

    def __init__(self):
        model_name = os.getenv("LLM_MODEL", settings.LLM_MODEL)
        self.llm = ChatOpenAI(
            api_key=settings.LLM_API_KEY,
            base_url=settings.LLM_BASE_URL,
            model=model_name,
            temperature=0.4,
        )

    @staticmethod
    def _capabilities_message() -> str:
        return (
            "I only support TAG application tasks. I can help you with:\n"
            "1. Querying project data (tasks, assets, facilities, users, companies).\n"
            "2. Counting/listing records from the database.\n"
            "3. Creating or updating records with valid fields.\n"
            "4. Explaining schema-aware errors and required input fields."
        )

    def _is_domain_query(self, query: str) -> bool:
        q = (query or "").lower()
        return any(hint in q for hint in self.DOMAIN_HINTS)

    async def run(self, state: Dict) -> Dict:
        messages = state.get("messages", [])
        query = messages[-1].content if messages else ""
        q = str(query or "").strip()

        if re.search(r"\b(what can you do|how can you help|capabilities|help)\b", q, flags=re.IGNORECASE):
            return {"messages": [AIMessage(content=self._capabilities_message())], "token_usage": {}}

        if not self._is_domain_query(q):
            return {
                "messages": [
                    AIMessage(
                        content=(
                            "I can only help with this TAG project and its database operations. "
                            "Ask about tasks, assets, facilities, users, schedules, or SQL-backed actions."
                        )
                    )
                ],
                "token_usage": {},
            }

        prompt = (
            "You are the TAG backend assistant. Keep responses strictly limited to this project's "
            "application domain: tasks, assets, facilities, users, companies, schedules, and DB actions. "
            "Do not offer generic assistant abilities outside the project.\n"
            f"User: {q}"
        )
        response = await self.llm.ainvoke(prompt)
        usage = response.response_metadata.get("token_usage", {})
        return {"messages": [response], "token_usage": usage}
