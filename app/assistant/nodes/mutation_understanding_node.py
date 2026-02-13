from typing import Dict

from app.assistant.services.mutation_understanding_service import MutationUnderstandingService


class MutationUnderstandingNode:
    def __init__(self):
        self.service = MutationUnderstandingService()

    async def run(self, state: Dict) -> Dict:
        messages = state.get("messages", [])
        query = str(messages[-1].content) if messages else ""

        intent = dict(state.get("intent") or {})
        operation = str(intent.get("operation", "select") or "select").lower()

        if operation not in {"insert", "update"}:
            return {"intent": intent}

        resolved_table = self.service.resolve_table(query, intent)
        if resolved_table:
            intent["table"] = resolved_table

        return {"intent": intent}
