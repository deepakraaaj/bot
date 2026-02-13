import asyncio
import json

from app.core import lifespan
from app.services.chat_service import ChatService
from app.schemas.chat import ChatRequest


def test_chat_service_returns_error_when_workflow_not_initialized():
    svc = ChatService()
    lifespan.workflow = None
    req = ChatRequest(session_id="s1", message="hello")

    async def collect_first_line():
        async for line in svc.generate_chat_stream(req):
            return line
        return ""

    line = asyncio.run(collect_first_line())
    payload = json.loads(line)
    assert payload["type"] == "error"
    assert "workflow not initialized" in payload["message"].lower()
