import asyncio
from types import SimpleNamespace
from typing import Any, cast

import app.assistant.services.router_service as router_module
from app.assistant.services.router_service import RouterService


def _make_service() -> RouterService:
    svc = object.__new__(RouterService)
    svc.llm = cast(Any, object())
    return svc


def test_router_fallback_routes_sql_for_project_data_keywords():
    assert RouterService.fallback("how many tasks are there") == "SQL"
    assert RouterService.fallback("show user list") == "SQL"


def test_router_fallback_routes_chat_for_non_data_prompt():
    assert RouterService.fallback("hello there") == "CHAT"


def test_router_route_uses_model_json(monkeypatch):
    svc = _make_service()

    async def fake_ainvoke_with_retry(*args, **kwargs):
        return SimpleNamespace(content='{"route":"SQL"}')

    monkeypatch.setattr(router_module, "ainvoke_with_retry", fake_ainvoke_with_retry)

    route = asyncio.run(svc.route("count tasks"))
    assert route == "SQL"


def test_router_route_falls_back_when_model_fails(monkeypatch):
    svc = _make_service()

    async def fake_ainvoke_with_retry(*args, **kwargs):
        raise RuntimeError("llm down")

    monkeypatch.setattr(router_module, "ainvoke_with_retry", fake_ainvoke_with_retry)

    route = asyncio.run(svc.route("hello"))
    assert route == "CHAT"
