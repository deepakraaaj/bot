import json
import logging
import re
import uuid
from typing import Any, AsyncGenerator, Dict, List, Optional

from langchain_core.messages import AIMessage, HumanMessage

from app.assistant.services.sql_builder_service import SQLBuilderService
from app.core import lifespan
from app.schemas.chat import ChatRequest
from app.services.cache import cache

logger = logging.getLogger(__name__)


class ChatService:
    @staticmethod
    def _history_key(session_id: str) -> str:
        return cache.generate_key("history", session_id)

    @staticmethod
    def _mutation_key(session_id: str) -> str:
        return cache.generate_key("mutation_state", session_id)

    async def _load_history(self, session_id: str) -> List[Dict[str, str]]:
        history = await cache.get(self._history_key(session_id))
        if isinstance(history, list):
            return [h for h in history if isinstance(h, dict) and "role" in h and "content" in h]
        return []

    async def _save_history(self, session_id: str, history: List[Dict[str, str]]) -> None:
        trimmed = history[-20:]
        await cache.set(self._history_key(session_id), trimmed, ttl=86400)

    async def _load_mutation_state(self, session_id: str) -> Optional[Dict[str, Any]]:
        state = await cache.get(self._mutation_key(session_id))
        return state if isinstance(state, dict) else None

    async def _save_mutation_state(self, session_id: str, state: Dict[str, Any]) -> None:
        await cache.set(self._mutation_key(session_id), state, ttl=3600)

    async def _clear_mutation_state(self, session_id: str) -> None:
        await cache.delete(self._mutation_key(session_id))

    @staticmethod
    def _next_missing_field(required_fields: List[str], collected_fields: Dict[str, Any]) -> str:
        for field in required_fields:
            if not str(collected_fields.get(field, "")).strip():
                return field
        return ""

    @staticmethod
    def _format_field_prompt(state: Dict[str, Any]) -> str:
        table = str(state.get("table", "record"))
        pending = str(state.get("pending_field", "")).strip()
        if not pending:
            return f"All required fields collected for {table}."

        descriptions = state.get("field_descriptions") or {}
        desc = str(descriptions.get(pending, "")).strip()
        details = f" ({desc})" if desc else ""
        return (
            f"Let's continue creating `{table}`. Please provide `{pending}`{details}. "
            "You can reply with just the value or `field=value`."
        )

    @staticmethod
    def _parse_user_field_updates(message: str, pending_field: str) -> Dict[str, str]:
        updates = SQLBuilderService.parse_kv_pairs(message)
        if updates:
            return updates

        text = (message or "").strip()
        if text and pending_field:
            lowered = text.lower()
            if any(
                token in lowered
                for token in [
                    "create ",
                    "insert ",
                    "add ",
                    "update ",
                    "show ",
                    "list ",
                    "count ",
                    "get ",
                    "find ",
                ]
            ):
                return {}
        if pending_field and text:
            return {pending_field: text}
        return {}

    @staticmethod
    def _build_final_response(
        session_id: str,
        message: str,
        status: str = "ok",
        workflow_payload: Optional[Dict[str, Any]] = None,
        sql_data: Optional[Dict[str, Any]] = None,
        token_usage: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return {
            "type": "result",
            "session_id": session_id,
            "message": str(message),
            "status": status,
            "labels": [],
            "workflow": workflow_payload,
            "sql": sql_data,
            "token_usage": token_usage,
            "provider_used": "tag_backend",
            "trace_id": "",
        }

    @staticmethod
    def _extract_invalid_column(error_message: str) -> str:
        match = re.search(r"for column '([^']+)'", str(error_message))
        return str(match.group(1)).strip() if match else ""

    @staticmethod
    def _extract_missing_required_column(error_message: str) -> str:
        match = re.search(r"Field '([^']+)' doesn't have a default value", str(error_message))
        return str(match.group(1)).strip() if match else ""

    async def _handle_active_mutation(
        self,
        request: ChatRequest,
        mutation_state: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        user_text = str(request.message or "").strip()
        lower_text = user_text.lower()

        if lower_text in {"cancel", "stop", "exit", "abort"}:
            await self._clear_mutation_state(request.session_id)
            return self._build_final_response(
                request.session_id,
                "Mutation workflow cancelled. Ask a new project request when ready.",
            )

        required_fields = [str(x) for x in mutation_state.get("required_fields", [])]
        collected_fields = dict(mutation_state.get("collected_fields") or {})
        pending_field = str(mutation_state.get("pending_field", "")).strip()

        updates = self._parse_user_field_updates(user_text, pending_field)
        accepted_updates = 0
        for key, value in updates.items():
            if key in required_fields and str(value).strip():
                collected_fields[key] = str(value).strip()
                accepted_updates += 1

        if accepted_updates == 0:
            prompt = self._format_field_prompt(mutation_state)
            return self._build_final_response(
                request.session_id,
                prompt,
                workflow_payload={
                    "workflow_id": "mutation_menu",
                    "state": mutation_state.get("state", "collect_mutation"),
                    "completed": False,
                    "next_field": mutation_state.get("pending_field", ""),
                    "collected_data": {
                        "operation": mutation_state.get("operation", "insert"),
                        "table": mutation_state.get("table", ""),
                        "required_fields": required_fields,
                        "collected_fields": collected_fields,
                    },
                },
            )

        pending_field = self._next_missing_field(required_fields, collected_fields)
        mutation_state["collected_fields"] = collected_fields
        mutation_state["pending_field"] = pending_field

        if pending_field:
            await self._save_mutation_state(request.session_id, mutation_state)
            return self._build_final_response(
                request.session_id,
                self._format_field_prompt(mutation_state),
                workflow_payload={
                    "workflow_id": "mutation_menu",
                    "state": mutation_state.get("state", "collect_mutation"),
                    "completed": False,
                    "next_field": pending_field,
                    "collected_data": {
                        "operation": mutation_state.get("operation", "insert"),
                        "table": mutation_state.get("table", ""),
                        "required_fields": required_fields,
                        "collected_fields": collected_fields,
                    },
                },
            )

        await self._clear_mutation_state(request.session_id)
        if request.metadata is None:
            request.metadata = {}

        request.metadata["mutation_context"] = {
            "operation": mutation_state.get("operation", "insert"),
            "table": mutation_state.get("table", ""),
            "fields": collected_fields,
        }
        return None

    async def start_session(self):
        return {"session_id": str(uuid.uuid4()), "message": "Session started"}

    async def generate_chat_stream(self, request: ChatRequest) -> AsyncGenerator[str, None]:
        workflow = lifespan.workflow

        if not workflow:
            yield json.dumps({"type": "error", "message": "Workflow not initialized"}) + "\n"
            return

        if request.metadata is None:
            request.metadata = {}
        request.metadata["session_id"] = request.session_id

        history_payload = await self._load_history(request.session_id)
        mutation_state = await self._load_mutation_state(request.session_id)

        if mutation_state:
            active_mutation_result = await self._handle_active_mutation(request, mutation_state)
            if active_mutation_result is not None:
                message = str(active_mutation_result.get("message", ""))
                yield json.dumps({"type": "token", "content": message}) + "\n"
                history_payload.extend(
                    [
                        {"role": "user", "content": request.message},
                        {"role": "assistant", "content": message},
                    ]
                )
                await self._save_history(request.session_id, history_payload)
                yield json.dumps(active_mutation_result, default=str) + "\n"
                return

        use_cache = mutation_state is None and "mutation_context" not in request.metadata
        cache_key = cache.generate_key("chat", request.session_id, len(history_payload), request.message)
        if use_cache:
            cached_response = await cache.get(cache_key)
            if cached_response:
                logger.info("Cache HIT for key: %s", cache_key)
                if cached_response.get("sql"):
                    cached_response["sql"]["cached"] = True

                cached_message = cached_response.get("message")
                if cached_message:
                    yield json.dumps({"type": "token", "content": str(cached_message)}) + "\n"
                else:
                    yield json.dumps({"type": "token", "content": "I processed your previous request from cache."}) + "\n"

                history_payload.extend(
                    [
                        {"role": "user", "content": request.message},
                        {"role": "assistant", "content": str(cached_message or "")},
                    ]
                )
                await self._save_history(request.session_id, history_payload)

                yield json.dumps(cached_response, default=str) + "\n"
                return

        logger.info("Cache MISS for key: %s", cache_key)

        try:
            prior_messages: List[Any] = []
            for item in history_payload:
                role = item.get("role")
                content = str(item.get("content", ""))
                if not content:
                    continue
                if role == "assistant":
                    prior_messages.append(AIMessage(content=content))
                else:
                    prior_messages.append(HumanMessage(content=content))

            logger.info("Invoking workflow with session_id: %s, metadata: %s", request.session_id, request.metadata)
            inputs = {
                "messages": prior_messages + [HumanMessage(content=request.message)],
                "metadata": request.metadata,
                "retry_count": 0,
            }
            result = await workflow.ainvoke(inputs)

            final_message = result["messages"][-1].content or ""
            executed_sql = result.get("sql_query", "")
            error = result.get("error", None)
            workflow_payload = result.get("workflow_payload", None)

            mutation_context = request.metadata.get("mutation_context") or {}
            if error and mutation_context:
                invalid_column = self._extract_invalid_column(str(error))
                missing_required_column = self._extract_missing_required_column(str(error))

                if invalid_column or missing_required_column:
                    target_column = invalid_column or missing_required_column
                    mutation_fields = dict(mutation_context.get("fields") or {})
                    if invalid_column:
                        mutation_fields.pop(target_column, None)

                    existing_required = [str(x) for x in (mutation_context.get("fields") or {}).keys()]
                    if target_column not in existing_required:
                        existing_required.append(target_column)

                    recovery_state = {
                        "workflow_id": "mutation_menu",
                        "state": f"collect_{mutation_context.get('operation', 'insert')}_{mutation_context.get('table', '')}",
                        "operation": str(mutation_context.get("operation", "insert")),
                        "table": str(mutation_context.get("table", "")),
                        "required_fields": existing_required,
                        "collected_fields": mutation_fields,
                        "pending_field": target_column,
                        "field_descriptions": {},
                    }
                    await self._save_mutation_state(request.session_id, recovery_state)
                    workflow_payload = {
                        "workflow_id": "mutation_menu",
                        "state": recovery_state["state"],
                        "completed": False,
                        "next_field": target_column,
                        "collected_data": {
                            "operation": recovery_state["operation"],
                            "table": recovery_state["table"],
                            "required_fields": recovery_state["required_fields"],
                            "collected_fields": recovery_state["collected_fields"],
                        },
                    }
                    if invalid_column:
                        final_message = (
                            f"I could not save because `{target_column}` had an invalid value. "
                            f"Please provide a valid `{target_column}`."
                        )
                    else:
                        final_message = (
                            f"The database requires `{target_column}` for this record. "
                            f"Please provide `{target_column}`."
                        )
                    error = None
                    executed_sql = ""

            if workflow_payload and not bool(workflow_payload.get("completed")):
                collected_data = workflow_payload.get("collected_data") or {}
                required_fields = [str(x) for x in collected_data.get("required_fields", [])]
                collected_fields = dict(collected_data.get("collected_fields") or {})
                next_field = str(workflow_payload.get("next_field", "")).strip() or self._next_missing_field(
                    required_fields, collected_fields
                )
                ui_fields = (workflow_payload.get("ui") or {}).get("fields") or []
                field_descriptions = {
                    str(f.get("id")): str(f.get("description", ""))
                    for f in ui_fields
                    if isinstance(f, dict) and str(f.get("id", "")).strip()
                }

                state = {
                    "workflow_id": str(workflow_payload.get("workflow_id", "mutation_menu")),
                    "state": str(workflow_payload.get("state", "collect_mutation")),
                    "operation": str(collected_data.get("operation", "insert")),
                    "table": str(collected_data.get("table", "")),
                    "required_fields": required_fields,
                    "collected_fields": collected_fields,
                    "pending_field": next_field,
                    "field_descriptions": field_descriptions,
                }
                await self._save_mutation_state(request.session_id, state)

                if next_field:
                    final_message = self._format_field_prompt(state)

            yield json.dumps({"type": "token", "content": str(final_message)}) + "\n"

            status_code = "error" if error else "ok"
            sql_data = None
            if executed_sql and executed_sql != "SKIP":
                sql_data = {
                    "ran": True,
                    "cached": result.get("from_cache", False),
                    "query": executed_sql,
                    "row_count": result.get("row_count"),
                    "rows_preview": result.get("rows_preview"),
                }

            final_response = self._build_final_response(
                request.session_id,
                str(final_message),
                status=status_code,
                workflow_payload=workflow_payload,
                sql_data=sql_data,
                token_usage=result.get("token_usage", None),
            )

            if status_code == "ok" and use_cache and not workflow_payload:
                await cache.set(cache_key, final_response, ttl=3600)

            history_payload.extend(
                [
                    {"role": "user", "content": request.message},
                    {"role": "assistant", "content": str(final_message)},
                ]
            )
            await self._save_history(request.session_id, history_payload)

            yield json.dumps(final_response, default=str) + "\n"

        except Exception as exc:
            logger.error("Workflow execution failed: %s", exc)
            yield json.dumps({"type": "error", "message": str(exc)}) + "\n"
