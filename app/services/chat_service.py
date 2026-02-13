import json
import logging
import re
import uuid
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

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
    def _remaining_fields(required_fields: List[str], collected_fields: Dict[str, Any]) -> List[str]:
        return [f for f in required_fields if not str(collected_fields.get(f, "")).strip()]

    @staticmethod
    def _input_kind(field_name: str) -> str:
        name = str(field_name).lower()
        if "date" in name:
            return "date"
        if re.search(r"(^id$|_id$|count|qty|quantity|amount|price|occurrence|number|ref_no)", name):
            return "numeric"
        if name in {"is_active", "active", "enabled"}:
            return "boolean"
        return "text"

    @staticmethod
    def _suggested_options(field_name: str) -> List[Dict[str, str]]:
        name = str(field_name).lower()
        if name == "occurrence":
            return [
                {"label": "Daily", "value": "1"},
                {"label": "Weekly", "value": "2"},
                {"label": "Monthly", "value": "3"},
                {"label": "Quarterly", "value": "4"},
            ]
        if name in {"is_active", "active", "enabled"}:
            return [
                {"label": "Yes", "value": "1"},
                {"label": "No", "value": "0"},
            ]
        return []

    @staticmethod
    def _build_field_menu(state: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        table = str(state.get("table", "record"))
        required_fields = [str(x) for x in state.get("required_fields", [])]
        collected_fields = dict(state.get("collected_fields") or {})
        descriptions = state.get("field_descriptions") or {}
        remaining = ChatService._remaining_fields(required_fields, collected_fields)

        page_size = int(state.get("page_size", 5) or 5)
        page = max(0, int(state.get("page", 0) or 0))
        total_pages = max(1, (len(remaining) + page_size - 1) // page_size)
        page = min(page, total_pages - 1)

        start = page * page_size
        end = start + page_size
        page_fields = remaining[start:end]

        lines = [f"Select a field to fill for `{table}` ({len(remaining)} remaining):"]
        for idx, field in enumerate(page_fields, start=1):
            desc = str(descriptions.get(field, "")).strip()
            suffix = f" - {desc}" if desc else ""
            lines.append(f"{idx}. {field}{suffix}")

        controls = []
        if total_pages > 1:
            controls.append(f"Page {page + 1}/{total_pages}")
            controls.append("type `next` or `prev` for more options")
        controls.append("type option number to select")

        pending = str(state.get("pending_field", "")).strip()
        if pending:
            controls.append(f"or directly enter value for recommended `{pending}`")

        message = "\n".join(lines + ["", "; ".join(controls)])

        payload = {
            "workflow_id": "mutation_menu",
            "state": str(state.get("state", "collect_mutation")),
            "completed": False,
            "next_field": pending,
            "mode": "field_selection",
            "pagination": {
                "page": page + 1,
                "page_size": page_size,
                "total_pages": total_pages,
            },
            "collected_data": {
                "operation": state.get("operation", "insert"),
                "table": table,
                "required_fields": required_fields,
                "collected_fields": collected_fields,
            },
            "ui": {
                "type": "menu",
                "title": f"Choose next field for {table}",
                "options": [
                    {
                        "index": idx,
                        "id": field,
                        "label": field,
                        "description": str(descriptions.get(field, "")),
                    }
                    for idx, field in enumerate(page_fields, start=1)
                ],
            },
        }
        return message, payload

    @staticmethod
    def _build_value_prompt(state: Dict[str, Any], field_name: str) -> Tuple[str, Dict[str, Any]]:
        table = str(state.get("table", "record"))
        descriptions = state.get("field_descriptions") or {}
        desc = str(descriptions.get(field_name, "")).strip()
        detail = f" ({desc})" if desc else ""

        options = ChatService._suggested_options(field_name)
        kind = ChatService._input_kind(field_name)

        lines = [f"Enter value for `{field_name}`{detail} in `{table}`."]
        if options:
            lines.append("Options:")
            for idx, opt in enumerate(options, start=1):
                lines.append(f"{idx}. {opt['label']} ({opt['value']})")
            lines.append("Type option number or enter custom value.")
        elif kind == "date":
            lines.append("Please enter date in `YYYY-MM-DD` format.")
        elif kind == "numeric":
            lines.append("Please enter a numeric value.")
        elif kind == "boolean":
            lines.append("Please enter `1` (true) or `0` (false).")
        else:
            lines.append("Please enter a text value.")

        payload = {
            "workflow_id": "mutation_menu",
            "state": str(state.get("state", "collect_mutation")),
            "completed": False,
            "next_field": field_name,
            "mode": "field_value",
            "collected_data": {
                "operation": state.get("operation", "insert"),
                "table": table,
                "required_fields": [str(x) for x in state.get("required_fields", [])],
                "collected_fields": dict(state.get("collected_fields") or {}),
            },
            "ui": {
                "type": "input",
                "field": {
                    "id": field_name,
                    "label": field_name,
                    "kind": kind,
                    "description": desc,
                    "options": options,
                },
            },
        }
        return "\n".join(lines), payload

    @staticmethod
    def _build_confirmation_prompt(state: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        operation = str(state.get("operation", "insert")).lower()
        table = str(state.get("table", "record"))
        collected_fields = dict(state.get("collected_fields") or {})

        lines = [f"Please review before {operation} on `{table}`:"]
        for key in sorted(collected_fields.keys()):
            lines.append(f"- {key}: {collected_fields[key]}")
        lines.append("")
        lines.append("Reply `yes` to confirm and execute, or `no` to edit fields.")

        payload = {
            "workflow_id": "mutation_menu",
            "state": str(state.get("state", "confirm_mutation")),
            "completed": False,
            "next_field": "",
            "mode": "confirmation",
            "collected_data": {
                "operation": operation,
                "table": table,
                "required_fields": [str(x) for x in state.get("required_fields", [])],
                "collected_fields": collected_fields,
            },
            "ui": {
                "type": "confirmation",
                "title": f"Confirm {operation} on {table}",
                "actions": ["yes", "no"],
            },
        }
        return "\n".join(lines), payload

    @staticmethod
    def _resolve_field_selection(user_text: str, state: Dict[str, Any]) -> Optional[str]:
        required_fields = [str(x) for x in state.get("required_fields", [])]
        collected_fields = dict(state.get("collected_fields") or {})
        remaining = ChatService._remaining_fields(required_fields, collected_fields)

        text = str(user_text or "").strip().lower()
        if not text:
            return None

        if text in {f.lower() for f in remaining}:
            for field in remaining:
                if field.lower() == text:
                    return field
            return None

        if text.isdigit():
            page_size = int(state.get("page_size", 5) or 5)
            page = max(0, int(state.get("page", 0) or 0))
            start = page * page_size
            end = start + page_size
            page_fields = remaining[start:end]
            index = int(text)
            if 1 <= index <= len(page_fields):
                return page_fields[index - 1]
        return None

    @staticmethod
    def _is_command_like_input(text: str) -> bool:
        lowered = (text or "").lower()
        return any(
            token in lowered
            for token in ["create ", "insert ", "add ", "update ", "show ", "list ", "count ", "get ", "find "]
        )

    @staticmethod
    def _parse_user_field_updates(message: str, pending_field: str) -> Dict[str, str]:
        updates = SQLBuilderService.parse_kv_pairs(message)
        if updates:
            return updates

        text = (message or "").strip()
        if text and pending_field:
            if ChatService._is_command_like_input(text):
                return {}

            options = ChatService._suggested_options(pending_field)
            if options and text.isdigit():
                index = int(text)
                if 1 <= index <= len(options):
                    return {pending_field: str(options[index - 1]["value"])}

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
        awaiting = str(mutation_state.get("awaiting", "field_selection")).strip() or "field_selection"
        page = max(0, int(mutation_state.get("page", 0) or 0))
        page_size = max(1, int(mutation_state.get("page_size", 5) or 5))

        remaining = self._remaining_fields(required_fields, collected_fields)
        if not remaining:
            await self._clear_mutation_state(request.session_id)
            if request.metadata is None:
                request.metadata = {}
            request.metadata["mutation_context"] = {
                "operation": mutation_state.get("operation", "insert"),
                "table": mutation_state.get("table", ""),
                "fields": collected_fields,
            }
            return None

        if not pending_field:
            pending_field = remaining[0]

        if awaiting == "field_selection":
            if lower_text in {"next", "more"}:
                total_pages = max(1, (len(remaining) + page_size - 1) // page_size)
                mutation_state["page"] = min(page + 1, total_pages - 1)
                mutation_state["page_size"] = page_size
                mutation_state["pending_field"] = pending_field
                mutation_state["awaiting"] = "field_selection"
                await self._save_mutation_state(request.session_id, mutation_state)
                message, payload = self._build_field_menu(mutation_state)
                return self._build_final_response(request.session_id, message, workflow_payload=payload)

            if lower_text in {"prev", "back"}:
                mutation_state["page"] = max(0, page - 1)
                mutation_state["page_size"] = page_size
                mutation_state["pending_field"] = pending_field
                mutation_state["awaiting"] = "field_selection"
                await self._save_mutation_state(request.session_id, mutation_state)
                message, payload = self._build_field_menu(mutation_state)
                return self._build_final_response(request.session_id, message, workflow_payload=payload)

            selected_field = self._resolve_field_selection(user_text, mutation_state)
            if selected_field:
                mutation_state["pending_field"] = selected_field
                mutation_state["awaiting"] = "field_value"
                mutation_state["page"] = page
                mutation_state["page_size"] = page_size
                mutation_state["collected_fields"] = collected_fields
                await self._save_mutation_state(request.session_id, mutation_state)
                message, payload = self._build_value_prompt(mutation_state, selected_field)
                return self._build_final_response(request.session_id, message, workflow_payload=payload)
            elif user_text and not self._is_command_like_input(user_text):
                awaiting = "field_value"
            else:
                mutation_state["page"] = page
                mutation_state["page_size"] = page_size
                mutation_state["pending_field"] = pending_field
                mutation_state["awaiting"] = "field_selection"
                await self._save_mutation_state(request.session_id, mutation_state)
                message, payload = self._build_field_menu(mutation_state)
                return self._build_final_response(request.session_id, message, workflow_payload=payload)

        if awaiting == "field_value":
            updates = self._parse_user_field_updates(user_text, pending_field)
            accepted_updates = 0
            for key, value in updates.items():
                if key in required_fields and str(value).strip():
                    collected_fields[key] = str(value).strip()
                    accepted_updates += 1

            if accepted_updates == 0:
                mutation_state["pending_field"] = pending_field
                mutation_state["awaiting"] = "field_value"
                mutation_state["page"] = page
                mutation_state["page_size"] = page_size
                mutation_state["collected_fields"] = collected_fields
                await self._save_mutation_state(request.session_id, mutation_state)
                message, payload = self._build_value_prompt(mutation_state, pending_field)
                return self._build_final_response(request.session_id, message, workflow_payload=payload)

            next_field = self._next_missing_field(required_fields, collected_fields)
            mutation_state["collected_fields"] = collected_fields
            mutation_state["pending_field"] = next_field
            mutation_state["page"] = 0
            mutation_state["page_size"] = page_size

            if next_field:
                mutation_state["awaiting"] = "field_selection"
                await self._save_mutation_state(request.session_id, mutation_state)
                message, payload = self._build_field_menu(mutation_state)
                return self._build_final_response(request.session_id, message, workflow_payload=payload)

            mutation_state["awaiting"] = "confirmation"
            mutation_state["pending_field"] = ""
            await self._save_mutation_state(request.session_id, mutation_state)
            message, payload = self._build_confirmation_prompt(mutation_state)
            return self._build_final_response(request.session_id, message, workflow_payload=payload)

        if awaiting == "confirmation":
            if lower_text in {"yes", "y", "confirm", "confirmed", "proceed"}:
                await self._clear_mutation_state(request.session_id)
                if request.metadata is None:
                    request.metadata = {}
                request.metadata["mutation_context"] = {
                    "operation": mutation_state.get("operation", "insert"),
                    "table": mutation_state.get("table", ""),
                    "fields": collected_fields,
                }
                return None

            if lower_text in {"no", "n", "edit", "change"}:
                mutation_state["awaiting"] = "field_selection"
                mutation_state["pending_field"] = self._next_missing_field(required_fields, {})
                mutation_state["page"] = 0
                mutation_state["page_size"] = page_size
                await self._save_mutation_state(request.session_id, mutation_state)
                message, payload = self._build_field_menu(mutation_state)
                return self._build_final_response(request.session_id, message, workflow_payload=payload)

            message, payload = self._build_confirmation_prompt(mutation_state)
            return self._build_final_response(request.session_id, message, workflow_payload=payload)

        mutation_state["awaiting"] = "field_selection"
        await self._save_mutation_state(request.session_id, mutation_state)
        message, payload = self._build_field_menu(mutation_state)
        return self._build_final_response(request.session_id, message, workflow_payload=payload)

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
                        "awaiting": "field_value",
                        "page": 0,
                        "page_size": 5,
                    }
                    await self._save_mutation_state(request.session_id, recovery_state)
                    message, payload = self._build_value_prompt(recovery_state, target_column)
                    workflow_payload = payload
                    final_message = message
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
                    "awaiting": "field_selection",
                    "page": 0,
                    "page_size": 5,
                }
                await self._save_mutation_state(request.session_id, state)

                final_message, workflow_payload = self._build_field_menu(state)

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
