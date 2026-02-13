from typing import Any, Dict, List, Set

from app.services.schema_manifest_service import SchemaManifestService


class ManifestCatalog:
    DEFAULT_CREATE_EXCLUSIONS = {
        "id",
        "created_at",
        "updated_at",
        "deleted_at",
        "is_active",
    }

    def __init__(self):
        self.manifest = SchemaManifestService().manifest

    def table_names(self) -> Set[str]:
        return set((self.manifest.get("tables") or {}).keys())

    def table_meta(self, table: str) -> Dict[str, Any]:
        return (self.manifest.get("tables") or {}).get(table, {}) or {}

    def important_columns(self, table: str) -> Set[str]:
        return set((self.table_meta(table).get("important_columns") or {}).keys())

    def aliases(self, table: str) -> List[str]:
        meta = self.table_meta(table)
        base = [table.lower(), table.lower().replace("_", " ")]
        if table.lower().endswith("_details"):
            base.append(table.lower().replace("_details", ""))
        base.extend(str(a).lower() for a in (meta.get("aliases") or []) if str(a).strip())
        if "scheduler" in table.lower():
            base.extend(["schedule", "schedules", "scheduler", "schedulers"])
        return list(dict.fromkeys(base))

    def resolve_table_from_query(self, query: str) -> str:
        q = (query or "").lower()
        if not q:
            return ""
        for table in sorted(self.table_names()):
            aliases = self.aliases(table)
            if any(alias in q for alias in aliases):
                return table
        return ""

    def required_create_fields(self, table: str) -> List[str]:
        create_cfg = ((self.table_meta(table).get("operations") or {}).get("create") or {})
        explicit_required = [str(x).strip() for x in create_cfg.get("required_fields", []) if str(x).strip()]
        if explicit_required:
            return explicit_required

        important_columns = (self.table_meta(table).get("important_columns") or {})
        fallback = []
        for column in important_columns.keys():
            name = str(column).strip()
            if not name or name in self.DEFAULT_CREATE_EXCLUSIONS:
                continue
            fallback.append(name)
        return fallback

    def important_column_descriptions(self, table: str) -> Dict[str, str]:
        columns = (self.table_meta(table).get("important_columns") or {})
        out: Dict[str, str] = {}
        for name, meta in columns.items():
            desc = str((meta or {}).get("description", "")).strip()
            out[str(name)] = desc
        return out
