from app.assistant.services.manifest_catalog import ManifestCatalog


def test_manifest_catalog_resolves_schedule_alias_to_scheduler_table():
    catalog = ManifestCatalog()
    table = catalog.resolve_table_from_query("create schedule")
    assert table in {"scheduler_details", "scheduler_task_details"}


def test_manifest_catalog_required_create_fields_has_fallback_from_important_columns():
    catalog = ManifestCatalog()
    required = set(catalog.required_create_fields("scheduler_details"))
    assert "date" in required
    assert "occurrence" in required
