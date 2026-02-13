from app.assistant.services.mutation_understanding_service import MutationUnderstandingService


def test_schedule_task_maps_to_scheduler_task_details():
    svc = MutationUnderstandingService()
    table = svc.resolve_table("schedule a task", {"operation": "insert", "table": ""})
    assert table == "scheduler_task_details"


def test_schedule_without_task_maps_to_scheduler_details():
    svc = MutationUnderstandingService()
    table = svc.resolve_table("create schedule", {"operation": "insert", "table": ""})
    assert table == "scheduler_details"
