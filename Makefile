# TAG Backend Makefile

.PHONY: up down restart logs lint type test test-cov ci-local clean

up:
	docker compose up --build -d

down:
	docker compose down

restart:
	docker compose restart tag_backend

logs:
	docker logs -f tag_backend

lint:
	python3 -m ruff check .

type:
	python3 -m mypy app/config.py app/assistant/nodes/chat_node.py app/assistant/services/router_service.py tests

test:
	python3 -m pytest -q

test-cov:
	python3 -m pytest --cov=app/assistant --cov=app.services.chat_service --cov-report=term-missing --cov-fail-under=45

ci-local: lint type test-cov

test-docker:
	docker exec -it tag_impl_backend python3 -m pytest -q

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
