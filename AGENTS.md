# AGENTS.md

Guidance for coding agents working in this repository.

## Project Scope

- This is the backend API for Data Hunt.
- The stack is Python 3.12, FastAPI, SQLAlchemy, Alembic, APScheduler, and uvicorn.
- Keep changes narrowly scoped. Do not make major product, architecture, API, UX, or business-logic changes without explicit user confirmation.

## Repository Layout

- `server.py` wires the FastAPI app, routers, CORS, startup migrations, and scheduler.
- `routers/` contains API route modules. Prefer adding endpoint-specific logic near the owning router.
- `models.py`, `database.py`, and `alembic/versions/` own persistence and migrations.
- `tasks.py` contains scheduled data-fetching work.
- `config.py` owns environment-driven configuration.
- `docs/` stores static chain/history reference data.
- `tests/` contains Python unit tests.

## Development Commands

- Install or sync dependencies with `uv sync`.
- Run the API locally with `uv run python server.py`.
- Run tests with `uv run python -m unittest discover -s tests`.
- Create Alembic migrations with `uv run alembic revision --autogenerate -m "<message>"`.
- Apply migrations with `uv run alembic upgrade head`.

## Coding Rules

- Use the existing plain-module style unless a local pattern clearly supports a new abstraction.
- Keep router responses and request shapes backward compatible unless the user explicitly approves an API change.
- Treat database schema changes as migration-backed changes. Do not edit models without adding or validating the corresponding Alembic migration when schema behavior changes.
- Keep external-service calls async where the surrounding code is async, and preserve existing error handling patterns with `HTTPException` for API failures.
- Do not commit secrets, API keys, tokens, private keys, wallet data, or local `.env` files.

## Verification

- Run focused unit tests for touched behavior.
- For router or service changes, add or update tests when behavior, parsing, pagination, authentication, or error handling changes.
- For docs-only changes, tests are not required.

## Git Workflow

- Check `git status --short --branch` before editing and before committing.
- Do not revert or overwrite unrelated user changes.
- Stage only files changed for the requested task.
- Commit the finished change and push the branch unless the user explicitly says not to.
