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

## Deployment

Nothing reaches production until the image is rebuilt AND the manifest digest is bumped.
Pushing code alone changes nothing in the cluster, and `kubectl rollout restart` is useless
here because the image is pinned by digest — a restart just re-runs the same code.

### Where production lives

- Manifests are in this repo under `k8s/`. `backend.yaml` holds the Deployment, Service, and
  both Ingresses; `postgres.yaml`, `redis.yaml`, and the `backup-*.yaml` pair are separate.
- Cluster access is the `data-hunt` kubectl context, namespace `data-hunt`. Always pass
  `--context data-hunt` — the machine's current context is usually a different cluster, and
  without the flag you get `namespaces "data-hunt" not found`.
- Deployment, Service, and app label are all `data-hunt`. Public API is
  `https://hunt.data.lisacorp.com`, served on container port `8111`.
- Health checks: `/health/liveness` and `/health/readiness`.

### Deploy flow

1. Commit the code change and push to `master`.
2. Docker Hub rebuilds `dushes/data-hunt:latest` automatically from `master`. No local
   `docker build` — the flow does not need Docker installed. The build takes roughly 2–5 min.
3. Wait for the new digest, then read it:
   `curl -s https://hub.docker.com/v2/repositories/dushes/data-hunt/tags/latest | jq -r .digest`
   Poll until it differs from the digest currently in `k8s/backend.yaml`.
4. Replace the `image: dushes/data-hunt@sha256:…` digest in `k8s/backend.yaml` with the new
   one and commit it on its own, titled `Deploy <what shipped>`. This separate deploy commit
   is the repo's convention — keep it.
5. `kubectl --context data-hunt diff -f k8s/backend.yaml` should show only the image line.
6. `kubectl --context data-hunt apply -f k8s/backend.yaml`, then
   `kubectl --context data-hunt rollout status deployment/data-hunt -n data-hunt`.
7. Confirm the running image matches:
   `kubectl --context data-hunt get deploy data-hunt -n data-hunt -o jsonpath='{.spec.template.spec.containers[0].image}'`

### After deploying

- CSV routes cache responses for 60 s per parameter set, with an additional stale fallback
  from `CSV_CACHE_STALE_TTL_SECONDS`. A freshly deployed column will not show up in a table
  reloaded immediately — wait out the cache before concluding the deploy failed.
- If the response shape changed, refresh the sibling repo's `openapi.json` snapshot; the
  convention there is a `Refresh API contract for <area>` commit. Diff it against
  `https://hunt.data.lisacorp.com/openapi.json` first, since many changes leave it identical.
- App startup runs `alembic upgrade head`; review migration safety before deploying schema
  changes.
- Do not copy plaintext secret values from deployment manifests into docs, logs, commits, or
  chat. Prefer Kubernetes Secrets for future deployment edits.

## Coding Rules

- Use the existing plain-module style unless a local pattern clearly supports a new abstraction.
- Keep router responses and request shapes backward compatible unless the user explicitly approves an API change.
- Treat database schema changes as migration-backed changes. Do not edit models without adding or validating the corresponding Alembic migration when schema behavior changes.
- Keep external-service calls async where the surrounding code is async, and preserve existing error handling patterns with `HTTPException` for API failures.
- Do not commit secrets, API keys, tokens, private keys, wallet data, or local `.env` files.

## Landing Page Communication

- When adding or materially changing user-facing functionality, also update the landing page in the sibling `data-hunt-web` repository within the same task.
- This includes new data sources, integrations, supported platforms or blockchains, workflows, and capabilities that users can select or use.
- Present the change concisely and visually, emphasizing the user benefit instead of implementation details. Keep the landing page easy to understand with minimal reading.
- Only advertise functionality that is actually implemented and verified. Internal refactors, maintenance, and fixes that do not change visible capabilities do not require a landing-page update.
- Commit and verify the backend and landing-page changes in their respective repositories.

## Verification

- Run focused unit tests for touched behavior.
- For router or service changes, add or update tests when behavior, parsing, pagination, authentication, or error handling changes.
- For docs-only changes, tests are not required.

## Git Workflow

- Check `git status --short --branch` before editing and before committing.
- Do not revert or overwrite unrelated user changes.
- Stage only files changed for the requested task.
- Commit the finished change and push the branch unless the user explicitly says not to.
