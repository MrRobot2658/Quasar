# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A local Docker-Compose dev environment for a **multi-tenant real-time ID-Mapping + CDP (Customer Data Platform)** system, modeled on the production design in `docs/` (Kafka → Flink → MySQL → Doris). In dev, the heavy production components (Flink/StreamPark, Doris) are **simulated**: the `id-mapping` service stands in for Flink jobs, and MySQL stands in for Doris OLAP tables. **Scheduling, however, is real**: a single-container **Apache Airflow** service backs the "可视化编排 Pipelines" feature — `sql-engine`'s `scheduler_api.py` triggers a parametrized DAG (`airflow/dags/agenticdatahub_pipeline.py`) over Airflow's REST API when a pipeline runs, passing the pipeline's node list in `dag_run.conf`; the DAG uses **dynamic task mapping** (`.expand`) to fan out one `run_node` task per canvas node at runtime (graceful local-sim fallback if Airflow is down). The frontend CDP console mimics Twilio Segment's app (IA & page design — see memory `mimic-segment.md`).

Docs are the source of truth for the production design — `docs/design.md`, `docs/scale-comparison.md`, `docs/MCP调用链路.md`. Most code/UI text and commit messages are in Chinese; match that when editing.

## Commands

```bash
# Bring up the full stack (dev scale)
docker compose up -d --build

# Scale tiers (rewrites docker/mysql/conf.d/scale.cnf, sets Kafka/Redis/replica env)
bash scripts/scale-up.sh [dev|medium|large|xlarge]

# Apply incremental SQL migrations — ALWAYS via this script, not raw mysql pipe.
# It forces --default-character-set=utf8mb4 to avoid double-encoding Chinese text.
bash scripts/apply_migrations.sh

# Simulate data
bash scripts/simulate_kafka.sh          # full Kafka path (tenants 1001 + 1002)
bash scripts/simulate_via_api.sh        # direct API, skips Kafka
.venv/bin/python scripts/simulate_objects.py --leads 20   # multi-object CDP data

# Tests (pytest) — most tests hit live services on fixed ports, so the stack must be up.
# Tests are skipped (not failed) if a service isn't reachable.
.venv/bin/pip install -r tests/requirements.txt
.venv/bin/pytest tests/ -v
.venv/bin/pytest tests/test_dsl_engine.py -v                       # single file
.venv/bin/pytest tests/test_sql_engine.py::TestSqlEngine::test_health -v   # single test
bash scripts/run_profile_test.sh        # full E2E: up + migrate + run profile test

# Frontend (see frontend/README.md)
cd frontend && npm install && npm run dev    # :5173, proxies /api → sql-engine:8002
npm run build                                # outputs dist/ with base=/console/
```

## Service topology & ports

Everything is reached through the **Nginx gateway on :8080** (`docker/nginx/nginx.conf`). Direct ports exist for debugging.

| Service | Port | Role |
|---|---|---|
| nginx gateway | 8080 | `/` home, `/console/` CDP SPA, `/api/*` → sql-engine (strips `/api`), Swagger proxies |
| id-mapping | 8001 | Simulated Flink: Kafka consumer → Redis hot layer → MySQL cold layer → OneID merge |
| sql-engine | 8002 | OLAP query layer + CDP (objects/DSL/segments/NL/ETL/tags) |
| MySQL 8 | 3308 | Business + simulated-Doris tables |
| Redis 7 | 6381 | OneID hot cache |
| Kafka | 9094 | Multi-tenant event bus (`tenant-{id}-events`, `shared-tenant-events`) |
| Kafka UI | 8083 | Topic viewer |

The CDP **MCP server** (`services/mcp/server.py`, configured in `.mcp.json`) wraps sql-engine's read-only CDP capabilities as Claude tools (`cdp_schema`, `cdp_search`, `cdp_estimate`, `cdp_validate`, `cdp_nl_segment`, ...). It talks to sql-engine over HTTP; it never writes — saving a segment still goes through the human-confirm endpoint.

## Architecture

### sql-engine (`services/sql-engine/`) — the heart of the CDP

`main.py` wires FastAPI to a set of single-responsibility services, all sharing one swappable OLAP executor:

- `executor.py` — `OlapExecutor` ABC with `MysqlOlapExecutor` (default, MySQL simulating Doris) and a Doris path. **Storage is decoupled here**: switch backends with env `OLAP_BACKEND=doris OLAP_HOST=... OLAP_PORT=...`. Nothing else in the codebase issues raw connections to the OLAP store.
- `engine.py` + `templates/olap_queries.yaml` — template SQL. Queries are **named templates + parameter binding**, never string-concatenated SQL.
- `objects.py` — multi-object model (User/Lead/Account/Product/Store/Order). `OBJECT_REGISTRY` defines fields; `RELATION_MATRIX` defines allowed cross-object relations. This registry is the single source of truth that DSL validation, ETL, and the frontend all key off.
- `dsl.py` — the **DSL validation layer**, the security boundary for query-building. A "DSL Rule" is a controlled intermediate representation (`{object, logic, conditions[], relations[]}`). It does `validate` (fields/operators/relations/hop-count), `echo` (→ human-readable Chinese summary), `compile` (→ SQL, not executed), and `estimate` (dry-run COUNT). **Key invariant: the LLM never emits SQL directly — a candidate DSL must pass this layer before it is shown or run.**
- `agent.py` (`NlSegmentAgent`) + `nl_query.py` — natural-language → candidate DSL / template query, via DeepSeek. Degrades to rule-only mode when `AGENT_LLM_ENABLED=0`.
- `segments.py`, `groups.py`, `tags.py`, `etl.py` — saved audience segments, user groups, hierarchical tag tree, and visual ETL (CSV/paste → field mapping → dry-run preview → import into objects, optionally creating relations).
- `schemas.py` — all Pydantic request/response models.

### id-mapping (`services/id-mapping/main.py`)

Single-file simulation of the Flink ID-Mapping job. Consumes `UserEvent`s from Kafka (or direct `/events/process`), resolves/merges identities across channels (wechat openid/unionid, wework extid, phone, email, device) into a OneID, writes the hot mapping to Redis and the audit trail (`id_mapping`, `merge_log`, profile tables) to MySQL.

### Data / SQL

`sql/init.sql` is the base schema (loaded on MySQL container init). `sql/migrate_*.sql` are incremental migrations (groups, tags, objects, segments, doris-simulation tables) — apply via `scripts/apply_migrations.sh`.

### Frontend (`frontend/`)

React 18 + Vite + TypeScript + Tailwind, TailAdmin-style, Segment-inspired IA. `src/api/client.ts` calls sql-engine via `/api` (dev: Vite proxy; prod: nginx same-origin under `/console/`). The unified filter UI (`components/filter/`) builds DSL rules that round-trip through sql-engine's validate/estimate endpoints. `src/lib/objects.ts` mirrors the backend object registry. Tenant is carried in `context/TenantContext.tsx`.

## Conventions & gotchas

- **Migrations / any MySQL pipe import must use `--default-character-set=utf8mb4`** or Chinese text double-encodes. Use `scripts/apply_migrations.sh`.
- **Never hand-build SQL or let the LLM emit SQL.** Go through `engine.py` templates or the `dsl.py` validate→compile path.
- Test ports differ from in-container ports (MySQL 3308, Redis 6381, Kafka 9094) — see `tests/conftest.py`. Tests skip gracefully if services are down.
- DeepSeek key lives in `.env` (gitignored); copy from `.env.example`. The MCP server and sql-engine read it.
- The root `package.json` is only for Node doc tooling (Playwright PDF render); the real frontend is in `frontend/`.
