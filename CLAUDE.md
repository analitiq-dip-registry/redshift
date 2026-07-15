---
name: redshift
description: >
  Amazon Redshift cloud data warehouse — read schemas and tables over SQLAlchemy
type: database
---

# Amazon Redshift

Amazon Redshift is AWS's petabyte-scale cloud data warehouse. This connector reads schemas and tables from a Redshift cluster using the SQLAlchemy `redshift+redshift_connector` dialect (Amazon's official `redshift_connector` Python driver). Tables and columns are discovered at runtime via `information_schema`.

The connector root is also an installable Python package (`analitiq-connector-redshift`): `connector.py` carries `RedshiftDialect` + `RedshiftConnector`, registered under the `redshift` entry point for both `analitiq.source_connectors` and `analitiq.destination_connectors`.

## Driver: why a sync driver, and why validation currently fails

`redshift_connector` is **synchronous**. That is not an oversight — it is the only driver that works:

| Driver | Result (validated live against Redshift Serverless, see #3) |
|---|---|
| ADBC `postgresql` | reads OK; **writes impossible** — Redshift's `COPY` loads only from S3/EMR/DynamoDB, never `FROM STDIN`. Arrow documents its Redshift support as "experimental". |
| `asyncpg` | connects, but **all parameterized writes fail** — its internal `pg_catalog` type-introspection query is rejected by Redshift's catalog. |
| `psycopg3` | every query fails on `client_encoding=UNICODE`. |
| **`redshift_connector`** | **works fully** — connect+TLS, DDL incl. `VARBYTE`, parameterized INSERT, MERGE upsert. |

AWS is explicit: *"PostgreSQL drivers are not tested and not supported by the Amazon Redshift team."* asyncpg is equally explicit that other PG-protocol databases are "not being actively tested". Neither vendor supports asyncpg → Redshift.

**Known validation failure — deliberate, not a regression.** `definition/connector.json` declares `driver: "redshift+redshift_connector"`, which **fails** the published contract's `SqlAlchemyTransport.driver` check (`^[a-z][a-z0-9_]*\+(asyncpg|aiomysql|asyncmy|aiosqlite|oracledb)$` — async only). Everything else in the connector validates clean.

- The engine **already supports this**: it gained a synchronous SQLAlchemy transport in analitiq-ai/analitiq-engine#239 (closing #224, merged 2026-06-10), built for `redshift_connector` specifically. The published schema was never regenerated to admit it.
- Both ways of making the check pass were rejected. `redshift+asyncpg` is a **fabrication** — no vendor documents that combination; it would validate and die at connect. Deleting the `driver` field is schema-valid (`driver` is optional) but merely relocates the sync driver into the DSN template and `options`, where nothing validates it — converting a loud error into a silent connect-time failure. That was PR #6; it is closed.

The fix is a contract change: the pattern must admit `redshift_connector`, and the field's description — which currently names `redshift_connector` as unsupported — must be corrected. See #4's "Blocked on" section; it belongs to the infrastructure repo.

## Authentication

### Database (username + password)
- Client app required: no
- Transport: `sqlalchemy`, driver `redshift+redshift_connector` (sync)
- DSN: `redshift+redshift_connector://{username}:{password}@{host}:{port}/{database}`

AWS IAM temporary-credential authentication is supported by Redshift but is **not** implemented in this connector version.

### TLS

`ssl_mode` is libpq's canonical six: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` (default `verify-ca`).

The driver itself accepts only `verify-ca` / `verify-full`, via two connect parameters (`ssl: bool` + `sslmode: str`). `RedshiftDialect.build_tls_connect_args` reconciles the canonical vocabulary onto that:

| `ssl_mode` | Sent to driver |
|---|---|
| `disable` (and legacy `none`) | `{ssl: False}` — genuinely unencrypted |
| `allow`, `prefer`, `require`, `verify-ca` | `{ssl: True, sslmode: "verify-ca"}` |
| `verify-full` | `{ssl: True, sslmode: "verify-full"}` |

Every reconciliation is strictness-**increasing** or honest — no mapping silently weakens what was asked for. Note `disable` is **rejected server-side** by Redshift Serverless and by any cluster with `require_SSL=true`: the user gets a hard connection error, never a silent downgrade. `none` is accepted by the dialect only as a legacy alias so connections stored against v0.0.1's vocabulary keep resolving.

**`ssl_ca_certificate` is declared but cannot be honored.** `redshift_connector` has no parameter for a custom CA bundle — it verifies against its own bundled Amazon CA, and Redshift presents an ACM-issued certificate chaining to the public trust store, so a custom bundle is normally unnecessary. The input is declared, and `tls.ca_certificate` wired to it, **so the dialect can see a supplied value and raise**; dropping the wiring would make a user-supplied CA silently ignored, which is strictly worse. Supplying one fails loudly.

### Connection inputs
| Input | Phase | Storage | Required | Notes |
|-------|-------|---------|----------|-------|
| `host` | pre_auth | connection.parameters | yes | Cluster endpoint, e.g. `cluster.xxxx.us-west-1.redshift.amazonaws.com` |
| `port` | pre_auth | connection.parameters | yes | Default `5439` |
| `database` | pre_auth | connection.parameters | yes | e.g. `dev` |
| `ssl_mode` | pre_auth | connection.parameters | yes | Default `verify-ca` |
| `ssl_ca_certificate` | pre_auth | secrets | no | Declared but unusable — supplying it raises (see TLS above) |
| `username` | auth | connection.parameters | yes | |
| `password` | auth | secrets | yes | Secret |

## Post-Auth Steps

Resource discovery runs automatically on activation (`information_schema` strategy). The connector lists schemas/tables and emits a per-connection `type_map`. The schemas `information_schema`, `pg_catalog`, and `pg_internal` are excluded from discovery.

## Available Endpoints

This is a database connector — it has no static endpoints. Tables and columns are discovered at runtime from `information_schema` and selected per connection.

## Rate Limits

None imposed by the connector. Concurrency is bounded by the cluster's `max_connections` setting and WLM query-slot configuration.

## Type mapping

Read-map regex natives are authored **UPPERCASE** — the engine uppercases the native before matching, so lowercase patterns are dead code and never fire.

- `SUPER` → Json (schemaless container; a scalar canonical is a validator error)
- `GEOMETRY` / `GEOGRAPHY` / `VARBYTE` → Binary
- `HLLSKETCH` → **Utf8** — both renderings are text (SPARSE is JSON, DENSE is Base64), never raw bytes
- `INTERVAL` types → Utf8 (rendering is `IntervalStyle`-dependent, so no fixed structured mapping is safe)
- `TIMETZ` → Time64 — the zone is dropped by design; Arrow has no zone-aware time-of-day type
- Bare `DECIMAL` / `NUMERIC` → `Decimal128(18, 0)` — AWS's documented Redshift default, **not** Postgres's `(38, 0)`
- Write side: `Utf8` → `VARCHAR(65535)`, never `TEXT` — Redshift silently aliases `TEXT` to `VARCHAR(256)`, which truncates

## Caveats

- AWS IAM authentication is not implemented (username/password only).
- Redshift speaks the PostgreSQL wire protocol, but AWS does not test or support generic PostgreSQL drivers; this connector uses Amazon's `redshift_connector` driver.
- Redshift has no `ON CONFLICT`. Upsert uses `MERGE INTO` against an inlined `(SELECT ... UNION ALL ...) src` subquery of `CAST` literals — Redshift rejects bind parameters inside the `USING` subquery, and literal source rows do not implicitly cast. Both `WHEN` clauses are mandatory, so a table whose columns are *all* conflict keys cannot upsert; use `write_mode 'insert'`.
- `VARCHAR` is defined in **bytes**, not characters. A value over 65,535 bytes (multibyte UTF-8 costs up to 4 bytes/char) fails the write; Redshift has no unbounded text type.
- Known unexercised gaps in the write path, all loud failures rather than silent corruption: `float('nan')`/`inf` render as bare identifiers; `CAST(varchar AS SUPER)` is invalid (SUPER ingestion needs `JSON_PARSE()`); a reflected `SUPER`/`VARBYTE` column may come back as `NullType`; `UInt64` → `BIGINT` is lossy above 2^63-1.
- **Open risk (not connector-fixable):** `information_schema.columns.data_type` returns bare, unparameterized tokens (`numeric`, not `DECIMAL(8,2)`) with precision/scale in separate columns. If the builtin discovery strategy does not compose the parameterized form before handing the native to the read map, every decimal collapses to the `Decimal128(18, 0)` fallback and declared scale is silently lost. Needs an engine-side check before this connector is marked verified.
