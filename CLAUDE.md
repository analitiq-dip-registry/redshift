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

**Validation: resolved as of contract `1.0.0rc14`.** This section previously documented a deliberate validation failure — `driver: "redshift+redshift_connector"` was rejected by the contract's async-only `SqlAlchemyTransport.driver` pattern (`^[a-z][a-z0-9_]*\+(asyncpg|aiomysql|asyncmy|aiosqlite|oracledb)$`). That constraint was replaced at rc14 with a generic `dialect+driver` shape check (`^[a-z][a-z0-9_]*\+[a-z][a-z0-9_]*$`), which imposes no driver allow-list; dialect-registration validity is deferred to transport build time. The field's description now cites `redshift+redshift_connector` as a supported example rather than naming it unsupported.

The whole definition validates clean at rc17 (connector + both type maps, zero findings).

- The engine gained its synchronous SQLAlchemy transport in analitiq-ai/analitiq-engine#239 (closing #224, merged 2026-06-10), built for `redshift_connector` specifically.
- The two workarounds that were rejected at the time remain wrong and must not be reintroduced. `redshift+asyncpg` is a **fabrication** — no vendor documents that combination; it would validate and die at connect. Deleting the `driver` field is schema-valid (`driver` is optional) but merely relocates the sync driver into the DSN template and `options`, where nothing validates it — converting a loud error into a silent connect-time failure. That was PR #6; it is closed.

## Authentication

### Database (username + password)
- Client app required: no
- Transport: `sqlalchemy`, driver `redshift+redshift_connector` (sync)
- DSN: `redshift+redshift_connector://{username}:{password}@{host}:{port}/{database}`

AWS IAM temporary-credential authentication is supported by Redshift but is **not** implemented in this connector version.

### TLS

The **declared** `ssl_mode` enum is two values: `verify-ca`, `verify-full` (default `verify-ca`) — the only modes `redshift_connector` documents for its `sslmode` parameter. These are what a new connection may be configured with.

The driver takes TLS via two connect parameters (`ssl: bool` + `sslmode: str`). `RedshiftDialect.build_tls_connect_args` additionally reconciles the wider libpq vocabulary (`disable`, `allow`, `prefer`, `require`, and the legacy alias `none`) **purely so connections stored against the connector's prior ADBC/libpq vocabulary keep resolving** — those values are not offered for new connections:

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

## Write path (stage-then-merge)

The dialect is on the CDK's stage-then-merge surface (ADR sql-write-path-v2). Every write — not just upserts — lands in a stage table first, so both hooks are mandatory:

- `stage_table_sql` renders `CREATE [TEMPORARY] TABLE <stage> (LIKE <target> INCLUDING DEFAULTS)`. The parentheses are required (LIKE is an alternative inside the column-list production). AWS recommends `LIKE` over CTAS for staging because LIKE inherits the parent's distribution style and sort keys, keeping the stage collocated with the target. `INCLUDING DEFAULTS` is a deliberate deviation from AWS's runbook one-liner: LIKE copies NOT NULL but not defaults, so a target carrying a `NOT NULL DEFAULT` column the batch does not land would otherwise produce a stage the landing INSERT cannot satisfy. **This is the line most worth a live probe.**
- Under the declared `stage.scope: temp`, the CDK hands the hook a **schema-less** stage address — Redshift refuses a schema name on a temporary table ("temporary tables exist in a special schema").
- `merge_statement_sql` renders the upsert; see Caveats for the MERGE and insert-only details.

`DROP TABLE IF EXISTS`, the insert-mode anti-join, the append, and `empty_table_sql` are all rendered by the CDK — the dialect must not implement them. `empty_table_sql`'s ANSI `DELETE FROM` default is correct here precisely because Redshift's `TRUNCATE` commits implicitly and would break the stage cycle's single-transaction shape.

**Conformance.** The CDK's `override-surface` check computes the sanctioned set from `SqlDialect`'s public attributes; any public member the dialect adds of its own is a CI failure. This dialect's public surface is exactly nine names, all sanctioned. `max_identifier_length = 127` must stay set on the class — tier-1 asserts a composed stage name against the class attribute while composing it within the *declared* `sql_capabilities.limits.max_identifier_len`, so the base default of 63 fails. Run the suite with:

```
pytest -p cdk.conformance.plugin --pyargs cdk.conformance.tier1 \
  --connector-dir . --connector-class analitiq_connector_redshift.connector:RedshiftConnector
```

## Rate Limits

None imposed by the connector. Concurrency is bounded by the cluster's `max_connections` setting and WLM query-slot configuration.

## Type mapping

Read-map regex natives are authored **UPPERCASE** — the engine uppercases the native before matching, so lowercase patterns are dead code and never fire.

- `SUPER` → Json (schemaless container; a scalar canonical is a validator error)
- `VARBYTE` / `VARBINARY` / `BINARY VARYING` → Binary
- `GEOMETRY` / `GEOGRAPHY` → **Utf8** — AWS documents `ResultSetMetadata` reporting `VARCHAR` for these, and `UNLOAD` writes hexadecimal EWKB; both are text. Matched by regex so the parameterized spellings (`GEOMETRY(POINT)`, `GEOMETRY(POINT, 4326)`) resolve too
- `HLLSKETCH` → **Utf8** — both renderings are text (SPARSE is JSON, DENSE is Base64), never raw bytes
- `INTERVAL` types → Utf8 (rendering is `IntervalStyle`-dependent, so no fixed structured mapping is safe). `Duration` is semantically faithful for `INTERVAL DAY TO SECOND`, but AWS documents no per-type Python object mapping for `redshift_connector`, so Utf8 is the safe read map
- `TIMETZ` → Time64 — the zone is dropped by design; Arrow has no zone-aware time-of-day type
- Bare `DECIMAL` / `NUMERIC` → `Decimal128(18, 0)` — AWS's documented Redshift default, **not** Postgres's `(38, 0)`
- Write side: `Utf8` → `VARCHAR(65535)`, never `TEXT` — Redshift silently aliases `TEXT` to `VARCHAR(256)`, which truncates
- Write side: `Decimal` scale is bounded to 0–37 — Redshift's maximum DECIMAL scale is 37, so `Decimal128(38, 38)` fails at configuration time rather than emitting DDL the server rejects
- Write side: `Duration(*)` → **`VARCHAR(65535)`, not `INTERVAL DAY TO SECOND`** — and the native must stay character-identical to the `Utf8` rule. The read map maps every INTERVAL to Utf8, so a write rule rendering INTERVAL never converges (`Duration` → INTERVAL → Utf8 → VARCHAR), and the CDK's tier-1 `type-map-convergence` check fails it: a re-created destination table would silently change that column's type. A "right-sized" `VARCHAR(64)` would fail the same check. Any future write rule whose native reads back as `Utf8` is under the same obligation

## Declared capabilities (contract rc16/rc17)

`definition/connector.json` declares four optional blocks the engine reads instead of probing the live database. `sql_capabilities` is the load-bearing one: the CDK treats an undeclared block as "unknown" and makes dependent gates refuse loudly rather than guess.

| Block | Value | Grounding |
|---|---|---|
| `sql_capabilities.catalog` | `read` | Cross-database queries are read-only. **RA3 / Serverless only** — a catalog-qualified read fails on dc2 |
| `sql_capabilities.session_targeting` | `per_statement` | Every statement is schema-qualified; no `search_path` is set |
| `sql_capabilities.merge_form` | `merge` | Redshift has `MERGE INTO`, no `ON CONFLICT` |
| `sql_capabilities.bulk_load` | `{}` | **Deliberately empty.** `COPY` reads only from S3/EMR/DynamoDB/SSH — there is no stdin path, and this connector declares no S3/IAM inputs. `{}` is the contract's encoding of "no mechanism"; the key is required, and an explicit `null` is refused |
| `sql_capabilities.stage` | `temp` / `target` / `transactional_ddl: true` | `CREATE TEMPORARY TABLE` is session-scoped (no schema qualifier); Redshift runs `CREATE`/`DROP TABLE` inside a transaction |
| `sql_capabilities.limits.max_identifier_len` | `127` | AWS: identifiers are 1–127 bytes. Overrides the CDK default of 63 |
| `sql_capabilities.limits.max_bind_params` | `32767` | **Weakest grounding here.** Inferred from the PostgreSQL wire `Bind` Int16 parameter count, not an AWS-documented figure, and never exercised live. Challenge it if a live run contradicts it |
| `write_unit` | 2000 rows / 1 MiB | Derived, not documented. Redshift caps a single statement at 16 MB and the MERGE inlines the whole batch as literals (~5–8× inflation), so this keeps the rendered statement well under the ceiling |
| `concurrency.max_connections` | `50` | Deliberately **not** the 500–2000 server ceiling, which is a *shared* budget across all cluster clients. AWS caps total WLM concurrency at 50, so beyond that queries queue rather than parallelize |
| `error_map` | 11 SQLSTATEs + 1 exception | `XX000` is **deliberately unmapped**: it is Redshift's catch-all internal error and is also the state for serializable isolation failures, which AWS says to retry — mapping it to `write_rejected` would turn a retryable conflict fatal |

## Caveats

- AWS IAM authentication is not implemented (username/password only).
- **Duplicate conflict keys inside one batch can land silently.** The CDK deliberately does not collapse duplicate `conflict_keys` within an upsert batch, expecting the destination to fail loudly. Redshift only half-does: if the target already holds the key, MERGE raises *"Found multiple matches to update the same tuple"* (the intended loud failure), but if it does not, `WHEN NOT MATCHED THEN INSERT` fires once per source row and lands both — and Redshift's `PRIMARY KEY` is informational-only and never enforced, so nothing rejects the duplicate. The insert-only branch behaves the same way, so the two are at least consistent.
- **`pk_not_enforced` cannot be set on this dialect, though Redshift never enforces PRIMARY KEY.** The CDK conflates two meanings in that one flag: `SqlDialect.pk_clause` appends BigQuery's `NOT ENFORCED` qualifier (which Redshift's parser rejects) *and* `generic.py` uses it to downgrade insert-mode retry semantics to at-least-once. Consequence today: `retry_semantics` reports `EXACTLY_ONCE` for Redshift insert streams, claiming a structural backstop the system does not provide. Fixing it needs a CDK-side split of the flag, not a connector change.
- Redshift speaks the PostgreSQL wire protocol, but AWS does not test or support generic PostgreSQL drivers; this connector uses Amazon's `redshift_connector` driver.
- Redshift has no `ON CONFLICT`. Upsert uses `MERGE INTO` with the **stage table itself** as the source. Because the source is a real table rather than a subquery, the two constraints that shaped the previous implementation are gone: Redshift's rejection of bind parameters inside a `USING` subquery (`XX000 ... queryVoltDecorrCSQ`) and the `42804` implicit-cast failure on literal source rows cannot arise, so no batch value is ever rendered into SQL text. The target is never aliased (the grammar offers no alias slot; `MERGE INTO t AS x` and `MERGE INTO t x` both fail); the stage is aliased `src`; target columns in the INSERT list are unqualified.
- **When every landed column is a conflict key, the upsert degrades to insert-only** — matched rows untouched, never an error, per the CDK contract. It renders as `INSERT ... SELECT ... WHERE NOT EXISTS`, *not* as a MERGE: Redshift documents no insert-only MERGE (the two `WHEN` clauses are one indivisible grammar unit, there is no `WHEN MATCHED THEN DO NOTHING` and no conditional `WHEN MATCHED AND <pred>`), and the CDK's tier-1 conformance explicitly asserts no `WHEN MATCHED` token in this case — so the self-assign `UPDATE SET key = src.key` workaround would fail CI as well as rewriting every matched row on a columnar store.
- `VARCHAR` is defined in **bytes**, not characters. A value over 65,535 bytes (multibyte UTF-8 costs up to 4 bytes/char) fails the write; Redshift has no unbounded text type.
- Known unexercised gaps in the write path, all loud failures rather than silent corruption: `float('nan')`/`inf` render as bare identifiers; `CAST(varchar AS SUPER)` is invalid (SUPER ingestion needs `JSON_PARSE()`); a reflected `SUPER`/`VARBYTE` column may come back as `NullType`; `UInt64` → `BIGINT` is lossy above 2^63-1.
- **Open risk (not connector-fixable):** `information_schema.columns.data_type` returns bare, unparameterized tokens (`numeric`, not `DECIMAL(8,2)`) with precision/scale in separate columns. If the builtin discovery strategy does not compose the parameterized form before handing the native to the read map, every decimal collapses to the `Decimal128(18, 0)` fallback and declared scale is silently lost. Needs an engine-side check before this connector is marked verified.
