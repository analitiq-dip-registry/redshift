---
name: redshift
description: >
  Amazon Redshift cloud data warehouse — read schemas and tables over SQLAlchemy
type: database
---

# Amazon Redshift

Amazon Redshift is AWS's petabyte-scale cloud data warehouse. This connector reads schemas and tables from a Redshift cluster using the SQLAlchemy `redshift+redshift_connector` dialect (Amazon's official `redshift_connector` Python driver). Tables and columns are discovered at runtime via `information_schema`.

## Authentication

### Database (username + password)
- Client app required: no
- Transport: `sqlalchemy`, driver `redshift+redshift_connector`
- DSN: `redshift+redshift_connector://{username}:{password}@{host}:{port}/{database}`
- TLS: `ssl_mode` is one of `none`, `prefer`, `require`, `verify-ca`, `verify-full` (default `verify-ca`); optional `ssl_ca_certificate` (PEM) for the `verify-ca` / `verify-full` modes.

AWS IAM temporary-credential authentication is supported by Redshift but is **not** implemented in this connector version.

### Connection inputs
| Input | Phase | Storage | Required | Notes |
|-------|-------|---------|----------|-------|
| `host` | pre_auth | connection.parameters | yes | Cluster endpoint, e.g. `cluster.xxxx.us-west-1.redshift.amazonaws.com` |
| `port` | pre_auth | connection.parameters | yes | Default `5439` |
| `database` | pre_auth | connection.parameters | yes | e.g. `dev` |
| `ssl_mode` | pre_auth | connection.parameters | yes | Default `verify-ca` |
| `ssl_ca_certificate` | pre_auth | secrets | no | PEM bundle for CA verification |
| `username` | auth | connection.parameters | yes | |
| `password` | auth | secrets | yes | Secret |

## Post-Auth Steps

Resource discovery runs automatically on activation (`information_schema` strategy). The connector lists schemas/tables and emits a per-connection `type_map`. The schemas `information_schema`, `pg_catalog`, and `pg_internal` are excluded from discovery.

## Available Endpoints

This is a database connector — it has no static endpoints. Tables and columns are discovered at runtime from `information_schema` and selected per connection.

## Rate Limits

None imposed by the connector. Concurrency is bounded by the cluster's `max_connections` setting and WLM query-slot configuration.

## Caveats

- AWS IAM authentication is not implemented (username/password only).
- Redshift speaks the PostgreSQL wire protocol, but AWS does not test or support generic PostgreSQL drivers; this connector uses Amazon's `redshift_connector` driver.
- The `redshift_connector` driver only honors `verify-ca` / `verify-full` at the driver level; other canonical `ssl_mode` values are reconciled by the runtime materializer.
- Type mapping notes: `SUPER` → Json; `GEOMETRY` / `GEOGRAPHY` / `HLLSKETCH` / `VARBYTE` → Binary; `INTERVAL` types → Utf8; `TIMETZ` → Time64.
