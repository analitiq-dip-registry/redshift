# Amazon Redshift

[![Status: unverified](https://img.shields.io/badge/status-unverified-orange)](https://github.com/analitiq-dip-registry)
[![Latest release](https://img.shields.io/github/v/release/analitiq-dip-registry/redshift)](https://github.com/analitiq-dip-registry/redshift/releases)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

Read schemas and tables from an Amazon Redshift cloud data warehouse.

## What is this?

This is a **connector** — a configuration that defines how to authenticate with Amazon Redshift and what data is available for reading. It does not move data by itself. Instead, it is used by the [Analitiq](https://analitiq-app.com) data integration platform or the open-source `analitiq-dip-registry` engine to set up data pipelines.

## How to use this connector

There are two ways to use this connector:

### Option 1 — Analitiq Cloud (no setup required)

All connectors from this registry are automatically available on [analitiq-app.com](https://analitiq-app.com). Simply log in, select the connector, and follow the on-screen instructions to connect your account.

### Option 2 — Open Source (self-hosted)

All connectors are open source and free to use. To get started:

1. Clone the [analitiq-dip-registry](https://github.com/analitiq-dip-registry) repository
2. Install the Claude plugin `analitiq-plugin-dataflow`
3. Launch Claude in the root directory of `analitiq-dip-registry`
4. Tell it: *"I need to move data from X to Y"*

The `analitiq-plugin-dataflow` plugin will automatically fetch the required connectors from the [Analitiq DIP Registry](https://github.com/analitiq-dip-registry) and set up the data flow pipeline for you.

## Prerequisites

Before you can connect, you need:

- A running Amazon Redshift cluster and its endpoint (host) and port (default `5439`).
- The name of a database on the cluster (e.g. `dev`).
- A database user with a password and `SELECT` privileges on the schemas/tables you want to read.
- Network access to the cluster (a publicly accessible cluster, or connectivity via VPC/VPN/SSH as appropriate), and the cluster's security group allowing inbound traffic on the Redshift port from your client.

## Authentication

This connector authenticates with a **database username and password** over TLS, using the SQLAlchemy `redshift+redshift_connector` dialect (Amazon's official `redshift_connector` Python driver).

You provide the host, port, database, username, and password, and choose an SSL mode (`none`, `prefer`, `require`, `verify-ca`, or `verify-full`; the default is `verify-ca`). For `verify-ca` / `verify-full` you may supply a CA certificate (PEM) to verify the server.

AWS IAM temporary-credential authentication is supported by Redshift but is **not** implemented in this connector version.

### How to get your credentials

1. Open the [Amazon Redshift console](https://console.aws.amazon.com/redshiftv2/) and select your cluster.
2. Copy the cluster **endpoint** — it has the form `host:port/database`, e.g. `my-cluster.abc123.us-west-1.redshift.amazonaws.com:5439/dev`.
3. Use an existing database user, or create one and grant it read access:
   ```sql
   CREATE USER analitiq_reader PASSWORD 'your-strong-password';
   GRANT USAGE ON SCHEMA public TO analitiq_reader;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO analitiq_reader;
   ```
4. Ensure the cluster's security group allows inbound connections from your client on the Redshift port (default `5439`).

## Available Endpoints

This is a **database** connector — it has no fixed endpoint list. After you connect, the available schemas and tables are discovered automatically from Redshift's `information_schema`, and you select which tables to read. The catalog schemas `information_schema`, `pg_catalog`, and `pg_internal` are excluded from discovery.

## Limitations

- **Authentication** — Username/password only; AWS IAM temporary credentials are not supported in this version.
- **Driver** — Redshift speaks the PostgreSQL wire protocol, but AWS does not test or support generic PostgreSQL drivers; this connector uses Amazon's `redshift_connector` driver.
- **SSL modes** — The `redshift_connector` driver only honors `verify-ca` / `verify-full` at the driver level; other canonical `ssl_mode` values are reconciled by the runtime.
- **Concurrency** — Bounded by the cluster's `max_connections` setting and WLM query-slot configuration.
- **Type mapping** — Redshift-specific types are mapped as follows: `SUPER` → JSON; `GEOMETRY` / `GEOGRAPHY` / `HLLSKETCH` / `VARBYTE` → Binary; `INTERVAL` types → text; `TIMETZ` → Time.

## For AI agents

This connector includes `CLAUDE.md` and `AGENTS.md` files — machine-readable references used by AI agents and agentic frameworks. They document authentication types, connection inputs, post-auth steps, and any caveats for programmatic use. Both files are kept identical — `CLAUDE.md` is for Claude Code, `AGENTS.md` is for other agent frameworks.

## Create a connector to any system

You can create a new connector to any API or database using Claude and the Analitiq connector builder plugin:

1. Install [Claude Code](https://claude.ai/code)
2. Install the connector builder plugin:
   ```
   claude plugin add analitiq-dip-registry/analitiq-plugin-connector-builder
   ```
3. Launch Claude and say: *"I want to create a connector for [system name]"*
4. The plugin will interview you about the system, research its API documentation, and generate the full connector with all required files

No coding required — the plugin handles authentication research, endpoint schema generation, and file creation automatically.

![Example of Claude building a connector](media/example_1.png)

## Contributing

All connectors in this registry are community-maintained and live at [github.com/analitiq-dip-registry](https://github.com/analitiq-dip-registry). To add new endpoints or improve an existing connector, install the [connector builder plugin](https://github.com/analitiq-dip-registry/analitiq-plugin-connector-builder) and follow its instructions.

## Links

- [Amazon Redshift documentation](https://docs.aws.amazon.com/redshift/)
- [Amazon Redshift data types](https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html)
- [Analitiq Cloud](https://analitiq-app.com)
- [Analitiq Engine (open source)](https://github.com/analitiq-ai/analitiq-engine)
- [Analitiq DIP Registry (open source)](https://github.com/analitiq-dip-registry)
