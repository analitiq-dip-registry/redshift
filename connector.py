"""Amazon Redshift connector — dialect + connector class for the Analitiq CDK.

Everything Redshift-specific lives here, in the connector package: the
``redshift.redshift_connector`` SQLAlchemy flavour, the ``MERGE INTO``
upsert (Redshift has no ``ON CONFLICT``), the ``redshift_connector``
two-parameter TLS vocabulary (``ssl`` + ``sslmode``), and the
``CREATE SCHEMA`` pre-DDL. Column types for the write direction are
governed entirely by ``definition/type-map-write.json``; this module
ships no Python type-rendering table. The CDK base
(``GenericSQLConnector`` / ``SqlDialect``) is vendor-neutral and never
branches on this system.

Redshift has no first-class ADBC driver and no Arrow Flight SQL endpoint,
and its native bulk load (``COPY FROM S3``) needs external S3 staging and
IAM, so the connector takes the synchronous SQLAlchemy transport
(``redshift+redshift_connector``); the engine runs the sync DBAPI on its
sync engine path.

Registered under connector_id ``redshift`` via the package entry points
(``analitiq.source_connectors`` / ``analitiq.destination_connectors``).
"""

from __future__ import annotations

import re
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any, Dict, List

from sqlalchemy import text

from cdk.sql.dialects import SqlDialect
from cdk.sql.generic import GenericSQLConnector


class RedshiftDialect(SqlDialect):
    """Redshift SQL strategy: ANSI quoting, ``public`` default schema,
    ``MERGE INTO`` upsert, redshift_connector TLS."""

    name = "redshift"

    #: sqlalchemy-redshift registers one SA dialect per DBAPI flavour; the
    #: bare ``redshift`` entry point is the psycopg2 one. The read path
    #: resolves its SA dialect through the registry by this name, so it
    #: must name the redshift_connector flavour the DSN connects with.
    sqlalchemy_registry_name = "redshift.redshift_connector"

    system_schemas = ("information_schema", "pg_catalog", "pg_internal")
    supports_upsert_sqlalchemy = True

    #: One SQLAlchemy ``text()`` bind token. Mirrors BIND_PARAMS in
    #: sqlalchemy.sql.compiler (``:name`` / ``:$name``) without its
    #: lookbehind — see _escape_bind_tokens.
    _BIND_TOKEN = re.compile(r":([\w\$]+)(?![:\w\$])")

    # ---- schema semantics --------------------------------------------------
    def schema_is_implicit_default(self, schema_name: str) -> bool:
        return not schema_name or schema_name.lower() == "public"

    def sqlalchemy_pre_ddl(self, schema_name: str) -> List[str]:
        # ``public`` always exists; any other schema must be created before
        # ``MetaData.create_all`` references it.
        if schema_name and schema_name.lower() != "public":
            return [f"CREATE SCHEMA IF NOT EXISTS {self.quote_ident(schema_name)}"]
        return []

    # ---- SQLAlchemy write path ---------------------------------------------
    def build_sqlalchemy_upsert(
        self,
        table: Any,
        records: List[Dict[str, Any]],
        conflict_keys: List[str],
    ) -> Any:
        """Redshift INSERT-or-UPDATE: MERGE against a constant subquery.

        No target alias (Redshift rejects one); batch inlined as a
        (SELECT ... UNION ALL SELECT ...) src subquery of CAST literals.
        Both WHEN clauses are required by Redshift's standard MERGE form,
        so an all-key table cannot upsert (use write_mode 'insert').
        """
        columns = list(records[0].keys())
        update_cols = [c for c in columns if c not in conflict_keys]
        if not update_cols:
            raise ValueError(
                f"{self.name} MERGE upsert requires at least one non-key "
                f"column to update (all columns are conflict keys); use "
                f"write_mode 'insert' instead"
            )
        target = self.quote_qualified(table.schema or "", table.name)
        column_types = {c: str(table.c[c].type) for c in columns if c in table.c}
        selects = []
        for row_index, record in enumerate(records):
            parts = []
            for column in columns:
                rendered = self._literal(record[column])
                cast_type = column_types.get(column)
                if cast_type:
                    rendered = f"CAST({rendered} AS {cast_type})"
                if row_index == 0:
                    parts.append(f"{rendered} AS {self.quote_ident(column)}")
                else:
                    parts.append(rendered)
            selects.append("SELECT " + ", ".join(parts))
        src = " UNION ALL ".join(selects)
        on_clause = " AND ".join(
            f"{target}.{self.quote_ident(k)} = src.{self.quote_ident(k)}"
            for k in conflict_keys
        )
        set_clause = ", ".join(
            f"{self.quote_ident(c)} = src.{self.quote_ident(c)}" for c in update_cols
        )
        insert_cols = ", ".join(self.quote_ident(c) for c in columns)
        insert_vals = ", ".join(f"src.{self.quote_ident(c)}" for c in columns)
        return text(
            self._escape_bind_tokens(
                f"MERGE INTO {target} USING ({src}) src ON {on_clause} "
                f"WHEN MATCHED THEN UPDATE SET {set_clause} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
            )
        )

    @classmethod
    def _escape_bind_tokens(cls, sql: str) -> str:
        """Backslash-escape every ``:token`` so ``text()`` leaves it alone.

        The batch is fully inlined, so a record value carrying a colon
        (``'see :note'``) would otherwise be read by ``text()`` as a bind
        parameter and fail the execute — the handler calls
        ``conn.execute(stmt)`` with no parameter map. Backslash is the
        documented escape: the compiler's BIND_PARAMS lookbehind excludes
        it, and BIND_PARAMS_ESC strips the backslash again at compile
        time, so nothing but the original colon reaches Redshift.
        """
        return cls._BIND_TOKEN.sub(r"\\:\1", sql)

    @staticmethod
    def _literal(value: Any) -> str:
        """Render one pre-cast record value as a Redshift SQL literal.

        Values arrive through the engine's schema-contract cast, so the
        type set is closed. Strings double embedded quotes (Redshift
        defaults to standard_conforming_strings, so backslashes are
        literal); binary renders via from_hex for VARBYTE.
        """
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, Decimal)):
            return str(value)
        if isinstance(value, float):
            return repr(value)
        if isinstance(value, datetime):
            return f"'{value.isoformat(sep=' ')}'"
        if isinstance(value, (date, dt_time)):
            return f"'{value.isoformat()}'"
        if isinstance(value, (bytes, bytearray)):
            return f"from_hex('{bytes(value).hex()}')"
        return "'" + str(value).replace("'", "''") + "'"

    # ---- TLS ----------------------------------------------------------------
    def build_tls_connect_args(self, mode: str, ca_pem: str | None) -> Dict[str, Any]:
        """redshift_connector TLS spans two connect parameters: ssl + sslmode.

        The declared ssl_mode enum is verify-ca / verify-full — the only
        modes the redshift_connector driver documents for its ``sslmode``
        parameter, both verifying the server certificate against the
        driver's bundled Amazon Trust CA. The driver exposes no connect
        parameter for a user-supplied CA bundle, so a non-empty
        tls.ca_certificate is rejected loudly rather than silently ignored
        (verifying against the wrong roots would be worse).

        The broader libpq vocabulary (disable/allow/prefer/require) is not
        offered for new connections, but is still reconciled here so any
        connection stored against the connector's prior ADBC/libpq
        vocabulary keeps resolving: none/disable -> TLS off;
        allow/prefer/require -> TLS on with verify-ca.
        """
        if ca_pem:
            raise ValueError(
                f"{self.name}: redshift_connector verifies against its bundled "
                f"Amazon Trust CA and has no connect parameter for a custom "
                f"tls.ca_certificate bundle"
            )
        if mode in ("none", "disable"):
            return {"ssl": False}
        if mode in ("allow", "prefer", "require", "verify-ca"):
            return {"ssl": True, "sslmode": "verify-ca"}
        if mode == "verify-full":
            return {"ssl": True, "sslmode": "verify-full"}
        raise ValueError(
            f"{self.name} tls.mode {mode!r} not recognized; expected one of: "
            "verify-ca, verify-full"
        )


class RedshiftConnector(GenericSQLConnector):
    """Redshift connector: the CDK SQL base wired to the redshift dialect."""

    dialect_class = RedshiftDialect
