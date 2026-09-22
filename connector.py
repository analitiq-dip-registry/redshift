"""Amazon Redshift connector — dialect + connector class for the Analitiq CDK.

Everything Redshift-specific lives here, in the connector package: the
``redshift.redshift_connector`` SQLAlchemy flavour, the stage-then-merge
write hooks (``CREATE TEMPORARY TABLE ... (LIKE ...)`` for the stage and
``MERGE INTO`` for the upsert — Redshift has no ``ON CONFLICT``), the
``redshift_connector`` two-parameter TLS vocabulary (``ssl`` + ``sslmode``),
and the ``CREATE SCHEMA`` pre-DDL. Column types for the write direction are
governed entirely by ``definition/type-map.json`` (its ``write`` rules); this module ships
no Python type-rendering table. The CDK base (``GenericSQLConnector`` /
``SqlDialect``) is vendor-neutral and never branches on this system.

Every write is stage-then-merge (ADR sql-write-path-v2): the CDK creates the
stage from ``stage_table_sql``, lands the batch into it with bound
parameters, and runs exactly one mode statement from stage to target — its
own ANSI anti-join for ``insert``, its plain append for ``truncate_insert``,
and this dialect's ``merge_statement_sql`` for ``upsert``. Both hooks
compose dialect-quoted identifiers only: the merge source is a real table,
so no batch value is ever rendered into SQL text.

Redshift has no first-class ADBC driver and no Arrow Flight SQL endpoint,
and its native bulk load (``COPY``) reads only from S3/EMR/DynamoDB/SSH —
never from the client connection — so there is no stdin bulk path to
implement: the connector takes the synchronous SQLAlchemy transport
(``redshift+redshift_connector``), declares no mechanism in
``sql_capabilities.bulk_load``, and batches land via executemany. The
engine runs the sync DBAPI on its sync engine path.

Registered under connector_id ``redshift`` via the package entry points
(``analitiq.source_connectors`` / ``analitiq.destination_connectors``).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.generic import GenericSQLConnector

#: Stage alias in the stage-to-target statements. Redshift's MERGE grammar
#: offers an alias slot for the source only — ``MERGE INTO t AS x`` and
#: ``MERGE INTO t x`` both fail — so the target is spelled out in full and
#: only the stage is aliased.
_SRC = "src"

#: Target alias, used only inside the insert-only degradation's
#: ``NOT EXISTS`` subquery: that statement is a plain INSERT, not a MERGE,
#: so aliasing the target is allowed there.
_TGT = "tgt"


class RedshiftDialect(SqlDialect):
    """Redshift SQL strategy: ANSI quoting, ``public`` default schema,
    stage-then-merge writes, redshift_connector TLS."""

    name = "redshift"

    #: sqlalchemy-redshift registers one SA dialect per DBAPI flavour; the
    #: bare ``redshift`` entry point is the psycopg2 one, and it compiles
    #: named ``%(name)s`` parameters that redshift_connector rejects
    #: ("Only %s and %% are supported in the query"). The read path
    #: resolves its SA dialect through the registry by this name, so it
    #: must name the redshift_connector flavour the DSN connects with.
    sqlalchemy_registry_name = "redshift.redshift_connector"

    system_schemas = ("information_schema", "pg_catalog", "pg_internal")

    #: AWS documents Redshift identifiers as 1-127 bytes, and an over-long
    #: name is truncated SILENTLY rather than rejected, so two generated
    #: stage names sharing a 127-byte prefix would collide invisibly. The
    #: same fact is declared as
    #: ``sql_capabilities.limits.max_identifier_len``, which is the budget
    #: the CDK composes stage names within; this class attribute is the
    #: fallback for when no declaration is bound (an unbound dialect, the
    #: standalone control-plane path) and the ceiling the CDK's rendering
    #: conformance asserts a composed name against. Left at the base's 63
    #: the two channels would contradict each other the moment a long
    #: target name used the declared budget.
    max_identifier_length = 127

    # ---- schema semantics --------------------------------------------------
    def schema_is_implicit_default(self, schema_name: str) -> bool:
        return not schema_name or schema_name.lower() == "public"

    def sqlalchemy_pre_ddl(self, schema_name: str) -> list[str]:
        # ``public`` always exists; any other schema must be created before
        # ``MetaData.create_all`` references it.
        if schema_name and schema_name.lower() != "public":
            return [f"CREATE SCHEMA IF NOT EXISTS {self.quote_ident(schema_name)}"]
        return []

    # ---- stage-then-merge write path ---------------------------------------
    def stage_table_sql(
        self, stage: TableAddress, target: TableAddress, *, temp: bool
    ) -> str:
        """``CREATE [TEMPORARY] TABLE`` *stage* shaped like *target*.

        ``LIKE`` sits inside the column-list parentheses — they are
        required, because LIKE is an alternative inside the column-list
        production — and this is the statement AWS's own stage-then-merge
        runbook writes. AWS also recommends LIKE over ``CREATE TABLE AS``
        for staging: LIKE inherits the parent's distribution style and
        sort keys (CTAS does not), so the stage collocates with the target
        and the mode statement's join stays local. Primary and foreign
        keys are not copied, which a stage does not need.

        ``INCLUDING DEFAULTS`` copies the parent's default expressions.
        The documented default is EXCLUDING DEFAULTS, which would leave a
        stage column NOT NULL with no default whenever the target carries
        a NOT NULL DEFAULT column the batch does not land (the merge
        contract's "physical columns added out-of-band"), and the landing
        INSERT — which binds only the landed columns — would fail on it. A
        copied default never masks landed data: the executemany landing
        binds every landed column explicitly, so an explicit NULL stays
        NULL.

        Under the connector's declared temp scope the CDK hands this hook
        a schema-less stage address, so ``quote_table`` renders the bare
        identifier Redshift requires ("If you are creating a temporary
        table, you can't specify a schema name, because temporary tables
        exist in a special schema"). Going through the same sink as the
        CDK's own ``DROP TABLE IF EXISTS`` also keeps create and drop on
        one spelling.
        """
        create = "CREATE TEMPORARY TABLE" if temp else "CREATE TABLE"
        return (
            f"{create} {self.quote_table(stage)} "
            f"(LIKE {self.quote_table(target)} INCLUDING DEFAULTS)"
        )

    def merge_statement_sql(
        self,
        stage: TableAddress,
        target: TableAddress,
        conflict_keys: Sequence[str],
        columns: Sequence[str],
    ) -> str:
        """Render the upsert statement from *stage* to *target*.

        The source is the stage table itself — "the temporary or permanent
        table supplying the rows to merge into target_table" is a
        documented MERGE source — so no batch value is rendered into SQL
        text and Redshift's rejection of bind parameters inside a
        ``USING`` subquery (``XX000 ... queryVoltDecorrCSQ``) cannot
        arise. The target is never aliased: the grammar has no alias slot
        for it, and both ``MERGE INTO t AS x`` and ``MERGE INTO t x``
        fail. Target columns in the INSERT list are unqualified, per AWS
        ("Don't include the table name when specifying the target
        column"); the source side is qualified by the stage alias.

        When every landed column is a conflict key there is nothing to
        update, and the contract asks for the insert-only degradation —
        matched rows untouched, never an error. Redshift documents no
        insert-only MERGE: the two WHEN clauses are one indivisible
        grammar unit, there is no ``WHEN MATCHED THEN DO NOTHING`` and no
        conditional ``WHEN MATCHED AND <predicate>``, and the only
        documented matched actions are UPDATE and DELETE. So the
        degradation renders as ``INSERT ... SELECT ... WHERE NOT EXISTS``
        instead: every construct is documented, the correlation is
        single-level and matches none of AWS's six documented
        un-decorrelatable patterns, and the MERGE-only rule forbidding the
        target as a subquery source does not apply to a plain INSERT. The
        alternative — ``WHEN MATCHED THEN UPDATE SET <key> = src.<key>`` —
        is valid syntax but rewrites every matched row on a columnar store
        for no semantic gain.

        Known exposure, not fixable in this hook: MERGE requires that rows
        in the target not match multiple source rows ("Found multiple
        matches to update the same tuple"). The CDK deliberately does not
        collapse duplicate conflict keys inside an upsert batch, so a
        source page carrying one key twice fails loudly when the target
        already holds that key — the intended loud failure — but inserts
        both rows when it does not, and Redshift never enforces PRIMARY
        KEY, so nothing rejects the duplicate.
        """
        key_set = set(conflict_keys)
        update_columns = [c for c in columns if c not in key_set]
        target_ref = self.quote_table(target)
        stage_ref = self.quote_table(stage)
        insert_list = ", ".join(self.quote_ident(c) for c in columns)
        source_list = ", ".join(f"{_SRC}.{self.quote_ident(c)}" for c in columns)
        if not update_columns:
            match = " AND ".join(
                f"{_TGT}.{self.quote_ident(k)} = {_SRC}.{self.quote_ident(k)}"
                for k in conflict_keys
            )
            # Dialect-quoted identifiers only; batch values never enter
            # this text (they reach the stage as bound parameters).
            return (
                f"INSERT INTO {target_ref} ({insert_list}) "  # nosec B608
                f"SELECT {source_list} FROM {stage_ref} {_SRC} "
                f"WHERE NOT EXISTS (SELECT 1 FROM {target_ref} {_TGT} "
                f"WHERE {match})"
            )
        on_clause = " AND ".join(
            f"{target_ref}.{self.quote_ident(k)} = {_SRC}.{self.quote_ident(k)}"
            for k in conflict_keys
        )
        set_clause = ", ".join(
            f"{self.quote_ident(c)} = {_SRC}.{self.quote_ident(c)}"
            for c in update_columns
        )
        # Identifiers only; see above.
        return (
            f"MERGE INTO {target_ref} USING {stage_ref} {_SRC} "  # nosec B608
            f"ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {set_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_list}) "
            f"VALUES ({source_list})"
        )

    # ---- TLS ----------------------------------------------------------------
    def build_tls_connect_args(self, mode: str, ca_pem: str | None) -> dict[str, Any]:
        """redshift_connector TLS spans two connect parameters: ssl + sslmode.

        This overrides the plural hook rather than the singular
        ``build_tls_connect_arg`` because the driver does not take its TLS
        configuration through one argument.

        The declared ssl_mode enum is verify-ca / verify-full — the only
        modes the redshift_connector driver documents for its ``sslmode``
        parameter, both verifying the server certificate against the
        driver's bundled Amazon Trust CA. The driver exposes no connect
        parameter for a user-supplied CA bundle, so a non-empty
        tls.ca_certificate is rejected loudly rather than silently ignored
        (verifying against the wrong roots would be worse). The driver's
        ``ssl_insecure`` flag is scoped to the IdP host certificate, not
        the database connection, and is never used as a TLS toggle here.

        The broader libpq vocabulary (disable/allow/prefer/require) is not
        offered for new connections, but is still reconciled here so any
        connection stored against the connector's prior ADBC/libpq
        vocabulary keeps resolving: none/disable -> TLS off;
        allow/prefer/require -> TLS on with verify-ca. Every reconciliation
        is strictness-increasing or honest.
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


class _RecoveredSqlstate(Exception):
    """Carries a SQLSTATE recovered from redshift_connector's error dict as a
    flat ``.sqlstate`` attribute, so it can be re-run through
    :meth:`~cdk.declarations.ErrorMap.match_exception`'s declared-codes lookup.
    Never raised — only ever passed for classification.
    """

    def __init__(self, sqlstate: str) -> None:
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


class RedshiftConnector(GenericSQLConnector):
    """Redshift connector: the CDK SQL base wired to the redshift dialect."""

    dialect_class = RedshiftDialect

    def classify_error(self, exc: BaseException) -> str | None:
        """Recover SQLSTATE from redshift_connector's raw error dict.

        redshift_connector's own exception classes (``redshift_connector.error``)
        carry no ``.sqlstate`` attribute — the wire protocol's SQLSTATE lands in
        ``args[0]["C"]`` of whichever class ``handle_ERROR_RESPONSE`` raises
        (``redshift_connector.core``). ``definition/connector.json``'s declared
        ``error_map`` has only ``key_attrs: ["sqlstate"]``, a flat attribute
        read, so it can never see it either — this is the CDK's documented
        code escape hatch for exactly that case, consulted only once the
        declared lookup misses: unwrap SQLAlchemy's one driver-wrapping hop
        (``.orig``) and re-run the connector's own declared ``error_map``
        against the recovered value — never a second codes table. (Keep this
        in sync with ``definition/connector.json``'s ``error_map`` and with
        CLAUDE.md's capabilities table — both describe the same declared
        block and drifted out of sync with each other once already.)
        """
        for member in (exc, getattr(exc, "orig", None)):
            args = getattr(member, "args", None)
            if not args or not isinstance(args[0], dict):
                continue
            sqlstate = args[0].get("C")
            if not sqlstate or self._error_map is None:
                continue
            match = self._error_map.match_exception(_RecoveredSqlstate(sqlstate))
            if match is not None:
                return match.category
        return None
