# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import typing as t
from uuid import uuid4

from sqlglot import exp, parse_one

from sqlmesh import CustomMaterialization, Model

if t.TYPE_CHECKING:
    from sqlmesh import QueryOrDF


class PostgresMaterializedView(CustomMaterialization):
    """Materialize a SQLMesh model as a native PostgreSQL materialized view."""

    NAME = "postgres_materialized_view"

    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        render_kwargs: dict[str, t.Any],
        **kwargs: t.Any,
    ) -> None:
        if not isinstance(query_or_df, exp.Query):
            raise TypeError("PostgreSQL materialized views require a SQL query")

        existing = self.adapter.get_data_object(table_name)
        if existing and existing.type.is_materialized_view:
            self.adapter.execute(f"REFRESH MATERIALIZED VIEW {table_name}")
            return

        table = exp.to_table(table_name, dialect=self.adapter.dialect)
        create = exp.Create(
            this=table,
            kind="VIEW",
            expression=query_or_df,
            properties=exp.Properties(expressions=[exp.MaterializedProperty()]),
        )
        if not existing:
            self.adapter.execute(create)
            return

        # Skip/empty backfills create table placeholders and virtual views before
        # insert is called. Replace the placeholder without losing those views.
        backup = table.copy()
        backup.set("this", exp.to_identifier(f"sqlmesh_backup_{uuid4().hex}"))
        lookup_table = table.copy()
        lookup_table.set("catalog", None)
        relation = exp.Literal.string(lookup_table.sql(dialect=self.adapter.dialect)).sql(
            dialect=self.adapter.dialect
        )
        with self.adapter.transaction():
            dependent_views = self.adapter.fetchall(
                f"""
                SELECT DISTINCT namespace.nspname, view_object.relname,
                                pg_get_viewdef(view_object.oid, true)
                FROM pg_depend AS dependency
                JOIN pg_rewrite AS rewrite ON rewrite.oid = dependency.objid
                JOIN pg_class AS view_object ON view_object.oid = rewrite.ev_class
                JOIN pg_namespace AS namespace ON namespace.oid = view_object.relnamespace
                WHERE dependency.classid = 'pg_rewrite'::regclass
                  AND dependency.refobjid = to_regclass({relation})
                  AND view_object.relkind = 'v'
                ORDER BY namespace.nspname, view_object.relname
                """
            )
            self.adapter.rename_table(table, backup)
            self.adapter.execute(create)
            for schema, name, definition in dependent_views:
                # PostgreSQL's adapter drops views when replacing them. Use
                # CREATE OR REPLACE directly to retain downstream dependencies.
                self.adapter.execute(
                    exp.Create(
                        this=exp.Table(
                            this=exp.to_identifier(name, quoted=True),
                            db=exp.to_identifier(schema, quoted=True),
                        ),
                        kind="VIEW",
                        expression=parse_one(definition, dialect=self.adapter.dialect),
                        replace=True,
                    )
                )
            self.adapter.drop_table(backup)

    def delete(self, name: str, **kwargs: t.Any) -> None:
        cascade = kwargs.pop("cascade", False)
        existing = self.adapter.get_data_object(name)
        if existing and existing.type.is_materialized_view:
            self.adapter.drop_view(name, materialized=True, cascade=cascade)
        else:
            self.adapter.drop_table(name, cascade=cascade)
