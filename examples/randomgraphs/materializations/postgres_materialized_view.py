# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import typing as t

from sqlglot import exp

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

        if existing:
            self.adapter.drop_table(table_name)

        self.adapter.execute(
            exp.Create(
                this=exp.to_table(table_name, dialect=self.adapter.dialect),
                kind="VIEW",
                expression=query_or_df,
                properties=exp.Properties(expressions=[exp.MaterializedProperty()]),
            )
        )

    def delete(self, name: str, **kwargs: t.Any) -> None:
        cascade = kwargs.pop("cascade", False)
        existing = self.adapter.get_data_object(name)
        if existing and existing.type.is_materialized_view:
            self.adapter.drop_view(name, materialized=True, cascade=cascade)
        else:
            self.adapter.drop_table(name, cascade=cascade)
