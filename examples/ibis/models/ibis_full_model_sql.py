import os

import ibis  # type: ignore

from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model


@model(
    "ibis.ibis_full_model_sql",
    is_sql=True,
    kind="FULL",
    audits=["assert_positive_order_ids"],
    description="This model uses ibis to generate and return a SQL string",
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    # create table reference
    incremental_model = ibis.table(
        name="incremental_model",
        schema={"id": "int32", "item_id": "int32", "ds": "varchar"},
        catalog=os.getenv("IBIS_CATALOG", "local") or None,
        database=os.getenv("IBIS_DATABASE", "ibis"),
    )

    # build query
    count = incremental_model.id.nunique()
    aggregate = incremental_model.group_by("item_id").aggregate(num_orders=count)
    query = aggregate.order_by("item_id")

    return ibis.to_sql(query, dialect=evaluator.dialect)
