# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import pytest

import dual_planner


@pytest.mark.parametrize("changed_value", (False, True), ids=("row_order", "data_change"))
def test_database_signature_ignores_row_order_and_detects_data_changes(
    monkeypatch: pytest.MonkeyPatch, changed_value: bool
) -> None:
    reference_rows = ("generated.a|3|6|6|12.000000|3|1|3", "generated.b|2|3|3|9.000000|2|1|2")
    candidate_rows = (
        reference_rows[1],
        reference_rows[0].replace("12.000000", "13.000000") if changed_value else reference_rows[0],
    )

    def query(target: str, sql: str) -> tuple[str, ...]:
        if "information_schema.columns" in sql:
            return ("a|1|entity_id|bigint|64|0", "b|1|entity_id|bigint|64|0")
        if "pg_matviews" in sql:
            return ("0",)
        return reference_rows if target == "reference" else candidate_rows

    monkeypatch.setattr(dual_planner, "query", query)
    model_names = ("generated.a", "generated.b")
    reference = dual_planner.database_signature("reference", model_names)
    candidate = dual_planner.database_signature("candidate", model_names)
    assert (candidate == reference) is not changed_value
