# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import pytest

from accuracy_tiers import MutationScenario, selected_mutation_scenarios
from dual_planner import (
    assert_model_rows_equal,
    assert_state_equal,
    audit,
    database_signature,
    plan,
    query,
    reset_database,
)
from generate_model_graph import generate_model_graph
from randomgraph_mutations import generate_model_mutations, write_randomgraph_audit


@pytest.mark.parametrize(
    "scenario",
    selected_mutation_scenarios(),
    ids=lambda scenario: scenario.test_id,
)
def test_random_model_mutations_preserve_planner_and_audit_parity(
    scenario: MutationScenario, accuracy_stack
) -> None:
    write_randomgraph_audit()
    graph = generate_model_graph(scenario.width, seed=scenario.graph_seed)
    model_names = (*graph.input_models, *(model for layer in graph.layers for model in layer))
    materialized_view_names = tuple(model for layer in graph.layers for model in layer)
    mutations = generate_model_mutations(graph, seed=scenario.mutation_seed)
    audit_mutation = next(mutation for mutation in mutations if mutation.kind == "audit")
    predeployment_kinds = {"audit", "metadata", "schema"}

    # The main-branch custom materialization cannot replace PostgreSQL materialized views after
    # audit, owner, or schema changes propagate downstream, so randomize those before deployment.
    for mutation in mutations:
        if mutation.kind in predeployment_kinds:
            mutation.apply()

    reset_database("reference")
    reset_database("candidate")
    plan("reference")
    plan("candidate")
    assert database_signature("candidate", model_names) == database_signature(
        "reference", model_names
    )
    assert_model_rows_equal(materialized_view_names)
    assert_state_equal()

    for mutation in (
        mutation for mutation in mutations if mutation.kind not in predeployment_kinds
    ):
        mutation.apply()
        reference_plan = plan("reference")
        candidate_plan = plan("candidate")
        assert "No changes to plan" not in reference_plan, mutation
        assert "No changes to plan" not in candidate_plan, mutation
        assert database_signature("candidate", model_names) == database_signature(
            "reference", model_names
        ), mutation
        assert_model_rows_equal(materialized_view_names)
        assert_state_equal()

    audit_model = audit_mutation.model_name
    reference_pass = audit("reference", audit_model)
    candidate_pass = audit("candidate", audit_model)
    assert reference_pass == candidate_pass
    assert reference_pass.returncode == 0
    assert reference_pass.found == reference_pass.passed == 1
    assert reference_pass.failed == reference_pass.errors == 0

    corrupt_row = f"""
        UPDATE {audit_model}
        SET row_count = 0
        WHERE entity_id = (SELECT MIN(entity_id) FROM {audit_model})
    """
    for target in ("reference", "candidate"):
        query(target, corrupt_row)

    reference_failure = audit("reference", audit_model)
    candidate_failure = audit("candidate", audit_model)
    assert reference_failure == candidate_failure
    assert reference_failure.returncode != 0
    assert reference_failure.found == reference_failure.failed == 1
    assert reference_failure.errors == 1
    assert len(reference_failure.result_counts) == 1
    assert reference_failure.result_counts[0] > 0

    for target in ("reference", "candidate"):
        query(target, f"UPDATE {audit_model} SET row_count = 1 WHERE row_count = 0")
    reference_repaired = audit("reference", audit_model)
    candidate_repaired = audit("candidate", audit_model)
    assert reference_repaired == candidate_repaired
    assert reference_repaired.returncode == 0
    assert reference_repaired.found == reference_repaired.passed == 1

    assert "No changes to plan" in plan("reference")
    assert "No changes to plan" in plan("candidate")
    assert_state_equal()
