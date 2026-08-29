# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dual_planner import (
    PROJECT_PATH,
    assert_state_equal,
    database_signature,
    plan,
    plan_result,
    query,
    reset_database,
    state_signature,
)
from generate_model_graph import generate_model_graph
from randomgraph_mutations import write_randomgraph_audit
from randomgraph_scenarios import write_full_model


def test_audit_failure_preserves_environment_and_retry_reaches_matching_state(
    accuracy_stack,
) -> None:
    write_randomgraph_audit()
    graph = generate_model_graph(
        2,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=131,
    )
    audited_model = "generated.atomic_model"
    write_full_model(audited_model, row_count=1, with_audit=True)
    model_names = (
        *graph.input_models,
        *(model for layer in graph.layers for model in layer),
        audited_model,
    )
    reset_database("reference")
    reset_database("candidate")
    plan("reference", permutation_seed=13101)
    plan("candidate", permutation_seed=13101)
    assert_state_equal()
    reference_environment_before = state_signature("reference")["environments"]
    candidate_environment_before = state_signature("candidate")["environments"]

    write_full_model(audited_model, row_count=0, with_audit=True)
    reference_failure = plan_result("reference", permutation_seed=13102)
    candidate_failure = plan_result("candidate", permutation_seed=13102)
    assert reference_failure.returncode != 0
    assert candidate_failure.returncode != 0
    assert "audit" in f"{reference_failure.stdout}\n{reference_failure.stderr}".lower()
    assert "audit" in f"{candidate_failure.stdout}\n{candidate_failure.stderr}".lower()
    assert state_signature("reference")["environments"] == reference_environment_before
    assert state_signature("candidate")["environments"] == candidate_environment_before
    assert query("reference", f"SELECT DISTINCT row_count FROM {audited_model}") == ("1",)
    assert query("candidate", f"SELECT DISTINCT row_count FROM {audited_model}") == ("1",)

    write_full_model(audited_model, row_count=2, with_audit=True)
    plan("reference", permutation_seed=13103)
    plan("candidate", permutation_seed=13103)
    assert query("reference", f"SELECT DISTINCT row_count FROM {audited_model}") == ("2",)
    assert query("candidate", f"SELECT DISTINCT row_count FROM {audited_model}") == ("2",)
    assert database_signature("candidate", model_names) == database_signature(
        "reference", model_names
    )
    assert_state_equal()
