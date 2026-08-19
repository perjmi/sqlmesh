# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import pytest

from accuracy_tiers import AccuracyScenario, selected_accuracy_scenarios
from dual_planner import PROJECT_PATH, database_signature, plan, reset_database
from generate_model_graph import chunk_model_graph, generate_model_graph


@pytest.mark.parametrize(
    "scenario",
    selected_accuracy_scenarios(),
    ids=lambda scenario: scenario.test_id,
)
def test_reference_and_candidate_are_equivalent(
    scenario: AccuracyScenario, accuracy_stack
) -> None:
    graph = generate_model_graph(
        scenario.width,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=scenario.seed,
    )
    model_names = (*graph.input_models, *(model for layer in graph.layers for model in layer))

    reset_database("reference")
    reset_database("candidate")
    plan("reference")
    if scenario.candidate_mode == "full":
        plan("candidate")
    else:
        for chunk in chunk_model_graph(graph):
            plan("candidate", chunk)

    reference_signature = database_signature("reference", model_names)
    candidate_signature = database_signature("candidate", model_names)
    assert candidate_signature == reference_signature

    assert "No changes to plan" in plan("reference")
    assert "No changes to plan" in plan("candidate")
