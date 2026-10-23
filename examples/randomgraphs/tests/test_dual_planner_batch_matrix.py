# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import pytest

from dual_planner import PROJECT_PATH, assert_state_equal, database_signature, plan, reset_database
from generate_model_graph import generate_model_graph
from randomgraph_mutations import generate_model_mutations


@pytest.mark.parametrize(
    "profile",
    (
        {
            "MODEL_BATCH_SIZE": "1",
            "SNAPSHOT_BATCH_SIZE": "1",
            "HYDRATED_MODEL_CACHE_SIZE": "1",
            "HYDRATED_SNAPSHOT_CACHE_SIZE": "1",
            "STREAMING_WORKERS": "1",
            "STREAMING_WORKER_MAX_TASKS": "1",
        },
        {
            "MODEL_BATCH_SIZE": "3",
            "SNAPSHOT_BATCH_SIZE": "2",
            "HYDRATED_MODEL_CACHE_SIZE": "2",
            "HYDRATED_SNAPSHOT_CACHE_SIZE": "2",
            "STREAMING_WORKERS": "2",
            "STREAMING_WORKER_MAX_TASKS": "1",
        },
    ),
    ids=("single_entry_batches", "uneven_two_worker_batches"),
)
def test_tiny_batch_cache_and_worker_recycling_profiles_preserve_parity(
    profile: dict[str, str], accuracy_stack, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in profile.items():
        monkeypatch.setenv(name, value)

    graph = generate_model_graph(
        5,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=137,
    )
    model_names = (*graph.input_models, *(model for layer in graph.layers for model in layer))
    reset_database("reference")
    reset_database("candidate")
    plan("reference", permutation_seed=13701)
    plan("candidate", permutation_seed=13701)
    assert database_signature("candidate", model_names) == database_signature(
        "reference", model_names
    )
    assert_state_equal()

    mutations = generate_model_mutations(graph, seed=1371)
    next(mutation for mutation in mutations if mutation.kind == "data").apply()
    next(mutation for mutation in mutations if mutation.kind == "filter").apply()
    plan("reference", permutation_seed=13702)
    plan("candidate", permutation_seed=13702)
    assert database_signature("candidate", model_names) == database_signature(
        "reference", model_names
    )
    assert_state_equal()
    assert "No changes to plan" in plan("reference", permutation_seed=13703)
    assert_state_equal()
    assert "No changes to plan" in plan("candidate", permutation_seed=13703)
    assert_state_equal()
