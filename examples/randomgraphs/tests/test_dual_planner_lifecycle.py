# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dual_planner import (
    PROJECT_PATH,
    assert_state_equal,
    database_signature,
    model_names_for_environment,
    plan,
    plan_result,
    query,
    reset_database,
    state_signature,
)
from generate_model_graph import GeneratedGraph, generate_model_graph
from randomgraph_mutations import generate_model_mutations
from randomgraph_scenarios import (
    write_fan_in_view,
    write_full_model,
    write_passthrough_view,
)


def _model_names(graph: GeneratedGraph, *additional: str) -> tuple[str, ...]:
    return (
        *graph.input_models,
        *(model for layer in graph.layers for model in layer),
        *additional,
    )


def _assert_database_equal(model_names: tuple[str, ...], *, schema: str = "generated") -> None:
    assert database_signature("candidate", model_names, schema=schema) == database_signature(
        "reference", model_names, schema=schema
    )


def _descendants(graph: GeneratedGraph, model_name: str) -> set[str]:
    descendants: set[str] = set()
    frontier = {model_name}
    while frontier:
        next_frontier = {
            child
            for child, parents in graph.dependencies.items()
            if child not in descendants and any(parent in frontier for parent in parents)
        }
        descendants.update(next_frontier)
        frontier = next_frontier
    return descendants


def test_deep_fan_in_disconnected_and_model_lifecycle_preserve_parity(
    accuracy_stack,
) -> None:
    graph = generate_model_graph(
        4,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=109,
    )
    disconnected_root = "generated.disconnected_root"
    disconnected_view = "generated.disconnected_view"
    write_full_model(disconnected_root)
    write_passthrough_view(disconnected_view, disconnected_root)

    deep_models: list[str] = []
    upstream = graph.input_models[0]
    for index in range(10):
        model_name = f"generated.deep_{index:02d}"
        write_passthrough_view(model_name, upstream)
        deep_models.append(model_name)
        upstream = model_name

    fan_in_model = "generated.maximum_fan_in"
    write_fan_in_view(fan_in_model, graph.input_models)
    diamond_left = "generated.diamond_left"
    diamond_right = "generated.diamond_right"
    diamond_merge = "generated.diamond_merge"
    write_passthrough_view(diamond_left, graph.input_models[1])
    write_passthrough_view(diamond_right, graph.input_models[1])
    write_fan_in_view(diamond_merge, (diamond_left, diamond_right))

    topology_models = (
        disconnected_root,
        disconnected_view,
        *deep_models,
        fan_in_model,
        diamond_left,
        diamond_right,
        diamond_merge,
    )
    model_names = _model_names(graph, *topology_models)
    reset_database("reference")
    reset_database("candidate")
    plan("reference", permutation_seed=10901)
    plan("candidate", permutation_seed=10901)
    _assert_database_equal(model_names)
    assert_state_equal()

    added_model = "generated.lifecycle_added"
    added_path = write_passthrough_view(added_model, deep_models[-1])
    plan("reference", permutation_seed=10902)
    plan("candidate", permutation_seed=10902)
    model_names = (*model_names, added_model)
    _assert_database_equal(model_names)
    assert_state_equal()

    renamed_model = "generated.lifecycle_renamed"
    added_path.unlink()
    renamed_path = write_passthrough_view(renamed_model, deep_models[-1])
    plan("reference", permutation_seed=10903)
    plan("candidate", permutation_seed=10903)
    model_names = (*model_names[:-1], renamed_model)
    _assert_database_equal(model_names)
    assert query(
        "candidate",
        """
        SELECT EXISTS (
          SELECT 1 FROM information_schema.views
          WHERE table_schema = 'generated' AND table_name = 'lifecycle_added'
        )
        """,
    ) == ("f",)
    assert_state_equal()

    renamed_path.unlink()
    plan("reference", permutation_seed=10904)
    plan("candidate", permutation_seed=10904)
    model_names = model_names[:-1]
    _assert_database_equal(model_names)
    assert query(
        "candidate",
        """
        SELECT EXISTS (
          SELECT 1 FROM information_schema.views
          WHERE table_schema = 'generated' AND table_name = 'lifecycle_renamed'
        )
        """,
    ) == ("f",)
    assert_state_equal()


def test_randomized_selector_expansion_with_simultaneous_changes_preserves_parity(
    accuracy_stack,
) -> None:
    graph = generate_model_graph(
        3,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=113,
    )
    reset_database("reference")
    reset_database("candidate")
    plan("reference", permutation_seed=11301)
    plan("candidate", permutation_seed=11301)

    mutations = generate_model_mutations(graph, seed=1131)
    data_mutation = next(mutation for mutation in mutations if mutation.kind == "data")
    filter_mutation = next(mutation for mutation in mutations if mutation.kind == "filter")
    data_mutation.apply()
    filter_mutation.apply()

    environment = "selector_expansion"
    selected_models = (f"+{data_mutation.model_name}", f"{filter_mutation.model_name}+")
    options = (
        ("--create-from", "prod"),
        ("--execution-time", "2026-09-02T00:00:00+00:00"),
    )
    plan(
        "reference",
        selected_models,
        environment=environment,
        option_groups=options,
        permutation_seed=11302,
    )
    plan(
        "candidate",
        selected_models,
        environment=environment,
        option_groups=options,
        permutation_seed=11302,
    )
    expected_models = {
        data_mutation.model_name,
        filter_mutation.model_name,
        *_descendants(graph, filter_mutation.model_name),
    }
    expected_tables = {model_name.rsplit(".", 1)[-1] for model_name in expected_models}
    reference_tables = set(
        query(
            "reference",
            f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'generated__{environment}'
            ORDER BY table_name
            """,
        )
    )
    candidate_tables = set(
        query(
            "candidate",
            f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'generated__{environment}'
            ORDER BY table_name
            """,
        )
    )
    assert candidate_tables == reference_tables == expected_tables
    environment_models = model_names_for_environment(tuple(sorted(expected_models)), environment)
    _assert_database_equal(environment_models, schema=f"generated__{environment}")
    assert_state_equal()


def test_cycle_and_unresolved_dependency_fail_without_state_changes(
    accuracy_stack,
) -> None:
    graph = generate_model_graph(
        2,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=127,
    )
    reset_database("reference")
    reset_database("candidate")
    plan("reference", permutation_seed=12701)
    plan("candidate", permutation_seed=12701)
    reference_before = state_signature("reference")
    candidate_before = state_signature("candidate")

    unresolved_path = write_passthrough_view(
        "generated.unresolved_model", "generated.missing_dependency"
    )
    reference_unresolved = plan_result("reference", permutation_seed=12702)
    candidate_unresolved = plan_result("candidate", permutation_seed=12702)
    assert reference_unresolved.returncode != 0
    assert candidate_unresolved.returncode != 0
    assert "missing_dependency" in (f"{reference_unresolved.stdout}\n{reference_unresolved.stderr}")
    assert "missing_dependency" in (f"{candidate_unresolved.stdout}\n{candidate_unresolved.stderr}")
    reference_after_unresolved = state_signature("reference")
    candidate_after_unresolved = state_signature("candidate")
    for section in ("intervals", "environments"):
        assert reference_after_unresolved[section] == reference_before[section]
        assert candidate_after_unresolved[section] == candidate_before[section]
    assert_state_equal()

    unresolved_path.unlink()
    cycle_a = write_passthrough_view("generated.cycle_a", "generated.cycle_b")
    cycle_b = write_passthrough_view("generated.cycle_b", "generated.cycle_a")
    reference_cycle = plan_result("reference", permutation_seed=12703)
    candidate_cycle = plan_result("candidate", permutation_seed=12703)
    assert reference_cycle.returncode != 0
    assert candidate_cycle.returncode != 0
    assert "cycle" in f"{reference_cycle.stdout}\n{reference_cycle.stderr}".lower()
    assert "cycle" in f"{candidate_cycle.stdout}\n{candidate_cycle.stderr}".lower()
    reference_after_cycle = state_signature("reference")
    candidate_after_cycle = state_signature("candidate")
    for section in ("intervals", "environments"):
        assert reference_after_cycle[section] == reference_after_unresolved[section]
        assert candidate_after_cycle[section] == candidate_after_unresolved[section]
    assert_state_equal()
    cycle_a.unlink()
    cycle_b.unlink()
