# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dual_planner import (
    PROJECT_PATH,
    assert_state_equal,
    database_signature,
    plan,
    plan_result,
    randomized_plan_options,
    reset_database,
)
from generate_model_graph import generate_model_graph
from randomgraph_mutations import generate_model_mutations


def test_randomized_plan_options_preserve_groups_and_are_reproducible() -> None:
    groups = (
        ("--start", "2026-01-01"),
        ("--end", "2026-01-02"),
        ("--execution-time", "2026-09-03T00:00:00+00:00"),
        ("--include-unmodified",),
        ("--select-model", "generated.input_00000"),
    )

    first = randomized_plan_options(groups, seed=17)
    second = randomized_plan_options(groups, seed=17)

    assert first == second
    assert first != tuple(value for group in groups for value in group)
    for group in groups:
        offset = first.index(group[0])
        assert first[offset : offset + len(group)] == group


def test_randomized_baseline_and_environment_options_preserve_parity(accuracy_stack) -> None:
    graph = generate_model_graph(
        3,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=73,
    )
    model_names = (*graph.input_models, *(model for layer in graph.layers for model in layer))
    baseline_options = (
        ("--execution-time", "2026-09-03T00:00:00+00:00"),
        ("--skip-tests",),
        ("--skip-linter",),
        ("--no-diff",),
    )

    reset_database("reference")
    reset_database("candidate")
    plan("reference", option_groups=baseline_options, permutation_seed=7301)
    plan("candidate", option_groups=baseline_options, permutation_seed=7301)
    assert database_signature("candidate", model_names) == database_signature(
        "reference", model_names
    )
    assert_state_equal()

    environment_options = (
        ("--create-from", "prod"),
        ("--include-unmodified",),
        ("--execution-time", "2026-09-04T00:00:00+00:00"),
        ("--diff-rendered",),
    )
    environment = "random_options_dev"
    plan(
        "reference",
        environment=environment,
        option_groups=environment_options,
        permutation_seed=7302,
    )
    plan(
        "candidate",
        environment=environment,
        option_groups=environment_options,
        permutation_seed=7302,
    )
    assert database_signature("candidate", model_names) == database_signature(
        "reference", model_names
    )
    assert "No changes to plan" in plan(
        "reference",
        environment=environment,
        option_groups=environment_options,
        permutation_seed=7303,
    )
    assert "No changes to plan" in plan(
        "candidate",
        environment=environment,
        option_groups=environment_options,
        permutation_seed=7303,
    )
    assert_state_equal()


def test_randomized_non_backfill_run_and_categorization_options_preserve_parity(
    accuracy_stack,
) -> None:
    graph = generate_model_graph(
        3,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=83,
    )
    model_names = (*graph.input_models, *(model for layer in graph.layers for model in layer))
    reset_database("reference")
    reset_database("candidate")
    baseline_options = (("--execution-time", "2026-09-09T00:00:00+00:00"),)
    plan("reference", option_groups=baseline_options, permutation_seed=8301)
    plan("candidate", option_groups=baseline_options, permutation_seed=8301)

    run_options = (
        ("--run",),
        ("--ignore-cron",),
        ("--min-intervals", "2"),
        ("--execution-time", "2026-09-10T00:00:00+00:00"),
    )
    plan("reference", option_groups=run_options, permutation_seed=8302)
    plan("candidate", option_groups=run_options, permutation_seed=8302)
    assert database_signature("candidate", model_names) == database_signature(
        "reference", model_names
    )
    assert_state_equal()

    mutation = next(
        mutation
        for mutation in generate_model_mutations(graph, seed=839)
        if mutation.kind == "filter"
    )
    mutation.apply()
    dry_run_options = (
        ("--create-from", "prod"),
        ("--include-unmodified",),
        ("--skip-backfill",),
        ("--no-gaps",),
        ("--execution-time", "2026-09-11T00:00:00+00:00"),
        ("--no-diff",),
    )
    plan(
        "reference",
        environment="random_options_dry",
        option_groups=dry_run_options,
        permutation_seed=8303,
    )
    plan(
        "candidate",
        environment="random_options_dry",
        option_groups=dry_run_options,
        permutation_seed=8303,
    )
    assert_state_equal()

    empty_backfill_options = (
        ("--create-from", "prod"),
        ("--include-unmodified",),
        ("--empty-backfill",),
        ("--execution-time", "2026-09-12T00:00:00+00:00"),
        ("--diff-rendered",),
    )
    plan(
        "reference",
        environment="random_options_empty",
        option_groups=empty_backfill_options,
        permutation_seed=8304,
    )
    plan(
        "candidate",
        environment="random_options_empty",
        option_groups=empty_backfill_options,
        permutation_seed=8304,
    )
    assert database_signature("candidate", model_names) == database_signature(
        "reference", model_names
    )
    assert_state_equal()

    uncategorized_options = (
        ("--no-auto-categorization",),
        ("--execution-time", "2026-09-13T00:00:00+00:00"),
        ("--diff-rendered",),
    )
    reference_result = plan_result(
        "reference", option_groups=uncategorized_options, permutation_seed=8305
    )
    candidate_result = plan_result(
        "candidate", option_groups=uncategorized_options, permutation_seed=8305
    )
    reference_result.check_returncode()
    candidate_result.check_returncode()
    assert database_signature("candidate", model_names) == database_signature(
        "reference", model_names
    )
    assert_state_equal()


def test_randomized_restate_backfill_and_forward_only_options_preserve_parity(
    accuracy_stack,
) -> None:
    graph = generate_model_graph(
        3,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=79,
    )
    model_names = (*graph.input_models, *(model for layer in graph.layers for model in layer))
    reset_database("reference")
    reset_database("candidate")
    baseline_options = (("--execution-time", "2026-09-05T00:00:00+00:00"),)
    plan("reference", option_groups=baseline_options, permutation_seed=7901)
    plan("candidate", option_groups=baseline_options, permutation_seed=7901)

    restated_model = graph.input_models[0]
    backfilled_model = graph.layers[-1][0]
    restatement_options = (
        ("--restate-model", restated_model),
        ("--backfill-model", backfilled_model),
        ("--start", "2026-08-29"),
        ("--end", "2026-09-05"),
        ("--execution-time", "2026-09-05T00:00:00+00:00"),
        ("--no-diff",),
    )
    plan("reference", option_groups=restatement_options, permutation_seed=7902)
    plan("candidate", option_groups=restatement_options, permutation_seed=7902)
    assert database_signature("candidate", model_names) == database_signature(
        "reference", model_names
    )
    assert_state_equal()

    mutation = next(
        mutation
        for mutation in generate_model_mutations(graph, seed=791)
        if mutation.kind == "data"
    )
    mutation.apply()
    preview_options = (
        ("--create-from", "prod"),
        ("--include-unmodified",),
        ("--forward-only",),
        ("--enable-preview",),
        ("--execution-time", "2026-09-06T00:00:00+00:00"),
        ("--allow-destructive-model", mutation.model_name),
        ("--allow-additive-model", mutation.model_name),
        ("--diff-rendered",),
        ("--explain",),
    )
    reference_preview = plan(
        "reference",
        environment="random_options_preview",
        option_groups=preview_options,
        permutation_seed=7903,
    )
    candidate_preview = plan(
        "candidate",
        environment="random_options_preview",
        option_groups=preview_options,
        permutation_seed=7903,
    )
    assert "No changes to plan" not in reference_preview
    assert "No changes to plan" not in candidate_preview
    assert_state_equal()

    forward_only_options = (
        ("--forward-only",),
        ("--effective-from", "2026-08-29"),
        ("--execution-time", "2026-09-07T00:00:00+00:00"),
        ("--allow-destructive-model", mutation.model_name),
        ("--allow-additive-model", mutation.model_name),
        ("--diff-rendered",),
        ("--explain",),
    )
    reference_output = plan("reference", option_groups=forward_only_options, permutation_seed=7904)
    candidate_output = plan("candidate", option_groups=forward_only_options, permutation_seed=7904)
    assert "No changes to plan" not in reference_output
    assert "No changes to plan" not in candidate_output
    assert database_signature("candidate", model_names) == database_signature(
        "reference", model_names
    )
    assert_state_equal()
