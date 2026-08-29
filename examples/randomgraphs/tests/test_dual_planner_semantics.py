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
from randomgraph_scenarios import INCREMENTAL_MODEL_NAME, write_incremental_model


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


def test_incremental_run_restatement_and_interval_state_preserve_parity(
    accuracy_stack,
) -> None:
    graph = generate_model_graph(
        2,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=101,
    )
    write_incremental_model()
    model_names = _model_names(graph, INCREMENTAL_MODEL_NAME)
    reset_database("reference")
    reset_database("candidate")

    baseline_options = (("--execution-time", "2026-08-28T00:00:00+00:00"),)
    plan("reference", option_groups=baseline_options, permutation_seed=10101)
    plan("candidate", option_groups=baseline_options, permutation_seed=10101)
    _assert_database_equal(model_names)
    assert_state_equal()
    reference_dates = query(
        "reference",
        f"SELECT COUNT(*), MIN(event_date), MAX(event_date) FROM {INCREMENTAL_MODEL_NAME}",
    )
    candidate_dates = query(
        "candidate",
        f"SELECT COUNT(*), MIN(event_date), MAX(event_date) FROM {INCREMENTAL_MODEL_NAME}",
    )
    assert candidate_dates == reference_dates
    assert int(candidate_dates[0].split("|", 1)[0]) > 0

    run_options = (
        ("--run",),
        ("--ignore-cron",),
        ("--min-intervals", "2"),
        ("--execution-time", "2026-08-29T12:00:00+00:00"),
    )
    plan("reference", option_groups=run_options, permutation_seed=10102)
    plan("candidate", option_groups=run_options, permutation_seed=10102)
    _assert_database_equal(model_names)
    assert_state_equal()

    restatement_options = (
        ("--restate-model", INCREMENTAL_MODEL_NAME),
        ("--backfill-model", INCREMENTAL_MODEL_NAME),
        ("--start", "2026-08-26"),
        ("--end", "2026-08-27"),
        ("--execution-time", "2026-08-29T12:00:00+00:00"),
    )
    plan("reference", option_groups=restatement_options, permutation_seed=10103)
    plan("candidate", option_groups=restatement_options, permutation_seed=10103)
    _assert_database_equal(model_names)
    assert_state_equal()


def test_skip_empty_and_normal_backfills_have_distinct_matching_semantics(
    accuracy_stack,
) -> None:
    graph = generate_model_graph(
        2,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=103,
    )
    graph_model_names = _model_names(graph)
    reset_database("reference")
    reset_database("candidate")
    baseline_options = (("--execution-time", "2026-08-28T00:00:00+00:00"),)
    plan("reference", option_groups=baseline_options, permutation_seed=10301)
    plan("candidate", option_groups=baseline_options, permutation_seed=10301)
    _assert_database_equal(graph_model_names)
    assert_state_equal()

    write_incremental_model()
    all_model_names = (*graph_model_names, INCREMENTAL_MODEL_NAME)
    skip_environment = "semantics_skip"
    skip_options = (
        ("--create-from", "prod"),
        ("--include-unmodified",),
        ("--skip-backfill",),
        ("--no-gaps",),
        ("--execution-time", "2026-08-28T00:00:00+00:00"),
    )
    plan(
        "reference",
        environment=skip_environment,
        option_groups=skip_options,
        permutation_seed=10302,
    )
    plan(
        "candidate",
        environment=skip_environment,
        option_groups=skip_options,
        permutation_seed=10302,
    )
    assert_state_equal()
    skip_model = model_names_for_environment((INCREMENTAL_MODEL_NAME,), skip_environment)[0]
    assert query("candidate", f"SELECT COUNT(*) FROM {skip_model}") == ("0",)
    assert query("reference", f"SELECT COUNT(*) FROM {skip_model}") == ("0",)
    active_interval_count_sql = """
        SELECT COUNT(*)
        FROM sqlmesh._intervals
        WHERE name LIKE '%incremental_events%' AND NOT is_removed
    """
    assert query("reference", active_interval_count_sql) == ("0",)
    assert query("candidate", active_interval_count_sql) == ("0",)

    empty_environment = "semantics_empty"
    empty_options = (
        ("--create-from", "prod"),
        ("--include-unmodified",),
        ("--empty-backfill",),
        ("--execution-time", "2026-08-28T00:00:00+00:00"),
    )
    plan(
        "reference",
        environment=empty_environment,
        option_groups=empty_options,
        permutation_seed=10303,
    )
    plan(
        "candidate",
        environment=empty_environment,
        option_groups=empty_options,
        permutation_seed=10303,
    )
    assert_state_equal()
    empty_model = model_names_for_environment((INCREMENTAL_MODEL_NAME,), empty_environment)[0]
    assert query("candidate", f"SELECT COUNT(*) FROM {empty_model}") == ("0",)
    assert query("reference", f"SELECT COUNT(*) FROM {empty_model}") == ("0",)
    assert int(query("reference", active_interval_count_sql)[0]) > 0
    assert int(query("candidate", active_interval_count_sql)[0]) > 0
    pending_options = (
        ("--create-from", "prod"),
        ("--include-unmodified",),
        ("--execution-time", "2026-08-28T00:00:00+00:00"),
        ("--explain",),
    )
    assert "No changes to plan" in plan(
        "reference",
        environment=empty_environment,
        option_groups=pending_options,
        permutation_seed=10304,
    )
    assert "No changes to plan" in plan(
        "candidate",
        environment=empty_environment,
        option_groups=pending_options,
        permutation_seed=10304,
    )

    normal_model_name = "generated.incremental_normal"
    write_incremental_model(name=normal_model_name)
    normal_model_names = (*all_model_names, normal_model_name)
    normal_environment = "semantics_normal"
    normal_options = (
        ("--create-from", "prod"),
        ("--include-unmodified",),
        ("--execution-time", "2026-08-28T00:00:00+00:00"),
    )
    plan(
        "reference",
        environment=normal_environment,
        option_groups=normal_options,
        permutation_seed=10305,
    )
    plan(
        "candidate",
        environment=normal_environment,
        option_groups=normal_options,
        permutation_seed=10305,
    )
    environment_model_names = model_names_for_environment(normal_model_names, normal_environment)
    _assert_database_equal(environment_model_names, schema=f"generated__{normal_environment}")
    normal_model = model_names_for_environment((normal_model_name,), normal_environment)[0]
    assert int(query("candidate", f"SELECT COUNT(*) FROM {normal_model}")[0]) > 0
    assert_state_equal()


def test_forward_only_allowlists_and_applied_preview_preserve_parity(
    accuracy_stack,
) -> None:
    graph = generate_model_graph(
        2,
        output_dir=PROJECT_PATH / "models" / "generated",
        seed=107,
    )
    write_incremental_model()
    model_names = _model_names(graph, INCREMENTAL_MODEL_NAME)
    reset_database("reference")
    reset_database("candidate")
    baseline_options = (("--execution-time", "2026-09-01T00:00:00+00:00"),)
    plan("reference", option_groups=baseline_options, permutation_seed=10701)
    plan("candidate", option_groups=baseline_options, permutation_seed=10701)
    assert_state_equal()

    write_incremental_model(include_extra=True)
    additive_options = (
        ("--forward-only",),
        ("--execution-time", "2026-09-03T00:00:00+00:00"),
        ("--explain",),
    )
    reference_before = state_signature("reference")
    candidate_before = state_signature("candidate")
    reference_additive = plan_result(
        "reference", option_groups=additive_options, permutation_seed=10702
    )
    candidate_additive = plan_result(
        "candidate", option_groups=additive_options, permutation_seed=10702
    )
    assert reference_additive.returncode != 0
    assert candidate_additive.returncode != 0
    assert "additive" in f"{reference_additive.stdout}\n{reference_additive.stderr}".lower()
    assert "additive" in f"{candidate_additive.stdout}\n{candidate_additive.stderr}".lower()
    assert state_signature("reference") == reference_before
    assert state_signature("candidate") == candidate_before

    preview_environment = "semantics_preview"
    preview_options = (
        ("--create-from", "prod"),
        ("--include-unmodified",),
        ("--forward-only",),
        ("--enable-preview",),
        ("--allow-additive-model", INCREMENTAL_MODEL_NAME),
        ("--execution-time", "2026-09-03T00:00:00+00:00"),
    )
    plan(
        "reference",
        environment=preview_environment,
        option_groups=preview_options,
        permutation_seed=10703,
    )
    plan(
        "candidate",
        environment=preview_environment,
        option_groups=preview_options,
        permutation_seed=10703,
    )
    preview_model_names = model_names_for_environment(model_names, preview_environment)
    _assert_database_equal(preview_model_names, schema=f"generated__{preview_environment}")
    assert (
        int(
            query(
                "candidate",
                f"SELECT COUNT(*) FROM generated__{preview_environment}.incremental_events",
            )[0]
        )
        > 0
    )
    assert_state_equal()

    write_incremental_model(include_extra=True, include_legacy=False)
    destructive_options = (
        ("--forward-only",),
        ("--allow-additive-model", INCREMENTAL_MODEL_NAME),
        ("--effective-from", "2026-08-29"),
        ("--execution-time", "2026-09-04T00:00:00+00:00"),
        ("--explain",),
    )
    reference_destructive = plan_result(
        "reference", option_groups=destructive_options, permutation_seed=10704
    )
    candidate_destructive = plan_result(
        "candidate", option_groups=destructive_options, permutation_seed=10704
    )
    assert reference_destructive.returncode != 0
    assert candidate_destructive.returncode != 0
    assert (
        "destructive" in (f"{reference_destructive.stdout}\n{reference_destructive.stderr}").lower()
    )
    assert (
        "destructive" in (f"{candidate_destructive.stdout}\n{candidate_destructive.stderr}").lower()
    )

    production_options = (
        ("--forward-only",),
        ("--allow-additive-model", INCREMENTAL_MODEL_NAME),
        ("--allow-destructive-model", INCREMENTAL_MODEL_NAME),
        ("--effective-from", "2026-08-29"),
        ("--execution-time", "2026-09-04T00:00:00+00:00"),
    )
    plan("reference", option_groups=production_options, permutation_seed=10705)
    plan("candidate", option_groups=production_options, permutation_seed=10705)
    _assert_database_equal(model_names)
    columns = query(
        "candidate",
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'generated' AND table_name = 'incremental_events'
        ORDER BY ordinal_position
        """,
    )
    assert "extra_metric" in columns
    assert "legacy_metric" not in columns
    assert_state_equal()
