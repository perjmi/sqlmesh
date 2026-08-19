# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path

from generate_model_graph import GeneratedGraph

PROJECT_PATH = Path(__file__).parent
AUDIT_PATH = PROJECT_PATH / "audits" / "randomgraph_invariants.sql"


def randomgraph_audit_sql() -> str:
    return """AUDIT (
  name randomgraph_invariants,
  blocking true
);

SELECT *
FROM @this_model
WHERE
  entity_id IS NULL
  OR bucket_id IS NULL
  OR total_value IS NULL
  OR row_count IS NULL
  OR row_count <= 0;
"""


def write_randomgraph_audit() -> None:
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_PATH.write_text(randomgraph_audit_sql(), encoding="utf-8")


@dataclass(frozen=True)
class ModelMutation:
    kind: str
    model_name: str
    path: Path
    replacements: tuple[tuple[str, str], ...]

    def apply(self) -> None:
        sql = self.path.read_text(encoding="utf-8")
        for before, after in self.replacements:
            occurrences = sql.count(before)
            if occurrences != 1:
                raise ValueError(
                    f"Expected one occurrence while applying {self.kind} to {self.model_name}, "
                    f"found {occurrences}: {before!r}"
                )
            sql = sql.replace(before, after, 1)
        self.path.write_text(sql, encoding="utf-8")


def generate_model_mutations(graph: GeneratedGraph, *, seed: int) -> tuple[ModelMutation, ...]:
    """Builds a deterministic, randomly targeted and ordered semantic mutation sequence."""
    rng = random.Random(seed)
    paths = {f"generated.{path.stem}": path for path in graph.model_paths}
    metadata_model = rng.choice(graph.input_models)
    metadata_name = f"mutation_owner_{seed}"
    mutations = [
        ModelMutation(
            kind="metadata",
            model_name=metadata_model,
            path=paths[metadata_model],
            replacements=(
                (
                    f"  name {metadata_model},\n",
                    f"  name {metadata_model},\n  owner {metadata_name},\n",
                ),
            ),
        )
    ]

    data_model = rng.choice(graph.input_models)
    data_index = graph.input_models.index(data_model)
    multiplier = data_index + 1
    mutations.append(
        ModelMutation(
            kind="data",
            model_name=data_model,
            path=paths[data_model],
            replacements=(
                (
                    f"(series_id * {multiplier})::DECIMAL(38, 6)",
                    f"(series_id * {multiplier + seed + 1})::DECIMAL(38, 6)",
                ),
            ),
        )
    )

    filter_candidates = [name for name in graph.input_models if name != data_model]
    filter_model = rng.choice(filter_candidates or list(graph.input_models))
    rows_per_input = graph.input_row_counts[filter_model]
    mutations.append(
        ModelMutation(
            kind="filter",
            model_name=filter_model,
            path=paths[filter_model],
            replacements=(
                (
                    f"FROM GENERATE_SERIES(1, {rows_per_input}) AS source(series_id);",
                    f"FROM GENERATE_SERIES(1, {rows_per_input}) AS source(series_id)\n"
                    f"WHERE MOD(series_id, {seed % 5 + 2}) <> 0;",
                ),
            ),
        )
    )

    dependency_candidates = [name for name in graph.input_models if name != filter_model]
    dependency_model = rng.choice(dependency_candidates)
    replacement_dependency = rng.choice(
        [name for name in graph.input_models if name != dependency_model]
    )
    dependency_rows = graph.input_row_counts[dependency_model]
    mutations.append(
        ModelMutation(
            kind="dependency",
            model_name=dependency_model,
            path=paths[dependency_model],
            replacements=(
                (
                    f"FROM GENERATE_SERIES(1, {dependency_rows}) AS source(series_id);",
                    "FROM (\n"
                    "  SELECT entity_id AS series_id\n"
                    f"  FROM {replacement_dependency}\n"
                    ") AS source;",
                ),
            ),
        )
    )

    schema_model = rng.choice(graph.input_models)
    marker = seed % 97 + 1
    mutations.append(
        ModelMutation(
            kind="schema",
            model_name=schema_model,
            path=paths[schema_model],
            replacements=(
                (
                    "    total_value DECIMAL(38, 6),\n    row_count BIGINT\n",
                    "    total_value DECIMAL(38, 6),\n    row_count BIGINT,\n"
                    "    mutation_marker INTEGER\n",
                ),
                (
                    "  1::BIGINT AS row_count\nFROM ",
                    f"  1::BIGINT AS row_count,\n  {marker}::INTEGER AS mutation_marker\nFROM ",
                ),
            ),
        )
    )

    kind_candidates = [
        name for name in graph.input_models if name not in {data_model, filter_model}
    ]
    kind_model = rng.choice(kind_candidates or list(graph.input_models))
    mutations.append(
        ModelMutation(
            kind="model_kind",
            model_name=kind_model,
            path=paths[kind_model],
            replacements=(("  kind FULL,", "  kind VIEW,"),),
        )
    )

    audit_model = rng.choice([name for name in graph.input_models if name != kind_model])
    mutations.append(
        ModelMutation(
            kind="audit",
            model_name=audit_model,
            path=paths[audit_model],
            replacements=(
                (
                    f"  name {audit_model},\n",
                    f"  name {audit_model},\n  audits (randomgraph_invariants),\n",
                ),
            ),
        )
    )

    rng.shuffle(mutations)
    return tuple(mutations)
