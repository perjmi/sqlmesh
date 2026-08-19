# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AccuracyScenario:
    width: int
    seed: int
    candidate_mode: str

    @property
    def test_id(self) -> str:
        return f"width_{self.width}-seed_{self.seed}-{self.candidate_mode}"


@dataclass(frozen=True)
class MutationScenario:
    width: int
    graph_seed: int
    mutation_seed: int

    @property
    def test_id(self) -> str:
        return (
            f"width_{self.width}-graph_{self.graph_seed}-mutations_{self.mutation_seed}"
        )


ACCURACY_TIERS: dict[str, tuple[AccuracyScenario, ...]] = {
    "smoke": (
        AccuracyScenario(width=3, seed=7, candidate_mode="full"),
        AccuracyScenario(width=3, seed=7, candidate_mode="chunked"),
    ),
    "pr": (
        AccuracyScenario(width=3, seed=0, candidate_mode="chunked"),
        AccuracyScenario(width=10, seed=0, candidate_mode="chunked"),
        AccuracyScenario(width=10, seed=1, candidate_mode="chunked"),
    ),
    "nightly": (
        AccuracyScenario(width=10, seed=0, candidate_mode="chunked"),
        AccuracyScenario(width=30, seed=1, candidate_mode="chunked"),
        AccuracyScenario(width=100, seed=2, candidate_mode="chunked"),
    ),
    "stress": (
        AccuracyScenario(width=100, seed=42, candidate_mode="chunked"),
        AccuracyScenario(width=200, seed=42, candidate_mode="chunked"),
        AccuracyScenario(width=300, seed=42, candidate_mode="chunked"),
    ),
}

MUTATION_TIERS: dict[str, tuple[MutationScenario, ...]] = {
    "smoke": (MutationScenario(width=3, graph_seed=7, mutation_seed=11),),
    "sweep10": tuple(
        MutationScenario(width=3, graph_seed=100 + index, mutation_seed=1001 + index)
        for index in range(10)
    ),
    "pr": (
        MutationScenario(width=3, graph_seed=0, mutation_seed=11),
        MutationScenario(width=10, graph_seed=1, mutation_seed=29),
    ),
    "nightly": (
        MutationScenario(width=10, graph_seed=0, mutation_seed=11),
        MutationScenario(width=30, graph_seed=1, mutation_seed=29),
    ),
    "stress": (
        MutationScenario(width=100, graph_seed=42, mutation_seed=11),
        MutationScenario(width=200, graph_seed=43, mutation_seed=29),
    ),
}


def selected_accuracy_scenarios() -> tuple[AccuracyScenario, ...]:
    tier = os.environ.get("RANDOMGRAPHS_ACCURACY_TIER", "smoke")
    try:
        return ACCURACY_TIERS[tier]
    except KeyError as ex:
        choices = ", ".join(sorted(ACCURACY_TIERS))
        raise ValueError(f"Unknown accuracy tier {tier!r}; choose one of: {choices}") from ex


def selected_mutation_scenarios() -> tuple[MutationScenario, ...]:
    tier = os.environ.get("RANDOMGRAPHS_MUTATION_TIER", "smoke")
    try:
        return MUTATION_TIERS[tier]
    except KeyError as ex:
        choices = ", ".join(sorted(MUTATION_TIERS))
        raise ValueError(f"Unknown mutation tier {tier!r}; choose one of: {choices}") from ex
