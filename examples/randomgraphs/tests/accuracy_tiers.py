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


def selected_accuracy_scenarios() -> tuple[AccuracyScenario, ...]:
    tier = os.environ.get("RANDOMGRAPHS_ACCURACY_TIER", "smoke")
    try:
        return ACCURACY_TIERS[tier]
    except KeyError as ex:
        choices = ", ".join(sorted(ACCURACY_TIERS))
        raise ValueError(f"Unknown accuracy tier {tier!r}; choose one of: {choices}") from ex
