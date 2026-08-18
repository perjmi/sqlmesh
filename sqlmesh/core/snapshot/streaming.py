from __future__ import annotations

import typing as t

from sqlmesh.core.model.graph import ProjectGraphIndex
from sqlmesh.core.model.registry import ModelRegistry
from sqlmesh.core.snapshot.definition import SnapshotFingerprint
from sqlmesh.utils.hashing import hash_data


class StreamingFingerprinter:
    """Computes and persists fingerprints with a bounded hydrated-model working set."""

    def __init__(
        self,
        index: ProjectGraphIndex,
        registry: ModelRegistry,
        batch_size: int,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")
        self._index = index
        self._registry = registry
        self._batch_size = batch_size

    def fingerprint(self) -> t.Iterator[t.Tuple[str, SnapshotFingerprint]]:
        self._index.clear_fingerprints()
        for names in self._index.iter_topological_batches(self._batch_size):
            completed: t.List[t.Tuple[str, SnapshotFingerprint]] = []
            try:
                for models in self._registry.hydrate_batches(names, self._batch_size):
                    for model in models:
                        metadata = self._index.metadata(model.fqn)
                        parents = [
                            self._index.fingerprint(parent)
                            for parent in metadata.dependencies
                            if self._index.contains(parent)
                        ]
                        fingerprint = SnapshotFingerprint(
                            data_hash=model.data_hash,
                            metadata_hash=model.metadata_hash,
                            parent_data_hash=hash_data(
                                sorted(parent.to_version() for parent in parents)
                            ),
                            parent_metadata_hash=hash_data(
                                sorted(
                                    value
                                    for parent in parents
                                    for value in (
                                        parent.metadata_hash,
                                        parent.parent_metadata_hash,
                                    )
                                )
                            ),
                        )
                        completed.append((model.fqn, fingerprint))
                self._index.put_fingerprints(completed)
                yield from completed
            finally:
                self._registry.evict(names)
