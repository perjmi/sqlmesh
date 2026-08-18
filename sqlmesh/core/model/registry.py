from __future__ import annotations

import typing as t
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

from sqlmesh.core.model.definition import Model
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.cache import FileCache
from sqlmesh.utils.dag import DAG


@dataclass(frozen=True)
class ModelMetadata:
    """Compact model fields used for discovery, selection, and graph traversal."""

    fqn: str
    name: str
    source_path: t.Optional[Path]
    project: str
    dialect: str
    gateway: t.Optional[str]
    enabled: bool
    kind_name: str
    dependencies: t.Tuple[str, ...]
    tags: t.Tuple[str, ...]
    dbt_fqn: t.Optional[str]
    payload_key: str = ""
    payload_digest: str = ""

    @classmethod
    def from_model(cls, model: Model, *, include_payload_digest: bool = True) -> ModelMetadata:
        return cls(
            fqn=model.fqn,
            name=model.name,
            source_path=model._path,
            project=model.project,
            dialect=model.dialect,
            gateway=model.gateway,
            enabled=model.enabled,
            kind_name=str(model.kind.name),
            dependencies=tuple(sorted(model.depends_on)),
            tags=tuple(model.tags),
            dbt_fqn=model.dbt_fqn,
            payload_key=model.fqn,
            payload_digest=(
                f"{model.data_hash}_{model.metadata_hash}" if include_payload_digest else ""
            ),
        )


@dataclass(frozen=True)
class StoredModelPayload:
    """Serialized model plus hashes that model pickling intentionally clears."""

    model: Model
    data_hash: str
    metadata_hash: str


class ModelPayloadStore:
    """Versioned on-disk storage for independently hydrated model payloads."""

    def __init__(self, path: Path) -> None:
        self._cache: FileCache[t.Union[Model, StoredModelPayload]] = FileCache(
            path, prefix="model_payload"
        )

    def put(self, model: Model, metadata: ModelMetadata) -> None:
        self._cache.put(
            metadata.payload_key or metadata.fqn,
            metadata.payload_digest,
            value=StoredModelPayload(
                model=model,
                data_hash=model.data_hash,
                metadata_hash=model.metadata_hash,
            ),
        )

    def get(self, metadata: ModelMetadata) -> t.Optional[Model]:
        payload = self._cache.get(
            metadata.payload_key or metadata.fqn,
            metadata.payload_digest,
        )
        if payload is None or isinstance(payload, Model):
            return payload

        payload.model._data_hash = payload.data_hash
        payload.model._metadata_hash = payload.metadata_hash
        return payload.model


class ModelRegistry(t.Protocol):
    """Read interface shared by eager and indexed model stores."""

    def metadata(self, name: str) -> ModelMetadata: ...

    def contains(self, name: str) -> bool: ...

    def iter_names(self) -> t.Iterator[str]: ...

    def match_names(self, pattern: str) -> t.Set[str]: ...

    def match_tags(self, pattern: str) -> t.Set[str]: ...

    def match_kinds(self, kind_names: t.Collection[str]) -> t.Set[str]: ...

    def match_source_paths(self, paths: t.Collection[Path]) -> t.Set[str]: ...

    def iter_metadata(
        self, names: t.Optional[t.Iterable[str]] = None
    ) -> t.Iterator[ModelMetadata]: ...

    def upstream(self, names: t.Iterable[str]) -> t.Set[str]: ...

    def downstream(self, names: t.Iterable[str]) -> t.Set[str]: ...

    def hydrate_batches(
        self, names: t.Iterable[str], batch_size: int
    ) -> t.Iterator[t.Tuple[Model, ...]]: ...

    def evict(self, names: t.Iterable[str]) -> None: ...


class EagerModelRegistry(UniqueKeyDict[str, Model]):
    """A dict-compatible registry for the existing eager loader.

    This compatibility implementation deliberately keeps every model hydrated and makes eviction a
    no-op. Consumers can migrate to the registry interface before the indexed implementation changes
    the lifetime of model objects.
    """

    def __init__(self, models: t.Optional[t.Mapping[str, Model]] = None) -> None:
        super().__init__("models")
        if models:
            self.update(models)

    def metadata(self, name: str) -> ModelMetadata:
        return ModelMetadata.from_model(self[name])

    def contains(self, name: str) -> bool:
        return name in self

    def iter_names(self) -> t.Iterator[str]:
        yield from sorted(self)

    def match_names(self, pattern: str) -> t.Set[str]:
        import fnmatch

        return {model.fqn for model in self.values() if fnmatch.fnmatchcase(model.name, pattern)}

    def match_tags(self, pattern: str) -> t.Set[str]:
        import fnmatch

        return {
            model.fqn
            for model in self.values()
            if any(fnmatch.fnmatchcase(tag.lower(), pattern.lower()) for tag in model.tags)
        }

    def match_kinds(self, kind_names: t.Collection[str]) -> t.Set[str]:
        return {model.fqn for model in self.values() if str(model.kind.name) in kind_names}

    def match_source_paths(self, paths: t.Collection[Path]) -> t.Set[str]:
        return {model.fqn for model in self.values() if model._path in paths}

    def iter_metadata(self, names: t.Optional[t.Iterable[str]] = None) -> t.Iterator[ModelMetadata]:
        selected_names = sorted(self if names is None else set(names))
        for name in selected_names:
            yield self.metadata(name)

    def _dag(self) -> DAG[str]:
        return DAG({name: set(model.depends_on) for name, model in self.items()})

    def upstream(self, names: t.Iterable[str]) -> t.Set[str]:
        dag = self._dag()
        return {dependency for name in names for dependency in dag.upstream(name)}

    def downstream(self, names: t.Iterable[str]) -> t.Set[str]:
        dag = self._dag()
        return {dependency for name in names for dependency in dag.downstream(name)}

    def hydrate_batches(
        self, names: t.Iterable[str], batch_size: int
    ) -> t.Iterator[t.Tuple[Model, ...]]:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

        batch: t.List[Model] = []
        for name in sorted(set(names)):
            batch.append(self[name])
            if len(batch) == batch_size:
                yield tuple(batch)
                batch = []
        if batch:
            yield tuple(batch)

    def evict(self, names: t.Iterable[str]) -> None:
        # Eager mode intentionally retains hydrated models.
        for _ in names:
            pass


if t.TYPE_CHECKING:
    from sqlmesh.core.model.graph import ProjectGraphIndex


class IndexedModelRegistry:
    """Hydrates indexed models on demand into an entry-bounded LRU."""

    def __init__(
        self,
        index: ProjectGraphIndex,
        loader: t.Callable[[str], Model],
        max_entries: int,
    ) -> None:
        if max_entries < 0:
            raise ValueError("max_entries must be greater than or equal to 0")
        self._index = index
        self._loader = loader
        self._max_entries = max_entries
        self._cache: OrderedDict[str, Model] = OrderedDict()
        self.max_cache_size_seen = 0

    @property
    def cache_size(self) -> int:
        return len(self._cache)

    def metadata(self, name: str) -> ModelMetadata:
        return self._index.metadata(name)

    def contains(self, name: str) -> bool:
        return self._index.contains(name)

    def iter_names(self) -> t.Iterator[str]:
        yield from self._index.iter_names()

    def match_names(self, pattern: str) -> t.Set[str]:
        return self._index.match_names(pattern)

    def match_tags(self, pattern: str) -> t.Set[str]:
        return self._index.match_tags(pattern)

    def match_kinds(self, kind_names: t.Collection[str]) -> t.Set[str]:
        return self._index.match_kinds(kind_names)

    def match_source_paths(self, paths: t.Collection[Path]) -> t.Set[str]:
        return self._index.match_source_paths(paths)

    def iter_metadata(self, names: t.Optional[t.Iterable[str]] = None) -> t.Iterator[ModelMetadata]:
        yield from self._index.iter_metadata(names)

    def upstream(self, names: t.Iterable[str]) -> t.Set[str]:
        return self._index.upstream(names)

    def downstream(self, names: t.Iterable[str]) -> t.Set[str]:
        return self._index.downstream(names)

    def hydrate(self, name: str) -> Model:
        model = self._cache.get(name)
        if model is not None:
            self._cache.move_to_end(name)
            return model

        model = self._loader(name)
        if self._max_entries:
            self._cache[name] = model
            self._cache.move_to_end(name)
            while len(self._cache) > self._max_entries:
                self._cache.popitem(last=False)
            self.max_cache_size_seen = max(self.max_cache_size_seen, len(self._cache))
        return model

    def hydrate_batches(
        self, names: t.Iterable[str], batch_size: int
    ) -> t.Iterator[t.Tuple[Model, ...]]:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

        batch: t.List[Model] = []
        for name in sorted(set(names)):
            batch.append(self.hydrate(name))
            if len(batch) == batch_size:
                yield tuple(batch)
                batch = []
        if batch:
            yield tuple(batch)

    def evict(self, names: t.Iterable[str]) -> None:
        for name in names:
            self._cache.pop(name, None)
