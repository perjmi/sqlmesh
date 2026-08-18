from __future__ import annotations

import typing as t
from concurrent.futures import Future, as_completed
from pathlib import Path

from sqlglot import exp
from sqlglot.errors import SchemaError
from sqlglot.schema import MappingSchema

from sqlmesh.core.model.cache import (
    _init_optimized_query_cache,
    load_optimized_query_and_mapping,
    optimized_query_cache_pool,
    OptimizedQueryCache,
)
from sqlmesh.core.model.graph import ProjectGraphIndex
from sqlmesh.core.model.registry import ModelMetadata, ModelPayloadStore
from sqlmesh.core import constants as c
from sqlmesh.utils.process import PoolExecutor, create_process_pool_executor
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core.model.definition import Model
    from sqlmesh.utils import UniqueKeyDict
    from sqlmesh.utils.dag import DAG


def update_model_schemas(
    dag: DAG[str],
    models: UniqueKeyDict[str, Model],
    cache_dir: Path,
) -> None:
    schema = MappingSchema(normalize=False)
    optimized_query_cache: OptimizedQueryCache = OptimizedQueryCache(cache_dir)

    _update_model_schemas(dag, models, schema, optimized_query_cache)


_streaming_schema_index: t.Optional[ProjectGraphIndex] = None
_streaming_schema_payload_store: t.Optional[ModelPayloadStore] = None


def _init_streaming_schema_worker(
    index_path: str,
    payload_store_path: str,
    cache_dir: str,
) -> None:
    global _streaming_schema_index, _streaming_schema_payload_store

    _streaming_schema_index = ProjectGraphIndex(Path(index_path))
    _streaming_schema_payload_store = ModelPayloadStore(Path(payload_store_path), cleanup=False)
    _init_optimized_query_cache(OptimizedQueryCache(Path(cache_dir), cleanup=False))


def _finalize_streaming_schema_model(name: str) -> t.Tuple[ModelMetadata, int, int]:
    """Finalizes one model entirely inside a disposable worker process."""
    if _streaming_schema_index is None or _streaming_schema_payload_store is None:
        raise SQLMeshError("Streaming schema worker was not initialized")

    index = _streaming_schema_index
    payload_store = _streaming_schema_payload_store
    metadata = index.metadata(name)
    model = payload_store.get(metadata)
    if model is None:
        raise SQLMeshError(f"Missing discovered model payload for '{name}'")

    mapping = {}
    max_hydrated_models = 1
    for parent_name in metadata.dependencies:
        if not index.contains(parent_name):
            continue
        parent_metadata = index.metadata(parent_name)
        parent = payload_store.get(parent_metadata)
        if parent is None:
            raise SQLMeshError(f"Missing finalized parent payload for '{parent_name}'")
        mapping[parent_name] = parent.columns_to_types
        max_hydrated_models = 2
        del parent

    try:
        fqn, _, data_hash, metadata_hash, mapping_schema = (
            load_optimized_query_and_mapping(model, mapping=mapping)
        )
        if fqn != name:
            raise SQLMeshError(f"Schema worker returned '{fqn}' while processing '{name}'")
        model._data_hash = data_hash
        model._metadata_hash = metadata_hash
        if model.mapping_schema != mapping_schema:
            model.set_mapping_schema(mapping_schema)
        if model.columns_to_types:
            # Validate the nesting level in the coordinator after the bounded result is returned.
            nesting_level = len(exp.to_table(model.fqn, dialect=model.dialect).parts)
        else:
            nesting_level = 0
        model.validate_definition()
    except Exception as ex:
        raise SchemaError(f"Failed to update model schemas\n\n{ex}")

    final_metadata = ModelMetadata.from_model(model)
    payload_store.put(model, final_metadata)
    return final_metadata, max_hydrated_models, nesting_level


def _shutdown_schema_executor(executor: t.Optional[PoolExecutor]) -> None:
    if executor is not None:
        executor.shutdown(wait=True)


def update_model_schemas_streaming(
    index: ProjectGraphIndex,
    payload_store: ModelPayloadStore,
    cache_dir: Path,
    batch_size: int,
    max_workers: t.Optional[int] = None,
    worker_max_tasks: int = 25,
) -> int:
    """Propagates schemas without retaining the complete set of model objects.

    Each model is hydrated with at most one direct parent at a time. Parent column mappings, which
    are the semantic input needed by query optimization, are copied into a compact mapping before
    the parent payload is released. Returns the maximum simultaneously hydrated model count.
    """
    if worker_max_tasks <= 0:
        raise ValueError("worker_max_tasks must be greater than 0")

    configured_workers = c.MAX_FORK_WORKERS if max_workers is None else max_workers
    resolved_workers = max(1, configured_workers or 1)
    task_capacity = resolved_workers * worker_max_tasks
    max_hydrated_models = 0
    expected_nesting_level: t.Optional[int] = None
    executor: t.Optional[PoolExecutor] = None
    tasks_in_executor = 0

    try:
        for names in index.iter_topological_batches(batch_size):
            offset = 0
            while offset < len(names):
                if executor is None:
                    executor = create_process_pool_executor(
                        initializer=_init_streaming_schema_worker,
                        initargs=(str(index.path), str(payload_store.path), str(cache_dir)),
                        max_workers=resolved_workers,
                    )
                    tasks_in_executor = 0

                available_capacity = task_capacity - tasks_in_executor
                chunk = names[offset : offset + available_capacity]
                futures: t.List[Future[t.Tuple[ModelMetadata, int, int]]] = [
                    executor.submit(_finalize_streaming_schema_model, name) for name in chunk
                ]
                results = [future.result() for future in as_completed(futures)]
                results.sort(key=lambda result: result[0].fqn)
                index.update_payload_references(result[0] for result in results)

                hydrated_counts = sorted(
                    (result[1] for result in results), reverse=True
                )[:resolved_workers]
                max_hydrated_models = max(max_hydrated_models, sum(hydrated_counts))
                for metadata, _, nesting_level in results:
                    if not nesting_level:
                        continue
                    if expected_nesting_level is None:
                        expected_nesting_level = nesting_level
                    elif nesting_level != expected_nesting_level:
                        from sqlmesh.core.console import get_console

                        get_console().log_error(
                            "SQLMesh requires all model names and references to have the same "
                            "level of nesting."
                        )
                        raise SchemaError(
                            f"Model '{metadata.fqn}' has nesting level {nesting_level}; expected "
                            f"{expected_nesting_level}"
                        )

                tasks_in_executor += len(chunk)
                offset += len(chunk)
                if tasks_in_executor == task_capacity:
                    _shutdown_schema_executor(executor)
                    executor = None
    finally:
        _shutdown_schema_executor(executor)

    return max_hydrated_models


def _update_schema_with_model(schema: MappingSchema, model: Model) -> None:
    columns_to_types = model.columns_to_types
    if columns_to_types:
        try:
            schema.add_table(model.fqn, columns_to_types, dialect=model.dialect)
        except SchemaError as e:
            if "nesting level:" in str(e):
                from sqlmesh.core.console import get_console

                get_console().log_error(
                    "SQLMesh requires all model names and references to have the same level of nesting."
                )
            raise


def _update_model_schemas(
    dag: DAG[str],
    models: UniqueKeyDict[str, Model],
    schema: MappingSchema,
    optimized_query_cache: OptimizedQueryCache,
) -> None:
    futures = set()
    graph = {
        model: {dep for dep in deps if dep in models}
        for model, deps in dag._dag.items()
        if model in models
    }

    def process_models(completed_model: t.Optional[Model] = None) -> None:
        for name in list(graph):
            deps = graph[name]

            if completed_model:
                deps.discard(completed_model.fqn)

            if not deps:
                del graph[name]
                model = models[name]
                futures.add(
                    executor.submit(
                        load_optimized_query_and_mapping,
                        model,
                        mapping={
                            parent: models[parent].columns_to_types
                            for parent in model.depends_on
                            if parent in models
                        },
                    )
                )

    with optimized_query_cache_pool(optimized_query_cache) as executor:
        process_models()

        while futures:
            for future in as_completed(futures):
                try:
                    futures.remove(future)
                    fqn, entry_name, data_hash, metadata_hash, mapping_schema = future.result()
                    model = models[fqn]
                    model._data_hash = data_hash
                    model._metadata_hash = metadata_hash
                    if model.mapping_schema != mapping_schema:
                        model.set_mapping_schema(mapping_schema)
                    optimized_query_cache.with_optimized_query(model, entry_name)
                    _update_schema_with_model(schema, model)
                    process_models(completed_model=model)
                except Exception as ex:
                    raise SchemaError(f"Failed to update model schemas\n\n{ex}")
