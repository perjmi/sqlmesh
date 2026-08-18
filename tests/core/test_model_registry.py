from pathlib import Path

from sqlglot import parse_one

from sqlmesh.core.model import create_sql_model
from sqlmesh.core.model.registry import EagerModelRegistry, ModelMetadata
from sqlmesh.core.selector import MetadataSelector, NativeSelector


def test_eager_registry_exposes_deterministic_metadata_and_graph_queries():
    model_a = create_sql_model(
        "catalog.db.a", parse_one("SELECT 1 AS id"), path=Path("models/a.sql")
    )
    model_b = create_sql_model(
        "catalog.db.b",
        parse_one("SELECT * FROM catalog.db.a"),
        depends_on={model_a.fqn},
        path=Path("models/b.sql"),
    )
    model_c = create_sql_model(
        "catalog.db.c",
        parse_one("SELECT * FROM catalog.db.b"),
        depends_on={model_b.fqn},
        path=Path("models/c.sql"),
        tags=["daily"],
        project="analytics",
    )
    registry = EagerModelRegistry(
        {model.fqn: model for model in (model_c, model_a, model_b)}
    )

    assert list(metadata.fqn for metadata in registry.iter_metadata()) == [
        model_a.fqn,
        model_b.fqn,
        model_c.fqn,
    ]
    assert registry.metadata(model_c.fqn) == ModelMetadata(
        fqn=model_c.fqn,
        name="catalog.db.c",
        source_path=Path("models/c.sql"),
        project="analytics",
        dialect="",
        gateway=None,
        enabled=True,
        kind_name="VIEW",
        dependencies=(model_b.fqn,),
        tags=("daily",),
        dbt_fqn=None,
    )
    assert registry.upstream({model_c.fqn}) == {model_a.fqn, model_b.fqn}
    assert registry.downstream({model_a.fqn}) == {model_b.fqn, model_c.fqn}


def test_eager_registry_hydrates_in_stable_bounded_batches():
    registry = EagerModelRegistry(
        {
            name: create_sql_model(name, parse_one("SELECT 1 AS id"))
            for name in ("db.c", "db.a", "db.b")
        }
    )

    batches = list(registry.hydrate_batches({"db.c", "db.a", "db.b"}, batch_size=2))

    assert [[model.fqn for model in batch] for batch in batches] == [
        ['"db"."a"', '"db"."b"'],
        ['"db"."c"'],
    ]
    registry.evict(model.fqn for batch in batches for model in batch)
    assert len(registry) == 3


def test_metadata_selector_matches_eager_native_selection():
    models = [
        create_sql_model("model1", parse_one("SELECT 1"), tags=["source"]),
        create_sql_model(
            "model2", parse_one("SELECT * FROM model1"), depends_on={'"model1"'}, tags=["daily"]
        ),
        create_sql_model(
            "model3", parse_one("SELECT * FROM model2"), depends_on={'"model2"'}, tags=["daily"]
        ),
        create_sql_model("other", parse_one("SELECT 1"), tags=["other"]),
    ]
    registry = EagerModelRegistry({model.fqn: model for model in models})
    eager = NativeSelector(object(), registry)
    metadata = MetadataSelector(registry)

    for selection in (
        ["*"],
        ["tag:daily"],
        ["+tag:daily"],
        ["tag:source+"],
        ["model* & ^model1"],
        ["model1 | other"],
        ["resource_type:model"],
    ):
        assert metadata.expand_model_selections(selection) == eager.expand_model_selections(
            selection
        )

    selected, upstream, downstream = metadata.selection_boundary(["model2"])
    assert selected == {'"model2"'}
    assert upstream == {'"model1"'}
    assert downstream == {'"model3"'}
