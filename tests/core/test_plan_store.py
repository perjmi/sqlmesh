from sqlglot import parse_one

from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.model import SqlModel
from sqlmesh.core.plan import PlanBuilder
from sqlmesh.core.plan.store import SerializedSnapshot, SnapshotPlanStore
from sqlmesh.core.snapshot import SnapshotChangeCategory


def test_snapshot_plan_store_is_persistent_and_bounded(tmp_path, make_snapshot):
    path = tmp_path / "plan.sqlite"
    snapshots = [
        make_snapshot(SqlModel(name=f"model_{index}", query=parse_one(f"SELECT {index} AS value")))
        for index in range(5)
    ]

    store = SnapshotPlanStore(path, max_cached_snapshots=2)
    for snapshot in reversed(snapshots):
        store.put_snapshot(snapshot, is_new=snapshot.name != snapshots[0].name)

    assert list(store.snapshots) == [snapshot.snapshot_id for snapshot in snapshots]
    assert list(store.new_snapshots) == [snapshot.snapshot_id for snapshot in snapshots[1:]]
    assert len(store.new_snapshot_sequence) == 4
    assert store.new_snapshot_sequence[0].snapshot_id == snapshots[1].snapshot_id
    assert store.new_snapshot_sequence[-1].snapshot_id == snapshots[-1].snapshot_id
    assert [snapshot.snapshot_id for snapshot in store.new_snapshot_sequence[1:3]] == [
        snapshots[2].snapshot_id,
        snapshots[3].snapshot_id,
    ]
    assert len(store.snapshots) == 5

    for snapshot_id in store.snapshots:
        assert store.snapshots[snapshot_id].snapshot_id == snapshot_id

    assert store.cache_size <= 2
    assert store.max_cache_size_seen == 2

    reopened = SnapshotPlanStore(path, max_cached_snapshots=1)
    assert reopened.snapshots[snapshots[3].snapshot_id].name == snapshots[3].name
    assert snapshots[0].snapshot_id not in reopened.new_snapshots


def test_snapshot_plan_store_accepts_worker_serialized_snapshots(tmp_path, make_snapshot):
    snapshot = make_snapshot(SqlModel(name="model", query=parse_one("SELECT 1 AS value")))
    serialized = SerializedSnapshot.from_snapshot(snapshot, is_new=True)
    store = SnapshotPlanStore(tmp_path / "plan.sqlite", max_cached_snapshots=0)

    store.put_serialized_snapshots((serialized,))

    restored = store.snapshots[snapshot.snapshot_id]
    assert restored.dict() == snapshot.dict()
    assert restored.snapshot_id in store.new_snapshots
    assert set(store.parents(snapshot.snapshot_id)) == set(snapshot.parents)


def test_snapshot_plan_store_persists_mutations_when_a_snapshot_is_evicted(tmp_path, make_snapshot):
    snapshots = [
        make_snapshot(SqlModel(name=name, query=parse_one("SELECT 1 AS value")))
        for name in ("a", "b")
    ]
    store = SnapshotPlanStore(tmp_path / "plan.sqlite", max_cached_snapshots=1)
    for snapshot in snapshots:
        store.put_snapshot(snapshot, is_new=True)

    stored_a = store.snapshots[snapshots[0].snapshot_id]
    stored_a.categorize_as(SnapshotChangeCategory.BREAKING)
    store.save_snapshot(stored_a)
    store.snapshots[snapshots[1].snapshot_id]
    assert (
        store.snapshots[snapshots[0].snapshot_id].change_category == SnapshotChangeCategory.BREAKING
    )
    store.flush()

    reopened = SnapshotPlanStore(tmp_path / "plan.sqlite", max_cached_snapshots=0)
    assert (
        reopened.snapshots[snapshots[0].snapshot_id].change_category
        == SnapshotChangeCategory.BREAKING
    )


def test_snapshot_plan_store_traverses_snapshot_edges_in_topological_batches(
    tmp_path, make_snapshot
):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("SELECT 1 AS value")))
    snapshot_b = make_snapshot(
        SqlModel(name="b", query=parse_one("SELECT * FROM a"), depends_on={"a"}),
        nodes={snapshot_a.name: snapshot_a.node},
    )
    snapshot_c = make_snapshot(
        SqlModel(name="c", query=parse_one("SELECT * FROM b"), depends_on={"b"}),
        nodes={snapshot_a.name: snapshot_a.node, snapshot_b.name: snapshot_b.node},
    )

    store = SnapshotPlanStore(tmp_path / "plan.sqlite", max_cached_snapshots=1)
    for snapshot in (snapshot_c, snapshot_a, snapshot_b):
        store.put_snapshot(snapshot, is_new=True)

    assert list(store.iter_topological_batches(batch_size=2)) == [
        (snapshot_a.snapshot_id,),
        (snapshot_b.snapshot_id,),
        (snapshot_c.snapshot_id,),
    ]
    assert store.upstream(snapshot_c.snapshot_id) == {
        snapshot_a.snapshot_id,
        snapshot_b.snapshot_id,
    }
    assert store.downstream(snapshot_a.snapshot_id) == {
        snapshot_b.snapshot_id,
        snapshot_c.snapshot_id,
    }


def test_streaming_plan_keeps_modified_snapshot_and_catalog_mutations_in_sync(
    tmp_path, make_snapshot
):
    old = make_snapshot(SqlModel(name="a", query=parse_one("SELECT 1 AS value")))
    old.categorize_as(SnapshotChangeCategory.BREAKING)
    new = make_snapshot(SqlModel(name="a", query=parse_one("SELECT 2 AS value")))
    store = SnapshotPlanStore(tmp_path / "plan.sqlite", max_cached_snapshots=1, write_batch_size=1)
    store.put_snapshot(new, is_new=True)
    context_diff = ContextDiff(
        environment="prod",
        is_new_environment=True,
        is_unfinalized_environment=False,
        normalize_environment_name=True,
        previous_gateway_managed_virtual_layer=False,
        gateway_managed_virtual_layer=False,
        create_from="prod",
        create_from_env_exists=False,
        added=set(),
        removed_snapshots={},
        modified_snapshots={new.name: (new, old)},
        snapshots=store.snapshots,
        new_snapshots=store.new_snapshots,
        previous_plan_id=None,
        previously_promoted_snapshot_ids=set(),
        previous_finalized_snapshots=None,
        environment_statements=[],
    )

    plan = PlanBuilder(context_diff, skip_backfill=True).build()
    stored = plan.context_diff.snapshots[new.snapshot_id]
    modified = plan.context_diff.modified_snapshots[new.name][0]

    assert stored.categorized
    assert modified.change_category == stored.change_category
    assert modified.version == stored.version
