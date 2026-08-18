from sqlglot import parse_one

from sqlmesh.core.model import SqlModel
from sqlmesh.core.plan.store import SnapshotPlanStore
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
