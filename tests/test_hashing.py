from collections import Counter

from shared.shared.hashing import ConsistentHashRing


def _coefficient_of_variation(counts: Counter[str], nodes: list[str]) -> float:
    values = [counts[node] for node in nodes]
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return (variance ** 0.5) / mean


def test_even_distribution_across_three_nodes() -> None:
    nodes = ["worker1", "worker2", "worker3"]
    ring = ConsistentHashRing(nodes=nodes, vnodes=150)

    docs = [f"doc-{i}" for i in range(12000)]
    counts = Counter(ring.get_node(doc) for doc in docs)

    expected = len(docs) / len(nodes)
    tolerance = expected * 0.15

    for node in nodes:
        assert abs(counts[node] - expected) <= tolerance


def test_node_removal_reroutes_correctly() -> None:
    nodes = ["worker1", "worker2", "worker3"]
    ring = ConsistentHashRing(nodes=nodes, vnodes=150)

    docs = [f"doc-{i}" for i in range(5000)]
    before = {doc: ring.get_node(doc) for doc in docs}

    removed = "worker2"
    ring.remove_node(removed)
    after = {doc: ring.get_node(doc) for doc in docs}

    assert all(node != removed for node in after.values())

    moved_docs = [doc for doc in docs if before[doc] == removed]
    assert moved_docs
    assert all(after[doc] in {"worker1", "worker3"} for doc in moved_docs)


def test_vnode_count_affects_distribution_variance() -> None:
    nodes = ["worker1", "worker2", "worker3"]
    docs = [f"doc-{i}" for i in range(9000)]

    low_vnode_ring = ConsistentHashRing(nodes=nodes, vnodes=1)
    high_vnode_ring = ConsistentHashRing(nodes=nodes, vnodes=150)

    low_counts = Counter(low_vnode_ring.get_node(doc) for doc in docs)
    high_counts = Counter(high_vnode_ring.get_node(doc) for doc in docs)

    low_cv = _coefficient_of_variation(low_counts, nodes)
    high_cv = _coefficient_of_variation(high_counts, nodes)

    assert high_cv < low_cv
