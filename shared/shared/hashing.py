import hashlib
from bisect import bisect
from typing import Dict, List, Sequence


def hash_payload(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class ConsistentHashRing:
    def __init__(self, nodes: Sequence[str] | None = None, vnodes: int = 150) -> None:
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")

        self.vnodes = vnodes
        self._ring: List[int] = []
        self._hash_to_node: Dict[int, str] = {}
        self._nodes: set[str] = set()

        if nodes:
            for node in nodes:
                self.add_node(node)

    @staticmethod
    def _hash(value: str) -> int:
        return int(hashlib.sha256(value.encode("utf-8")).hexdigest(), 16)

    def add_node(self, node: str) -> None:
        if node in self._nodes:
            return

        self._nodes.add(node)
        for i in range(self.vnodes):
            vnode_key = f"{node}:{i}"
            vnode_hash = self._hash(vnode_key)
            self._hash_to_node[vnode_hash] = node
            self._ring.append(vnode_hash)

        self._ring.sort()

    def remove_node(self, node: str) -> None:
        if node not in self._nodes:
            return

        self._nodes.remove(node)
        node_hashes = [h for h, owner in self._hash_to_node.items() if owner == node]
        node_hashes_set = set(node_hashes)

        for vnode_hash in node_hashes:
            del self._hash_to_node[vnode_hash]

        self._ring = [h for h in self._ring if h not in node_hashes_set]

    def get_node(self, document_id: str) -> str:
        if not self._ring:
            raise ValueError("hash ring has no nodes")

        doc_hash = self._hash(document_id)
        idx = bisect(self._ring, doc_hash)
        if idx == len(self._ring):
            idx = 0
        vnode_hash = self._ring[idx]
        return self._hash_to_node[vnode_hash]

    def get_nodes(self, document_id: str, n: int) -> List[str]:
        if n < 1:
            raise ValueError("n must be >= 1")
        if not self._ring:
            raise ValueError("hash ring has no nodes")

        target = min(n, len(self._nodes))
        doc_hash = self._hash(document_id)
        idx = bisect(self._ring, doc_hash)
        if idx == len(self._ring):
            idx = 0

        replicas: List[str] = []
        seen: set[str] = set()
        cursor = idx

        while len(replicas) < target:
            node = self._hash_to_node[self._ring[cursor]]
            if node not in seen:
                seen.add(node)
                replicas.append(node)
            cursor = (cursor + 1) % len(self._ring)

        return replicas
