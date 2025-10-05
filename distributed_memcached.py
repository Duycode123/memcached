#!/usr/bin/env python3
"""
distributed_memcached.py
- Consistent hashing ring + optional replication for multiple memcached nodes
- Uses pymemcache (pure Python client). Tested with memcached on localhost ports.
"""
import hashlib, bisect, time, argparse, random
from pymemcache.client import base

# -----------------------
# Consistent Hash Ring
# -----------------------
class ConsistentHashRing:
    def __init__(self, replicas=100):
        self.replicas = replicas
        self.ring = []              # sorted list of hashes
        self.hash2node = {}         # h -> node_name
        self.nodes = {}             # node_name -> client

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node_name: str, client):
        if node_name in self.nodes:
            return
        self.nodes[node_name] = client
        for i in range(self.replicas):
            h = self._hash(f"{node_name}:{i}")
            self.hash2node[h] = node_name
            self.ring.append(h)
        self.ring.sort()

    def remove_node(self, node_name: str):
        if node_name not in self.nodes:
            return
        del self.nodes[node_name]
        to_remove = [h for h,n in self.hash2node.items() if n == node_name]
        for h in to_remove:
            del self.hash2node[h]
            # remove from ring
            idx = bisect.bisect_left(self.ring, h)
            if idx < len(self.ring) and self.ring[idx] == h:
                self.ring.pop(idx)

    def get_node(self, key: str):
        if not self.ring:
            return None, None
        h = self._hash(key)
        idx = bisect.bisect(self.ring, h)
        if idx == len(self.ring):
            idx = 0
        node_name = self.hash2node[self.ring[idx]]
        return node_name, self.nodes[node_name]

    def get_nodes_for_key(self, key: str, count: int):
        """Return up to `count` distinct nodes (node_name, client) for replication."""
        if not self.ring:
            return []
        h = self._hash(key)
        idx = bisect.bisect(self.ring, h)
        found = []
        used_names = set()
        ring_len = len(self.ring)
        i = idx
        while len(found) < count and ring_len > 0:
            if i == ring_len:
                i = 0
            node_name = self.hash2node[self.ring[i]]
            if node_name not in used_names and node_name in self.nodes:
                found.append((node_name, self.nodes[node_name]))
                used_names.add(node_name)
            i += 1
            # safety break if we've looped full circle
            if len(used_names) == len(self.nodes):
                break
        return found

# -----------------------
# Distributed Client
# -----------------------
class DistributedMemcachedClient:
    def __init__(self, addrs, replicas=100, replication_factor=1, timeout=1):
        """
        addrs: list of ('host', port) tuples
        replication_factor: how many nodes to replicate to (>=1)
        """
        self.ring = ConsistentHashRing(replicas=replicas)
        self.replication_factor = max(1, replication_factor)
        for host, port in addrs:
            name = f"{host}:{port}"
            client = base.Client((host, port), connect_timeout=timeout, timeout=timeout)
            self.ring.add_node(name, client)
        # local bookkeeping of keys set through this client (for distribution stats)
        self.key_map = {}  # key -> list of node_names (where we attempted to store)

    def set(self, key, value, ttl=0):
        nodes = self.ring.get_nodes_for_key(key, self.replication_factor)
        results = {}
        success = False
        stored_nodes = []
        for node_name, client in nodes:
            try:
                client.set(key, value, expire=ttl)
                results[node_name] = True
                stored_nodes.append(node_name)
                success = True
            except Exception as e:
                results[node_name] = False
        # record where we attempted to store
        self.key_map[key] = stored_nodes
        return success, results

    def get(self, key):
        # Try primary first, then replicas
        nodes = self.ring.get_nodes_for_key(key, self.replication_factor)
        for node_name, client in nodes:
            try:
                v = client.get(key)
                if v is not None:
                    # decode bytes if possible
                    if isinstance(v, bytes):
                        try:
                            return v.decode()
                        except:
                            return v
                    return v
            except Exception:
                continue
        return None

    def delete(self, key):
        nodes = self.ring.get_nodes_for_key(key, self.replication_factor)
        results = {}
        for node_name, client in nodes:
            try:
                client.delete(key)
                results[node_name] = True
            except Exception:
                results[node_name] = False
        if key in self.key_map:
            del self.key_map[key]
        return results

    def distribution_stats(self):
        """Return simple counts of keys we've written (local bookkeeping)."""
        counts = {}
        for k, node_list in self.key_map.items():
            for n in node_list:
                counts[n] = counts.get(n, 0) + 1
        return counts

# -----------------------
# Simple CLI / test
# -----------------------
def demo():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bulk", type=int, default=0, help="Insert N random keys")
    parser.add_argument("--show", action="store_true", help="Show distribution stats")
    parser.add_argument("--get", type=str, help="Get key")
    parser.add_argument("--set", nargs=2, metavar=('KEY','VALUE'), help="Set single key value")
    parser.add_argument("--replication", type=int, default=1, help="Replication factor")
    parser.add_argument("--hosts", nargs='+', default=["127.0.0.1:11211","127.0.0.1:11212"], help="host:port list")
    args = parser.parse_args()

    addrs = []
    for h in args.hosts:
        host,port = h.split(":")
        addrs.append((host, int(port)))

    client = DistributedMemcachedClient(addrs, replicas=200, replication_factor=args.replication)

    if args.bulk and args.bulk > 0:
        n = args.bulk
        t0 = time.time()
        for i in range(n):
            k = f"key_{i}"
            v = f"value_{i}"
            client.set(k, v)
        t1 = time.time()
        print(f"Inserted {n} keys in {t1-t0:.3f}s")
        print("Distribution (local map):", client.distribution_stats())
        return

    if args.set:
        k, v = args.set
        ok, results = client.set(k, v)
        print("SET", k, "=>", v, "ok:", ok, results)
        return

    if args.get:
        k = args.get
        v = client.get(k)
        print("GET", k, "=>", v)
        return

    if args.show:
        print("Distribution (local bookkeeping):", client.distribution_stats())
        return

    parser.print_help()

if __name__ == "__main__":
    demo()
