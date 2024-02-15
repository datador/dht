import hashlib
import random
from typing import Dict, Optional

class Node:
    """ Represents a node in the Chord ring. """

    def __init__(self, node_id: int):
        """
        Initialize a Node with a given identifier.

        :param node_id: Unique identifier of the node.
        """
        self.node_id = node_id
        self.data = {}  # Stores data extents assigned to this node

    def store_data(self, key: str, value: str):
        """
        Store a key-value pair in the node's data.

        :param key: The key of the data extent.
        :param value: The value of the data extent.
        """
        self.data[key] = value


class ChordRing:
    """ Represents a Chord Distributed Hash Table (DHT) ring. """

    def __init__(self, m: int, num_extents: int, replication_factor: int):
        """
        Initialize the Chord ring with specific parameters.

        :param m: The size of the address space (2^m).
        :param num_extents: The number of data extents in the system.
        :param replication_factor: The number of replicas for each data extent.
        """
        self.nodes = []  # List of nodes in the Chord ring
        self.m = m
        self.max_nodes = 2 ** m
        self.num_extents = num_extents
        self.replication_factor = replication_factor

    def hash_key(self, key: str) -> int:
        """
        Hash a key to an integer using SHA-1 and modulo operation.

        :param key: The key to hash.
        :return: An integer hash value.
        """
        return int(hashlib.sha1(key.encode()).hexdigest(), 16) % self.max_nodes

    def add_node(self, node_id: int):
        """
        Add a new node to the Chord ring.

        :param node_id: The identifier of the new node.
        """
        new_node = Node(node_id)
        self.nodes.append(new_node)
        self.nodes.sort(key=lambda x: x.node_id)
        self.redistribute_data()

    def redistribute_data(self):
        """ Redistribute data among the nodes after any change in the node list. """
        for node in self.nodes:
            node.data.clear()

        for extent_id in range(self.num_extents):
            key = f'extent{extent_id}'
            primary_node = self.find_node(key)
            primary_node.store_data(key, f'data{extent_id}')

            # Replicate data to the next nodes based on replication factor
            for i in range(self.replication_factor):
                next_node = self.get_next_node(primary_node)
                next_node.store_data(key, f'data{extent_id}')
                primary_node = next_node

    def find_node(self, key: str) -> Node:
        """
        Find the node responsible for a given key.

        :param key: The key to locate in the ring.
        :return: Node object that is responsible for the key.
        """
        key_hash = self.hash_key(key)
        for node in self.nodes:
            if node.node_id >= key_hash:
                return node
        return self.nodes[0]

    def get_next_node(self, current_node: Node) -> Node:
        """
        Find the next node in the ring.

        :param current_node: The current node.
        :return: The next node in the ring.
        """
        current_index = self.nodes.index(current_node)
        next_index = (current_index + 1) % len(self.nodes)
        return self.nodes[next_index]

    def lookup_data(self, key: str) -> Optional[str]:
        """
        Lookup data for a given key.

        :param key: The key to look up.
        :return: The data associated with the key, or None if not found.
        """
        node = self.find_node(key)
        return node.data.get(key)

    def simulate_workload(self, num_operations: int) -> Dict[int, int]:
        """
        Simulate a workload of random write operations.

        :param num_operations: Number of write operations to simulate.
        :return: A dictionary mapping node IDs to the number of operations handled.
        """
        operation_count = {node.node_id: 0 for node in self.nodes}
        for _ in range(num_operations):
            random_extent = f'extent{random.randint(0, self.num_extents - 1)}'
            random_data = f'data{random.randint(0, 1000000)}'
            self.store_data(random_extent, random_data)

            primary_node = self.find_node(random_extent)
            operation_count[primary_node.node_id] += 1

            for _ in range(self.replication_factor):
                primary_node = self.get_next_node(primary_node)
                operation_count[primary_node.node_id] += 1

        return operation_count

    def store_data(self, key: str, value: str):
        """
        Store data in the appropriate node and its replicas.

        :param key: The key of the data extent.
        :param value: The value of the data extent.
        """
        node = self.find_node(key)
        node.store_data(key, value)
        for _ in range(self.replication_factor):
            node = self.get_next_node(node)
            node.store_data(key, value)

    def get_load_distribution(self) -> Dict[int, int]:
        """
        Get the distribution of data across nodes.

        :return: A dictionary mapping node IDs to the number of data extents they hold.
        """
        return {node.node_id: len(node.data) for node in self.nodes}
