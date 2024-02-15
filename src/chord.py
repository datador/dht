import hashlib
import random

class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.data = {}

    def store_data(self, key, value):
        self.data[key] = value


class ChordRing:
    def __init__(self, m, num_extents, replication_factor):
        self.nodes = []
        self.m = m
        self.max_nodes = 2 ** m
        self.num_extents = num_extents
        self.replication_factor = replication_factor

    def hash_key(self, key):
        return int(hashlib.sha1(key.encode()).hexdigest(), 16) % self.max_nodes

    def add_node(self, node_id):
        new_node = Node(node_id)
        self.nodes.append(new_node)
        self.nodes.sort(key=lambda x: x.node_id)
        self.redistribute_data()

    def redistribute_data(self):
        for node in self.nodes:
            node.data.clear()

        for extent_id in range(self.num_extents):
            key = f'extent{extent_id}'
            primary_node = self.find_node(key)
            primary_node.store_data(key, f'data{extent_id}')

            # Replicate data
            for i in range(self.replication_factor):
                next_node = self.get_next_node(primary_node)
                next_node.store_data(key, f'data{extent_id}')
                primary_node = next_node

    def find_node(self, key):
        key_hash = self.hash_key(key)
        for node in self.nodes:
            if node.node_id >= key_hash:
                return node
        return self.nodes[0]

    def get_next_node(self, current_node):
        current_index = self.nodes.index(current_node)
        next_index = (current_index + 1) % len(self.nodes)
        return self.nodes[next_index]

    def lookup_data(self, key):
        node = self.find_node(key)
        return node.data.get(key, None)

    # def simulate_workload(self):
    #     for i in range(self.num_extents):
    #         key = f'extent{i}'
    #         value = f'data{i}'
    #         self.store_data(key, value)

    def simulate_workload(self, num_operations):
        operation_count = {node.node_id: 0 for node in self.nodes}
        for _ in range(num_operations):
            # Create a random extent and data for each operation
            random_extent = f'extent{random.randint(0, self.num_extents - 1)}'
            random_data = f'data{random.randint(0, 1000000)}'
            self.store_data(random_extent, random_data)

            # Track which server handles the write operation
            primary_node = self.find_node(random_extent)
            operation_count[primary_node.node_id] += 1

            # Also track the writes for replicas
            for _ in range(self.replication_factor):
                primary_node = self.get_next_node(primary_node)
                operation_count[primary_node.node_id] += 1

        return operation_count

    def store_data(self, key, value):
        node = self.find_node(key)
        node.store_data(key, value)
        # Replicate data
        for _ in range(self.replication_factor):
            node = self.get_next_node(node)
            node.store_data(key, value)

    def get_load_distribution(self):
        return {node.node_id: len(node.data) for node in self.nodes}
