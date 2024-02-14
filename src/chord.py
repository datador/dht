import hashlib
import bisect
import random
import time 

class Node:
    def __init__(self, node_id, m):
        self.node_id = node_id
        self.data = {}
        self.finger_table = []
        self.predecessor = None
        self.m = m
        self.counter = 0

    def store_data(self, key, value):
        self.data[key] = value
        self.counter += 1

    def find_successor(self, key_hash):
        if self.node_id < key_hash <= self.finger_table[0].node_id or self.node_id == self.finger_table[0].node_id:
            return self.finger_table[0]
        else:
            # Find the closest preceding node
            for node in reversed(self.finger_table):
                if node.node_id < key_hash:
                    return node.find_successor(key_hash)
            return self
    def get_counter(self):
        return self.counter

class ChordRing:
    def __init__(self, m, num_extents, initial_nodes, n):
        self.m = m
        self.max_nodes = 2 ** m
        self.nodes = []
        self.extents = {i: None for i in range(num_extents)}
        self.replicas = n

        # Automatically add initial nodes
        for _ in range(initial_nodes):
            self.add_random_node()

    def add_random_node(self):
        unique_attribute = self.generate_unique_attribute()
        hashed_value = int(hashlib.sha1(unique_attribute.encode()).hexdigest(), 16)
        node_id = hashed_value % self.max_nodes
        self.add_node(node_id)

    def generate_unique_attribute(self):
        # Generate a unique attribute, e.g., a combination of timestamp and random string
        return str(time.time()) + str(random.random())

    def hash_key(self, key):
        return int(hashlib.sha1(key.encode()).hexdigest(), 16) % self.max_nodes

    def add_node(self, node_id):
        new_node = Node(node_id, self.m)
        self.nodes.append(new_node)
        self.nodes.sort(key=lambda node: node.node_id)
        self.update_finger_tables()
        self.redistribute_data(new_node)

    def remove_node(self, node_id):
        node_to_remove = next((node for node in self.nodes if node.node_id == node_id), None)
        if node_to_remove:
            self.nodes.remove(node_to_remove)
            self.update_finger_tables()
            self.redistribute_data_on_removal(node_to_remove)

    def update_finger_tables(self):
        for node in self.nodes:
            node.finger_table = []
            for k in range(self.m):
                successor_id = (node.node_id + 2 ** k) % self.max_nodes
                successor = self.find_successor(successor_id)
                node.finger_table.append(successor)

    def find_successor(self, key):
        # Convert key to an integer hash if it's not already
        if isinstance(key, str):
            key_hash = self.hash_key(key)
        else:
            key_hash = key

        # Handle case where no nodes are in the ring
        if not self.nodes:
            return None

        # Handle wrap-around case
        if key_hash > self.nodes[-1].node_id:
            return self.nodes[0]

        # Binary search to find the successor node
        left, right = 0, len(self.nodes) - 1
        while left <= right:
            mid = (left + right) // 2
            mid_node_id = self.nodes[mid].node_id

            if mid_node_id < key_hash:
                left = mid + 1
            elif mid_node_id > key_hash:
                right = mid - 1
            else:
                return self.nodes[mid]

        return self.nodes[left]

    def redistribute_data(self, new_node):
        # Assign data to the new node based on its responsibility
        for node in self.nodes:
            if node == new_node:
                continue
            keys_to_move = [key for key in node.data.keys() if self.find_successor(self.hash_key(key)) == new_node]
            for key in keys_to_move:
                new_node.store_data(key, node.data[key])
                del node.data[key]

    def redistribute_data_on_removal(self, removed_node):
        # Reassign data from the removed node to its successor
        successor = self.find_successor(removed_node.node_id + 1)
        for key, value in removed_node.data.items():
            successor.store_data(key, value)
  
    def store_data(self, key, value):
        key_hash = self.hash_key(key)
        initial_node = self.find_successor(key_hash)
        initial_node.store_data(key, value)

        # Replicate the data on the next 'self.replicas' successor nodes (n=1 means replication factor of 1, so stored on 2 nodes total)
        current_node = initial_node
        for _ in range(self.replicas):
            # Find the immediate successor of the current node from the fingertables (always index 0 for next in line)
            next_node_id = current_node.finger_table[0].node_id
            next_node = self.find_successor(next_node_id)
            next_node.store_data(key, value)

            # update next node for next round of iter
            current_node = next_node

    def lookup_data(self, key):
        key_hash = self.hash_key(key)
        node = self.find_successor(key_hash)
        data = node.data.get(key, None)
        return (node.node_id, key, data)  # Return the node ID instead of the node object


    def simulate_workload(self, num_operations):
        for i in range(1, num_operations+1):  # Assuming 10,000 extents
            extent_name = f"extent{i % 10000}"
            data = f"data{i}"
            self.store_data(extent_name, data)

    def analyze_workload_distribution(self):
        return {i+1: node.get_counter() for i,node in enumerate(self.nodes)}