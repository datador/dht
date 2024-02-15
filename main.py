  # imports
from src.chord import ChordRing
import random

# Parameters
m = 10  # size of the key space
num_extents = 10000
workload = 1000000
replication_factor = 3
chord_ring = ChordRing(m, num_extents, replication_factor)

# Add initial nodes and simulate workload
initial_nodes = 10
for i in range(initial_nodes):
    chord_ring.add_node(random.randint(0, chord_ring.max_nodes))

initial_distribution = chord_ring.get_load_distribution()
print("Initial distribution:", initial_distribution)

# Perform 1,000,000 random write operations
write_distribution = chord_ring.simulate_workload(workload)
print("Write Distribution:", write_distribution)

# Add more nodes and check distribution
additional_nodes = 5
for i in range(initial_nodes, initial_nodes + additional_nodes):
    chord_ring.add_node(random.randint(0, chord_ring.max_nodes))

updated_distribution = chord_ring.get_load_distribution()
print("Updated distribution after adding more nodes:", updated_distribution)

# Lookup data
key_to_lookup = 'extent500'
data = chord_ring.lookup_data(key_to_lookup)
print(f"Data for {key_to_lookup}: {data}")

# Test if sums are accurate
counter = 0
for i in range(len(chord_ring.nodes)):
    counter += len(chord_ring.nodes[i].data)
    print(f"Node{i} ({chord_ring.nodes[i].node_id}):",len(chord_ring.nodes[i].data))
print("total sum:", counter)