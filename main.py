  # imports
from src.chord import ChordRing
import random
from src.test_functions import print_distribution_statistics, plot_distribution, lookup_and_measure_time

# Parameters
m = 32  # size of the key space
num_extents = 10000
workload = 1000
replication_factor = 3
chord_ring = ChordRing(m, num_extents, replication_factor)

# Add initial nodes and simulate workload
initial_nodes = 10
for i in range(initial_nodes):
    chord_ring.add_node(random.randint(0, chord_ring.max_nodes))

initial_distribution = chord_ring.get_load_distribution()

# Print initial distribution statistics
print_distribution_statistics('Initial Distribution', initial_distribution.values())

# Plot initial distribution
plot_distribution(initial_distribution.values(), initial_nodes, 'Initial Distribution')

# Perform 1,000,000 random write operations
write_distribution = chord_ring.simulate_workload(workload)
print("Write Distribution:", write_distribution)
print("Sum of write distribution", sum(write_distribution.values()))

# Add more nodes and check distribution
nodes_per_increment = 5
max_number_of_nodes = 30 # maximum number of nodes
current_number_of_nodes = initial_nodes

for _ in range(int((max_number_of_nodes - initial_nodes) / nodes_per_increment)):
    current_number_of_nodes = current_number_of_nodes + nodes_per_increment
    for _ in range(nodes_per_increment):
        chord_ring.add_node(random.randint(0, chord_ring.max_nodes - 1))

    # Get updated distribution
    updated_distribution = chord_ring.get_load_distribution()
    # Simulate workload
    write_distribution = chord_ring.simulate_workload(workload)
    print("Write Distribution:", write_distribution)

    # Print updated distribution statistics
    print_distribution_statistics('Updated Distribution', updated_distribution.values())

    # Plot updated distribution
    plot_distribution(updated_distribution.values(), current_number_of_nodes, 'Updated Distribution')

# Lookup data
key_to_lookup = 'extent500'
lookup_and_measure_time(key_to_lookup, chord_ring)
