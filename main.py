  # imports
from src.chord import ChordRing

# Parameters
nodes = 10
num_extents = 10000
workload_size = 1000000
num_servers_max = 20
servers_increment = 5
m=10
n=3


# Initialize Chord Ring with 10 servers and 10,000 extents

chord_ring = ChordRing(m=m, num_extents=num_extents, initial_nodes=nodes,n=n)
chord_ring.simulate_workload(workload_size)
distribution_after_scaling = chord_ring.analyze_workload_distribution()
print(f"Workload Distribution after scaling to {nodes} nodes:", distribution_after_scaling)

# Scale out experiment
for total_nodes in range(15, num_servers_max+1, servers_increment):  # Incrementing by 5 each time up to 30 nodes
    for _ in range(servers_increment):  # Add 5 new nodes
        chord_ring.add_random_node()
    chord_ring.simulate_workload(workload_size)
    distribution_after_scaling = chord_ring.analyze_workload_distribution()
    print(f"Workload Distribution after scaling to {total_nodes} nodes:", distribution_after_scaling)

# Lookup for a specific extent
extent_id_to_lookup = 'extent124' 
lookup_info = chord_ring.lookup_data(str(extent_id_to_lookup))
print(f"Data for extent {extent_id_to_lookup}: {lookup_info}")