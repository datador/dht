  # imports
from src.chord import ChordRing
import random
import matplotlib.pyplot as plt
import scipy.stats as stats
import statistics as st

def plot_distribution(values: list, number_of_nodes: int , title: str):
    """
    Plot the distribution of data extents across nodes.

    :param values: The values to plot.
    :param number_of_nodes: The number of nodes.
    :param title: The title of the plot.
    """
    plt.bar(range(1, number_of_nodes + 1), values, color='grey')
    plt.title(title)
    plt.xlabel('Node Number')
    plt.ylabel('Number of Extents')
    plt.show()

def print_distribution_statistics(title: str, values: list):
    """
    Print statistics about the distribution of data extents across nodes.

    :param title: The title of the distribution.
    :param values: The values to analyze.
    """
    print("Number of nodes:", len(values))
    print(title + ":", list(values))
    print("Sum of " + title + ":", sum(values))
    print("Median of " + title + ":", st.median(values))
    print("Mean of " + title + ":", st.mean(values))
    print("Standard deviation of " + title + ":", st.stdev(values))
    print("Min and max of " + title + ":", min(values), max(values))
    print("Chi-squared test of " + title + ":", chi_squared_test(values))
  
def ideal_distribution(values: list):
    """
    Calculate the ideal distribution of values.

    :param values: The values to analyze.
    :return: The ideal distribution of values.
    """

    ideal_value = sum(values) / len(values)
    return [ideal_value] * len(values)

def chi_squared_test(values: list):
    """
    Perform a chi-squared test of the observed and expected distributions.

    :param observed: The observed distribution.
    :param expected: The expected distribution.
    :return: The chi-squared test p-value.
    """
    observed = list(values)
    expected = ideal_distribution(values)

    _, p_value = stats.chisquare(observed, expected)

    return p_value

# Parameters
m = 8  # size of the key space
num_extents = 10000
workload = 1000000
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
        chord_ring.add_node(random.randint(0, chord_ring.max_nodes))

    # Get updated distribution
    updated_distribution = chord_ring.get_load_distribution()
    # Simulate workload
    write_distribution = chord_ring.simulate_workload(workload)
    print_distribution_statistics('Write Distribution', write_distribution.values())

    # Print updated distribution statistics
    print_distribution_statistics('Updated Distribution', updated_distribution.values())

    # Plot updated distribution
    plot_distribution(updated_distribution.values(), current_number_of_nodes, 'Updated Distribution')

# Lookup data
key_to_lookup = 'extent500'
data = chord_ring.lookup_data(key_to_lookup)
print(f"Data for {key_to_lookup}: {data}")
