  # imports
from src.chord import ChordRing
import random
import matplotlib.pyplot as plt
import scipy.stats as stats
import statistics as st
import numpy as np

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
    print("Mean of " + title + ":", round(st.mean(values),2))
    print("Standard deviation of " + title + ":", round(st.stdev(values),2))
    print("Min and max of " + title + ":", min(values), max(values))
    print("Chi-squared test of " + title + ": p-value = ", chi_squared_test(values))
    print("Kolmogorov-Smirnov test of " + title + ": p-value = ", test_uniformity(values, min(values), max(values)))
  
def chi_squared_test(data: list):
    """
    Perform a chi-squared test of the observed and expected distributions.

    :param values: The observed distribution.
    :return: The chi-squared test p-value.
    """
    values = list(data)
    # Calculate the number of bins based on the unique values
    num_bins = len(np.unique(values))
    
    # Calculate the observed frequencies
    observed = np.histogram(values, bins=num_bins)[0]
    
    # Calculate the expected frequencies
    total_observations = len(values)
    expected = [total_observations / num_bins] * num_bins
    
    # Perform chi-squared test
    _, p_value = stats.chisquare(observed, expected)

    return round(p_value, 4)

def test_uniformity(values: list, min_value: float, max_value: float):
    """
    Perform a Kolmogorov-Smirnov test to check if the distribution of values is uniform.

    :param values: The observed distribution.
    :param min_value: The minimum value of the uniform distribution.
    :param max_value: The maximum value of the uniform distribution.
    :return: The p-value of the Kolmogorov-Smirnov test.
    """
    # Calculate the expected CDF for the uniform distribution
    expected_cdf = stats.uniform(loc=min_value, scale=max_value-min_value).cdf
    
    # Perform the Kolmogorov-Smirnov test
    _, p_value = stats.kstest(list(values), expected_cdf)

    return round(p_value,4)

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
        chord_ring.add_node(random.randint(0, chord_ring.max_nodes))

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
data = chord_ring.lookup_data(key_to_lookup)
print(f"Data for {key_to_lookup}: {data}")
