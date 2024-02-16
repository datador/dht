import matplotlib.pyplot as plt
import scipy.stats as stats
import statistics as st
import numpy as np
import time

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

def lookup_and_measure_time(key_to_lookup, chord_ring):
    # Record the start time
    start_time = time.perf_counter()

    # Lookup data
    data = chord_ring.lookup_data(key_to_lookup)

    # Record the end time
    end_time = time.perf_counter()

    # Calculate the time taken
    time_taken = end_time - start_time

    print(f"Data for {key_to_lookup}: {data}")
    print(f"Time taken for retrieval: {time_taken:.6f} seconds")