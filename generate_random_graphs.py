import random

import networkx as nx
import pandas as pd


def generate_graph(graph_size=1000, master_seed=42):
    random.seed(master_seed)

    def generate_random_graph(graph_size, seed=None):
        if not seed:
            seed = random.randint(5, 1000000)
        return nx.fast_gnp_random_graph(graph_size, 0.001, seed=seed, directed=False)

    def generate_random_nodes(num_rows):
        return pd.DataFrame({"unique_id": range(num_rows)})

    def generate_random_edges(G):
        edges = [{"unique_id_l": u, "unique_id_r": v} for u, v in G.edges()]
        return pd.DataFrame(edges)

    G = generate_random_graph(graph_size)
    nodes = generate_random_nodes(G.number_of_nodes())
    edges = generate_random_edges(G)

    return nodes, edges


def generate_chain_graph(graph_size=1000, master_seed=42):
    random.seed(master_seed)

    def generate_chain_networkx_graph(graph_size):
        return nx.path_graph(graph_size)

    def generate_random_nodes(num_rows):
        return pd.DataFrame({"unique_id": range(num_rows)})

    def generate_chain_edges(G):
        edges = [{"unique_id_l": u, "unique_id_r": v} for u, v in G.edges()]
        return pd.DataFrame(edges)

    G = generate_chain_networkx_graph(graph_size)
    nodes = generate_random_nodes(G.number_of_nodes())
    edges = generate_chain_edges(G)

    return nodes, edges


def generate_uniform_probability_graph(graph_size=1000, num_edges=2000, master_seed=42):
    random.seed(master_seed)

    def generate_random_nodes(num_rows):
        return pd.DataFrame({"unique_id": range(num_rows)})

    def generate_random_edges(num_rows, num_edges):
        edges = []
        for _ in range(num_edges):
            unique_id_l = random.randint(0, num_rows - 1)
            unique_id_r = random.randint(0, num_rows - 1)
            if unique_id_l != unique_id_r:
                match_probability = random.uniform(0, 1)
                edges.append(
                    {
                        "unique_id_l": unique_id_l,
                        "unique_id_r": unique_id_r,
                        "match_probability": match_probability,
                    }
                )
        return pd.DataFrame(edges)

    nodes = generate_random_nodes(graph_size)
    edges = generate_random_edges(graph_size, num_edges)

    return nodes, edges
