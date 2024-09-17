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
