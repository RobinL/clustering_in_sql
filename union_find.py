import random
import string
import time

import duckdb
import networkx as nx  # For validating answer
import pandas as pd

from generate_random_graphs import generate_chain_graph, generate_graph

random.seed(42)  # Set a fixed seed for reproducibility

# This algorith is called Breadth First Search
# in the paper https://arxiv.org/pdf/1802.09478.pdf


def ascii_uid(length):
    """Generate a random ASCII string of specified length."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


nodes, edges_without_self_loops = generate_chain_graph(10000)

# Register the DataFrames with DuckDB
duckdb.register("nodes", nodes)
duckdb.register("edges_without_self_loops", edges_without_self_loops)

start_time = time.time()

# Create the edges table, adding self-loops
sql = """
CREATE OR REPLACE TABLE edges AS
SELECT unique_id_l, unique_id_r
FROM edges_without_self_loops
WHERE unique_id_l <> unique_id_r

UNION

SELECT unique_id, unique_id AS unique_id_r
FROM nodes
"""
duckdb.execute(sql)

# Build the neighbours table
# Since the edges are undirected, we need to ensure both directions
create_neighbours_query = """
CREATE OR REPLACE TABLE neighbours AS
SELECT unique_id_l AS node_id, unique_id_r AS neighbour
FROM edges
UNION ALL
SELECT unique_id_r AS node_id, unique_id_l AS neighbour
FROM edges
"""
duckdb.execute(create_neighbours_query)

# Initialize the representatives: For each node, representative is the minimum neighbor
initial_representatives_query = """
CREATE OR REPLACE TABLE representatives AS
SELECT node_id, MIN(neighbour) AS representative
FROM neighbours
GROUP BY node_id
"""
duckdb.execute(initial_representatives_query)

iteration = 0
changes = 1  # To enter the loop

while changes > 0:
    iteration += 1

    # Update representatives by taking min of representatives of neighbors
    # Join neighbours with current representatives
    update_query = """
    CREATE OR REPLACE TABLE updated_representatives AS
    SELECT
        n.node_id,
        MIN(r2.representative) AS representative
    FROM neighbours AS n
    LEFT JOIN representatives AS r2
    ON n.neighbour = r2.node_id
    GROUP BY n.node_id
    """
    duckdb.execute(update_query)

    # Compare the updated representatives with the current ones
    # To check for changes, we can count the number of nodes where the representative changed
    changes_query = """
    SELECT COUNT(*) AS changes
    FROM (
        SELECT
            r.node_id,
            r.representative AS old_representative,
            u.representative AS new_representative
        FROM representatives AS r
        JOIN updated_representatives AS u
        ON r.node_id = u.node_id
        WHERE r.representative <> u.representative
    ) AS diff
    """
    changes_result = duckdb.execute(changes_query).fetchone()
    changes = changes_result[0]
    print(
        f"Iteration {iteration}: Number of nodes with changed representative: {changes}"
    )

    # Replace the old representatives with the updated ones
    # Drop the old representatives table
    duckdb.execute("DROP TABLE representatives")
    # Rename updated_representatives to representatives
    duckdb.execute("ALTER TABLE updated_representatives RENAME TO representatives")

# Final clustering results
final_query = """
SELECT node_id AS unique_id, representative AS cluster_id
FROM representatives
ORDER BY cluster_id, unique_id
"""
our_clusters = duckdb.execute(final_query).fetchdf()
print(our_clusters)

end_time = time.time()
execution_time = end_time - start_time
print(f"Core graph solving algorithm execution time: {execution_time:.2f} seconds")


# Validate the clusters using NetworkX
# Build the graph using NetworkX
edges_df = duckdb.execute("SELECT unique_id_l, unique_id_r FROM edges").fetchdf()
G = nx.Graph()
G.add_edges_from(edges_df.values)

# Get the connected components from NetworkX
nx_components = list(nx.connected_components(G))
# Assign cluster ids
nx_cluster_df = pd.DataFrame(
    [(node, idx) for idx, component in enumerate(nx_components) for node in component],
    columns=["unique_id", "nx_cluster_id"],
)

# Merge our clusters with NetworkX clusters
merged_clusters = our_clusters.merge(nx_cluster_df, on="unique_id")

# Check if the clusterings match
# Since the cluster IDs may not match, we check that for each of our clusters, the set of nodes matches the corresponding NetworkX cluster
grouped_our_clusters = merged_clusters.groupby("cluster_id")["nx_cluster_id"].nunique()

if grouped_our_clusters.eq(1).all():
    print("Clustering matches with NetworkX connected components.")
else:
    print("Clustering does not match with NetworkX connected components.")
