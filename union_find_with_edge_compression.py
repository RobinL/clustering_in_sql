import random
import string

import duckdb
import networkx as nx  # For validating answer
import pandas as pd

random.seed(42)  # Set a fixed seed for reproducibility


def ascii_uid(length):
    """Generate a random ASCII string of specified length."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_random_nodes(num_rows):
    data = []
    for i in range(num_rows):
        data.append({"unique_id": i})
    return pd.DataFrame(data)


def generate_random_edges(num_rows, num_edges):
    edges = []
    for _ in range(num_edges):
        unique_id_l = random.randint(0, num_rows - 1)
        unique_id_r = random.randint(0, num_rows - 1)
        if unique_id_l != unique_id_r:  # Exclude self-loops for initial edges
            edges.append({"unique_id_l": unique_id_l, "unique_id_r": unique_id_r})
    return pd.DataFrame(edges)


num_rows = 1000
num_edges = 2000
nodes = generate_random_nodes(num_rows)
edges_without_self_loops = generate_random_edges(num_rows, num_edges)

# Register the DataFrames with DuckDB
duckdb.register("nodes", nodes)
duckdb.register("edges_without_self_loops", edges_without_self_loops)

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

    # Path Compression Step: Update representative to the representative's representative
    path_compression_query = """
    CREATE OR REPLACE TABLE compressed_representatives AS
    SELECT
        u.node_id,
        CASE
            WHEN u.representative != u2.representative THEN u2.representative
            ELSE u.representative
        END AS representative
    FROM updated_representatives u
    LEFT JOIN updated_representatives u2 ON u.representative = u2.node_id
    """
    duckdb.execute(path_compression_query)

    # Compare the compressed representatives with the current ones
    # To check for changes, we can count the number of nodes where the representative changed
    changes_query = """
    SELECT COUNT(*) AS changes
    FROM (
        SELECT
            r.node_id,
            r.representative AS old_representative,
            c.representative AS new_representative
        FROM representatives AS r
        JOIN compressed_representatives AS c
        ON r.node_id = c.node_id
        WHERE r.representative <> c.representative
    ) AS diff
    """
    changes_result = duckdb.execute(changes_query).fetchone()
    changes = changes_result[0]
    print(
        f"Iteration {iteration}: Number of nodes with changed representative: {changes}"
    )

    # Replace the old representatives with the compressed ones
    # Drop the old representatives table
    duckdb.execute("DROP TABLE representatives")
    # Rename compressed_representatives to representatives
    duckdb.execute("ALTER TABLE compressed_representatives RENAME TO representatives")

# Final clustering results
final_query = """
SELECT node_id AS unique_id, representative AS cluster_id
FROM representatives
ORDER BY cluster_id, unique_id
"""
our_clusters = duckdb.execute(final_query).fetchdf()
print(our_clusters)

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
