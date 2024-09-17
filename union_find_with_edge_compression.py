import random
import string

import duckdb
import networkx as nx  # For validating answer
import pandas as pd


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
        edges.append({"unique_id_l": unique_id_l, "unique_id_r": unique_id_r})
    return pd.DataFrame(edges)


num_rows = 1000
num_edges = 2000
nodes = generate_random_nodes(num_rows)
edges_without_self_loops = generate_random_edges(num_rows, num_edges)


# Register the DataFrames with DuckDB
duckdb.register("nodes", nodes)
duckdb.register("edges_without_self_loops", edges_without_self_loops)

sql = """
CREATE OR REPLACE TABLE edges AS
SELECT unique_id_l, unique_id_r
FROM edges_without_self_loops
UNION
SELECT unique_id AS unique_id_l, unique_id AS unique_id_r
FROM nodes
"""
duckdb.execute(sql)

# Create a temporary table for valid edges (ensure bidirectional edges)
create_valid_edges_query = """
CREATE OR REPLACE TEMP TABLE valid_edges AS
SELECT unique_id_l, unique_id_r
FROM edges
UNION
SELECT unique_id_r AS unique_id_l, unique_id_l AS unique_id_r
FROM edges;
"""
duckdb.execute(create_valid_edges_query)

# Initialize the first UnionFind table with each node as its own parent
initial_unionfind_table = f"UnionFind_{ascii_uid(8)}"
init_query = f"""
CREATE TABLE {initial_unionfind_table} AS
SELECT unique_id AS node, unique_id AS parent
FROM nodes;
"""
duckdb.execute(init_query)

# Keep track of the current UnionFind table
current_unionfind = initial_unionfind_table

iteration = 0
changes = 1

while changes > 0:
    iteration += 1

    # Generate a unique name for the new UnionFind table
    new_unionfind_table = f"UnionFind_{ascii_uid(8)}"

    # Union Step: Find the minimum parent for each node based on neighbors
    union_step_query = f"""
    CREATE TABLE {new_unionfind_table} AS
    SELECT
        uf.node,
        MIN(uf2.parent) AS parent
    FROM {current_unionfind} uf
    JOIN valid_edges e ON uf.node = e.unique_id_r
    JOIN {current_unionfind} uf2 ON e.unique_id_l = uf2.node
    GROUP BY uf.node;
    """
    duckdb.execute(union_step_query)

    # Path Compression Step: Update parent to the parent's parent
    path_compression_table = f"UnionFind_{ascii_uid(8)}"
    path_compression_query = f"""
    CREATE TABLE {path_compression_table} AS
    SELECT
        u.node,
        CASE
            WHEN u.parent != u2.parent THEN u2.parent
            ELSE u.parent
        END AS parent
    FROM {new_unionfind_table} u
    LEFT JOIN {new_unionfind_table} u2 ON u.parent = u2.node;
    """
    duckdb.execute(path_compression_query)

    # Compare the new table with the current to count changes
    # Fetch the current and new parents
    current_df = duckdb.execute(
        f"SELECT node, parent FROM {current_unionfind}"
    ).fetchdf()
    new_df = duckdb.execute(
        f"SELECT node, parent FROM {path_compression_table}"
    ).fetchdf()

    # Merge on 'node' to compare parents
    merged_df = current_df.merge(new_df, on="node", suffixes=("_old", "_new"))

    # Count how many parents have changed
    changes = (merged_df["parent_old"] != merged_df["parent_new"]).sum()
    print(f"Iteration {iteration}: Changed rows count: {changes}")

    if changes == 0:
        # No changes; the algorithm has converged
        final_unionfind_table = path_compression_table
        print(f"No changes detected. Final UnionFind table: {final_unionfind_table}")
        break
    else:
        # Update the current_unionfind to the new table for the next iteration
        current_unionfind = path_compression_table


# Final clustering results
final_query = f"""
SELECT node AS unique_id, parent AS cluster_id
FROM {final_unionfind_table}
ORDER BY cluster_id, node;
"""
our_clusters = duckdb.execute(final_query).fetchdf()
print(our_clusters)


final_query = f"""
SELECT node AS unique_id, parent AS cluster_id
FROM {final_unionfind_table}
ORDER BY cluster_id, node;
"""
our_clusters = duckdb.execute(final_query).fetchdf()


G = nx.Graph()
edges_df = duckdb.execute("SELECT unique_id_l, unique_id_r FROM edges").fetchdf()
G.add_edges_from(edges_df.values)

nx_clusters = list(nx.connected_components(G))

# Convert NetworkX clusters to a DataFrame
nx_cluster_df = pd.DataFrame(
    [(node, i) for i, cluster in enumerate(nx_clusters) for node in cluster],
    columns=["unique_id", "nx_cluster_id"],
)

# Merge our clusters with NetworkX clusters
merged_clusters = our_clusters.merge(nx_cluster_df, on="unique_id")

# Check if the clusterings match
assert (
    merged_clusters.groupby("cluster_id")["nx_cluster_id"].nunique().eq(1).all()
), "Clustering mismatch detected"
