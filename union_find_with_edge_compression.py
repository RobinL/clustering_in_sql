import random
import string

import duckdb
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


num_rows = 200
num_edges = 100
nodes = generate_random_nodes(num_rows)
edges_without_self_loops = generate_random_edges(num_rows, num_edges)


# Create a DuckDB connection
conn = duckdb.connect()

# Register the DataFrames with DuckDB
conn.register("nodes", nodes)
conn.register("edges_without_self_loops", edges_without_self_loops)

sql = """
create table edges as
select unique_id_l, unique_id_r
from edges_without_self_loops
union
select unique_id as unique_id_l, unique_id as unique_id_r
from nodes
"""
conn.execute(sql)

# Create a temporary table for valid edges (ensure bidirectional edges)
create_valid_edges_query = """
CREATE OR REPLACE TEMP TABLE valid_edges AS
SELECT unique_id_l, unique_id_r
FROM edges
UNION
SELECT unique_id_r AS unique_id_l, unique_id_l AS unique_id_r
FROM edges;
"""
conn.execute(create_valid_edges_query)

# Initialize the first UnionFind table with each node as its own parent
initial_unionfind_table = f"UnionFind_{ascii_uid(8)}"
init_query = f"""
CREATE TABLE {initial_unionfind_table} AS
SELECT unique_id AS node, unique_id AS parent
FROM nodes;
"""
conn.execute(init_query)

# Keep track of the current UnionFind table
current_unionfind = initial_unionfind_table

iteration = 0
changes = 1

while changes > 0:
    iteration += 1
    print(f"\n--- Iteration {iteration} ---")

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
    conn.execute(union_step_query)

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
    conn.execute(path_compression_query)

    # Compare the new table with the current to count changes
    # Fetch the current and new parents
    current_df = conn.execute(f"SELECT node, parent FROM {current_unionfind}").fetchdf()
    new_df = conn.execute(
        f"SELECT node, parent FROM {path_compression_table}"
    ).fetchdf()

    # Merge on 'node' to compare parents
    merged_df = current_df.merge(new_df, on="node", suffixes=("_old", "_new"))

    # Count how many parents have changed
    changes = (merged_df["parent_old"] != merged_df["parent_new"]).sum()
    print(f"Changed rows count: {changes}")

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
our_clusters = conn.execute(final_query).fetchdf()
print(our_clusters)
