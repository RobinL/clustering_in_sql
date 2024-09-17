import logging
import random
import string
import time

import duckdb
import networkx as nx  # For validating answer
import pandas as pd
from splink import DuckDBAPI, Linker, SettingsCreator

import generate_random_graphs as gen

# random.seed(42)  # Set a fixed seed for reproducibility
ddb_con = duckdb.connect()
# This algorith is called Breadth First Search
# in the paper https://arxiv.org/pdf/1802.09478.pdf


# nodes, edges_without_self_loops = gen.generate_uniform_probability_graph(
#     int(2e6), int(8e6), 42
# )
#
nodes, edges_without_self_loops = gen.generate_uniform_probability_graph(
    int(2e5), int(4e5), random.randint(0, 1000000)
)

# Register the DataFrames with DuckDB
ddb_con.register("nodes", nodes)
ddb_con.register("edges_without_self_loops", edges_without_self_loops)

start_time = time.time()

# Create the edges table, adding self-loops
sql = """
CREATE OR REPLACE TABLE edges AS
SELECT unique_id_l, unique_id_r
FROM edges_without_self_loops
WHERE unique_id_l <> unique_id_r
and match_probability >= 0.5
UNION

SELECT unique_id as unique_id_l, unique_id AS unique_id_r
FROM nodes
"""
ddb_con.execute(sql)

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
ddb_con.execute(create_neighbours_query)

# Initialize the representatives and active nodes
initial_query = """
CREATE OR REPLACE TABLE representatives AS
SELECT node_id, MIN(neighbour) AS representative, TRUE AS active
FROM neighbours
GROUP BY node_id
"""
ddb_con.execute(initial_query)

iteration = 0
changes = 1  # To enter the loop

while changes > 0:
    iteration_start_time = time.time()
    iteration += 1

    update_query = """
    CREATE OR REPLACE TABLE updated_representatives AS
    SELECT
        n.node_id,
        MIN(r2.representative) AS representative,
        CASE
            WHEN MIN(r2.representative) <> MIN(r1.representative) THEN TRUE
            ELSE FALSE
        END AS active
    FROM neighbours AS n
    JOIN representatives AS r1 ON n.node_id = r1.node_id
    JOIN representatives AS r2 ON n.neighbour = r2.node_id
    WHERE r1.active = TRUE OR r2.active = TRUE
    GROUP BY n.node_id

    UNION ALL

    SELECT r.node_id, r.representative, r.active
    FROM representatives AS r
    WHERE r.node_id NOT IN (
        SELECT n.node_id
        FROM neighbours AS n
        JOIN representatives AS r1 ON n.node_id = r1.node_id
        JOIN representatives AS r2 ON n.neighbour = r2.node_id
        WHERE r1.active = TRUE OR r2.active = TRUE
        GROUP BY n.node_id
    )
    """
    ddb_con.execute(update_query)

    changes_query = """
    SELECT COUNT(*) AS changes
    FROM updated_representatives
    WHERE active = TRUE
    """
    changes_result = ddb_con.execute(changes_query).fetchone()
    changes = changes_result[0]

    ddb_con.execute("DROP TABLE representatives")
    ddb_con.execute("ALTER TABLE updated_representatives RENAME TO representatives")

    iteration_end_time = time.time()
    iteration_time = iteration_end_time - iteration_start_time
    print(
        f"Iteration {iteration}: Number of active nodes: {changes}, Time taken: {iteration_time:.2f} seconds"
    )

# Final clustering results
final_query = """
SELECT node_id AS unique_id, representative AS cluster_id
FROM representatives
ORDER BY cluster_id, unique_id
"""
our_clusters = ddb_con.execute(final_query).df()
# change one node id:
# increment the first cluster id by 1
# our_clusters.loc[0, "cluster_id"] += 1

print(our_clusters)

end_time = time.time()
execution_time = end_time - start_time
print(f"Core graph solving algorithm execution time: {execution_time:.2f} seconds")
our_clusters


# Validate the clusters using NetworkX
# Build the graph using NetworkX
edges_df = ddb_con.execute("SELECT unique_id_l, unique_id_r FROM edges").fetchdf()
G = nx.Graph()
G.add_edges_from(edges_df.values)

# Get the connected components from NetworkX
nx_components = list(nx.connected_components(G))
# Assign cluster ids
nx_cluster_df = pd.DataFrame(
    [(node, idx) for idx, component in enumerate(nx_components) for node in component],
    columns=["unique_id", "nx_cluster_id"],
)


cluster_stats_query = """
SELECT
    COUNT(DISTINCT cluster_id) AS num_clusters,
    AVG(cluster_size) AS avg_cluster_size,
    SUM(cluster_size) AS total_nodes
FROM (
    SELECT
        cluster_id,
        COUNT(*) AS cluster_size
    FROM our_clusters
    GROUP BY cluster_id
)
"""
cluster_stats = ddb_con.execute(cluster_stats_query).fetchdf()
print("Our Cluster Statistics:")
print(cluster_stats)


# Calculate cluster statistics for our method
our_cluster_stats_query = """
SELECT
    COUNT(DISTINCT nx_cluster_id) AS num_clusters,
    AVG(cluster_size) AS avg_cluster_size,
    SUM(cluster_size) AS total_nodes
FROM (
    SELECT
        nx_cluster_id,
        COUNT(*) AS cluster_size
    FROM nx_cluster_df
    GROUP BY nx_cluster_id
)
"""
our_cluster_stats = ddb_con.execute(our_cluster_stats_query).fetchdf()
print("NX Cluster Statistics:")
print(our_cluster_stats)

# Calculate cluster statistics for Splink's method

db_api = DuckDBAPI(ddb_con)
settings_creator = SettingsCreator(link_type="dedupe_only")
linker = Linker(nodes, settings_creator, db_api)

predict_splink_df = linker.table_management.register_table_predict(
    edges_without_self_loops
)
logging.getLogger("splink").setLevel(15)
clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    predict_splink_df, threshold_match_probability=0.5
)
splink_clusters = clusters.as_pandas_dataframe()


splink_cluster_stats_query = """
SELECT
    COUNT(DISTINCT cluster_id) AS num_clusters,
    AVG(cluster_size) AS avg_cluster_size,
    SUM(cluster_size) AS total_nodes
FROM (
    SELECT
        cluster_id,
        COUNT(*) AS cluster_size
    FROM splink_clusters
    GROUP BY cluster_id
)
"""
splink_cluster_stats = ddb_con.execute(splink_cluster_stats_query).fetchdf()
print("Splink Cluster Statistics:")
print(splink_cluster_stats)
