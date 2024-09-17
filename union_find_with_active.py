import random
import string
import time

import duckdb
import networkx as nx  # For validating answer
import pandas as pd

import generate_random_graphs as gen

# random.seed(42)  # Set a fixed seed for reproducibility
ddb_con = duckdb.connect()
# This algorith is called Breadth First Search
# in the paper https://arxiv.org/pdf/1802.09478.pdf


def ascii_uid(length):
    """Generate a random ASCII string of specified length."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


# nodes, edges_without_self_loops = gen.generate_uniform_probability_graph(
#     int(2e6), int(8e6), 42
# )
#
nodes, edges_without_self_loops = gen.generate_uniform_probability_graph(
    int(1e5), int(4e5), random.randint(0, 1000000)
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
    iteration += 1

    update_query = """
    CREATE OR REPLACE TABLE updated_representatives AS
    SELECT
        n.node_id,
        MIN(r2.representative) AS representative,
        CASE WHEN MIN(r2.representative) = r1.representative THEN FALSE ELSE TRUE END AS active
    FROM neighbours AS n
    JOIN representatives AS r1 ON n.node_id = r1.node_id
    JOIN representatives AS r2 ON n.neighbour = r2.node_id
    WHERE r1.active = TRUE
    GROUP BY n.node_id, r1.representative

    UNION ALL

    SELECT node_id, representative, FALSE AS active
    FROM representatives
    WHERE active = FALSE
    """
    ddb_con.execute(update_query)

    changes_query = """
    SELECT COUNT(*) AS changes
    FROM updated_representatives
    WHERE active = TRUE
    """
    changes_result = ddb_con.execute(changes_query).fetchone()
    changes = changes_result[0]
    print(f"Iteration {iteration}: Number of active nodes: {changes}")

    ddb_con.execute("DROP TABLE representatives")
    ddb_con.execute("ALTER TABLE updated_representatives RENAME TO representatives")

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

# 	unique_id	cluster_id
# 0	0	0
# 1	482	0
# Calculate number of clusters and average cluster size from our_clusters
cluster_stats_query = """
SELECT
    COUNT(DISTINCT cluster_id) AS num_clusters,
    AVG(cluster_size) AS avg_cluster_size
FROM (
    SELECT
        cluster_id,
        COUNT(*) AS cluster_size
    FROM our_clusters
    GROUP BY cluster_id
)
"""
cluster_stats = ddb_con.execute(cluster_stats_query).fetchdf()
print("Cluster Statistics:")
print(cluster_stats)


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

# Merge our clusters with NetworkX clusters
merged_clusters = our_clusters.merge(nx_cluster_df, on="unique_id")

# Check if the clusterings match
# Since the cluster IDs may not match, we check that for each of our clusters, the set of nodes matches the corresponding NetworkX cluster
grouped_our_clusters = merged_clusters.groupby("cluster_id")["nx_cluster_id"].nunique()

if grouped_our_clusters.eq(1).all():
    print("Clustering matches with NetworkX connected components.")
else:
    print("ERROR: Clustering does not match with NetworkX connected components.")

# Check the answer against Splink's connected components
from splink import DuckDBAPI, Linker, SettingsCreator

db_api = DuckDBAPI(ddb_con)
settings_creator = SettingsCreator(link_type="dedupe_only")
linker = Linker(nodes, settings_creator, db_api)

predict_splink_df = linker.table_management.register_table_predict(
    edges_without_self_loops
)
clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    predict_splink_df, threshold_match_probability=0.5
)
splink_clusters = clusters.as_pandas_dataframe()

#
# Merge our clusters with Splink clusters
merged_clusters = our_clusters.merge(
    splink_clusters, on="unique_id", suffixes=("_our", "_splink")
)

# Check if the clusterings match
# Since the cluster IDs may not match, we check that for each of our clusters,
# all nodes belong to the same Splink cluster and vice versa
grouped_our_clusters = merged_clusters.groupby("cluster_id_our")[
    "cluster_id_splink"
].nunique()
grouped_splink_clusters = merged_clusters.groupby("cluster_id_splink")[
    "cluster_id_our"
].nunique()

if grouped_our_clusters.eq(1).all() and grouped_splink_clusters.eq(1).all():
    print("Our clustering matches Splink's clustering.")
else:
    print("ERROR: Our clustering does not match Splink's clustering.")

# Additional statistics
print(f"Number of clusters in our method: {our_clusters['cluster_id'].nunique()}")
print(
    f"Number of clusters in Splink's method: {splink_clusters['cluster_id'].nunique()}"
)
