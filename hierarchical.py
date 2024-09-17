import random
import time

import duckdb
import networkx as nx
import pandas as pd

import generate_random_graphs as gen


def validate_with_networkx(nodes, edges_without_self_loops, probability_threshold):
    G = nx.Graph()
    G.add_nodes_from(nodes["unique_id"])

    for _, edge in edges_without_self_loops.iterrows():
        if edge["match_probability"] >= probability_threshold:
            G.add_edge(edge["unique_id_l"], edge["unique_id_r"])

    connected_components = list(nx.connected_components(G))

    nx_clusters = pd.DataFrame(
        [
            {"unique_id": node, "cluster_id": i}
            for i, component in enumerate(connected_components)
            for node in component
        ]
    )
    cluster_stats_query = """
    SELECT
        COUNT(DISTINCT cluster_id) AS num_clusters,
        AVG(cluster_size) AS avg_cluster_size
    FROM (
        SELECT
            cluster_id,
            COUNT(*) AS cluster_size
        FROM nx_clusters
        GROUP BY cluster_id
    )
    """

    duckdb.register("nx_clusters", nx_clusters)

    print("NetworkX Clustering Results:")

    print(duckdb.sql(cluster_stats_query))


def perform_clustering(nodes, edges_without_self_loops, probability_threshold=0.5):
    # Register the DataFrames with DuckDB
    duckdb.register("nodes", nodes)
    duckdb.register("edges_without_self_loops", edges_without_self_loops)

    # Create the edges table, adding self-loops
    duckdb.execute(f"""
    CREATE OR REPLACE TABLE edges AS
    SELECT unique_id_l, unique_id_r
    FROM edges_without_self_loops
    WHERE unique_id_l <> unique_id_r
    AND match_probability >= {probability_threshold}
    UNION
    SELECT unique_id, unique_id AS unique_id_r
    FROM nodes
    """)

    # Build the neighbours table
    duckdb.execute("""
    CREATE OR REPLACE TABLE neighbours AS
    SELECT unique_id_l AS node_id, unique_id_r AS neighbour
    FROM edges
    UNION ALL
    SELECT unique_id_r AS node_id, unique_id_l AS neighbour
    FROM edges
    """)

    # Initialize the representatives
    duckdb.execute("""
    CREATE OR REPLACE TABLE representatives AS
    SELECT node_id, MIN(neighbour) AS representative
    FROM neighbours
    GROUP BY node_id
    """)

    iteration = 0
    changes = 1

    while changes > 0:
        iteration += 1

        duckdb.execute("""
        CREATE OR REPLACE TABLE updated_representatives AS
        SELECT
            n.node_id,
            MIN(r2.representative) AS representative
        FROM neighbours AS n
        LEFT JOIN representatives AS r2
        ON n.neighbour = r2.node_id
        GROUP BY n.node_id
        """)

        changes = duckdb.execute("""
        SELECT COUNT(*) AS changes
        FROM (
            SELECT r.node_id
            FROM representatives AS r
            JOIN updated_representatives AS u
            ON r.node_id = u.node_id
            WHERE r.representative <> u.representative
        ) AS diff
        """).fetchone()[0]

        print(
            f"Iteration {iteration}: Number of nodes with changed representative: {changes}"
        )

        duckdb.execute("DROP TABLE representatives")
        duckdb.execute("ALTER TABLE updated_representatives RENAME TO representatives")

    return duckdb.sql("""

    SELECT node_id AS unique_id, representative AS cluster_id
    FROM representatives
    ORDER BY cluster_id, unique_id
    """)


# Main execution


random.seed(42)
nodes, edges_without_self_loops = gen.generate_uniform_probability_graph(1000, 1000, 42)


initial_clusters = perform_clustering(nodes, edges_without_self_loops, 0.5)

cluster_stats_query = """
    SELECT
        COUNT(DISTINCT cluster_id) AS num_clusters,
        AVG(cluster_size) AS avg_cluster_size
    FROM (
        SELECT
            cluster_id,
            COUNT(*) AS cluster_size
        FROM initial_clusters
        GROUP BY cluster_id
    )
    """
print(duckdb.sql(cluster_stats_query))


print("initial_clusters")
print(duckdb.sql("select * from initial_clusters"))
print(duckdb.sql("select count(distinct cluster_id) from initial_clusters"))

NEW_THRESHOLD = 0.55


sql = f"""
create or replace table stable_clusters as
with edges as (
select * from edges_without_self_loops
union all
select unique_id as unique_id_l, unique_id as unique_id_r, 1.0 as match_probability
from nodes
),
edges_with_clusters AS (
    SELECT e.*,
           cl.cluster_id AS cluster_id_l,
           cr.cluster_id AS cluster_id_r
    FROM edges e
    LEFT JOIN initial_clusters cl ON e.unique_id_l = cl.unique_id
    LEFT JOIN initial_clusters cr ON e.unique_id_r = cr.unique_id
)
SELECT
   cluster_id_l as cluster_id, min(match_probability) as min_match_probability
FROM edges_with_clusters
WHERE cluster_id_l = cluster_id_r
GROUP BY cluster_id_l
HAVING MIN(match_probability) > {NEW_THRESHOLD}
ORDER BY cluster_id_l
"""

duckdb.execute(sql)
print("stable_clusters")
print(duckdb.sql("select * from stable_clusters"))

# Now we want a table called stable_nodes which has all the nodes that are in stable_clusters
sql = """
create or replace table stable_nodes as
SELECT *
FROM initial_clusters ic
where ic.cluster_id in (
    select cluster_id from stable_clusters
    )
"""
duckdb.execute(sql)
print("stable_nodes")
print(duckdb.sql("select * from stable_nodes"))

sql = """
create or replace table edges_in_play as
with edges as (
select * from edges_without_self_loops
union all
select unique_id as unique_id_l, unique_id as unique_id_r, 1.0 as match_probability
from nodes
)
select * from edges
where unique_id_l not in (select unique_id from stable_nodes)
and unique_id_r not in (select unique_id from stable_nodes)
"""
print(duckdb.sql(sql))

# nodes still in play
sql = """
create or replace table nodes_in_play as
select * from nodes
where unique_id not in (select unique_id from stable_nodes)
"""
print(duckdb.sql(sql))

# Finally recluster the nodes in play
nodes_in_play = duckdb.sql("select * from nodes_in_play").df()
edges_in_play = duckdb.sql("select * from edges_in_play").df()

new_clusters = perform_clustering(nodes_in_play, edges_in_play, NEW_THRESHOLD)

# Finally append the new clusters to the stable clusters
final_result = duckdb.sql("""
create or replace table final_result as
with final_result as (
SELECT * FROM stable_nodes
UNION ALL
SELECT * FROM new_clusters
           )
           select * from final_result
           order by unique_id, cluster_id
""")

cluster_stats_query = """
    SELECT
        COUNT(DISTINCT cluster_id) AS num_clusters,
        AVG(cluster_size) AS avg_cluster_size
    FROM (
        SELECT
            cluster_id,
            COUNT(*) AS cluster_size
        FROM final_result
        GROUP BY cluster_id
    )
    """
print(duckdb.sql(cluster_stats_query))
# Validate the clusters using NetworkX
validate_with_networkx(nodes, edges_without_self_loops, NEW_THRESHOLD)
