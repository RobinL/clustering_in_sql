import random
import string
import time

import duckdb
import networkx as nx  # For validating answer
import pandas as pd

import generate_random_graphs as gen

random.seed(42)  # Set a fixed seed for reproducibility

# Define the thresholds
THRESHOLDS = list(reversed([i / 100 for i in range(0, 100, 5)]))


def ascii_uid(length):
    """Generate a random ASCII string of specified length."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


nodes, edges = gen.generate_uniform_probability_graph(100000, 400000, 42)

# Register the DataFrames with DuckDB
duckdb.register("nodes", nodes)
duckdb.register("edges", edges)

# Dictionary to store representatives at each threshold
representatives_dict = {}
total_start_time = time.time()
# Start with the lowest threshold
for idx, THRESHOLD_PROBABILITY in enumerate(THRESHOLDS):
    print(f"\nProcessing threshold: {THRESHOLD_PROBABILITY}")
    start_time = time.time()

    # Create the filtered edges table
    filtered_edges_table = f"filtered_edges_{idx}"
    sql = f"""
    CREATE OR REPLACE TABLE {filtered_edges_table} AS
    SELECT unique_id_l, unique_id_r
    FROM edges
    WHERE unique_id_l <> unique_id_r AND match_probability >= {THRESHOLD_PROBABILITY}

    UNION

    SELECT unique_id, unique_id AS unique_id_r
    FROM nodes
    """
    duckdb.execute(sql)

    # Build the neighbours table
    neighbours_table = f"neighbours_{idx}"
    create_neighbours_query = f"""
    CREATE OR REPLACE TABLE {neighbours_table} AS
    SELECT unique_id_l AS node_id, unique_id_r AS neighbour
    FROM {filtered_edges_table}
    UNION ALL
    SELECT unique_id_r AS node_id, unique_id_l AS neighbour
    FROM {filtered_edges_table}
    """
    duckdb.execute(create_neighbours_query)

    # Initialize the representatives
    representatives_table = f"representatives_{idx}"
    initial_representatives_query = f"""
    CREATE OR REPLACE TABLE {representatives_table} AS
    SELECT node_id, MIN(neighbour) AS representative
    FROM {neighbours_table}
    GROUP BY node_id
    """
    duckdb.execute(initial_representatives_query)

    iteration = 0
    changes = 1  # To enter the loop

    while changes > 0:
        iteration += 1

        # Update representatives by taking min of representatives of neighbors
        # Join neighbours with current representatives
        updated_representatives_table = f"updated_representatives_{idx}"
        update_query = f"""
        CREATE OR REPLACE TABLE {updated_representatives_table} AS
        SELECT
            n.node_id,
            MIN(r2.representative) AS representative
        FROM {neighbours_table} AS n
        LEFT JOIN {representatives_table} AS r2
        ON n.neighbour = r2.node_id
        GROUP BY n.node_id
        """
        duckdb.execute(update_query)

        # Compare the updated representatives with the current ones
        changes_query = f"""
        SELECT COUNT(*) AS changes
        FROM (
            SELECT
                r.node_id,
                r.representative AS old_representative,
                u.representative AS new_representative
            FROM {representatives_table} AS r
            JOIN {updated_representatives_table} AS u
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
        duckdb.execute(f"DROP TABLE {representatives_table}")
        duckdb.execute(
            f"ALTER TABLE {updated_representatives_table} RENAME TO {representatives_table}"
        )

    # Store the final representatives for this threshold
    final_representatives_query = f"""
    SELECT node_id AS unique_id, representative AS cluster_id
    FROM {representatives_table}
    ORDER BY unique_id
    """
    representatives_df = duckdb.execute(final_representatives_query).fetchdf()
    representatives_dict[THRESHOLD_PROBABILITY] = representatives_df
    print(
        f"Completed threshold {THRESHOLD_PROBABILITY} in {time.time() - start_time:.2f} seconds."
    )

# Combine the results into a single DataFrame
final_df = representatives_dict[THRESHOLDS[-1]][["unique_id"]]
for THRESHOLD_PROBABILITY in reversed(THRESHOLDS):
    col_name = f"cluster_id_at_{str(THRESHOLD_PROBABILITY).replace('.', '_')}"
    df = representatives_dict[THRESHOLD_PROBABILITY][["unique_id", "cluster_id"]]
    df.rename(columns={"cluster_id": col_name}, inplace=True)
    final_df = final_df.merge(df, on="unique_id")

# Reorder the columns as per the requirement
final_columns = ["unique_id"] + [
    f"cluster_id_at_{str(t).replace('.', '_')}" for t in reversed(THRESHOLDS)
]
final_df = final_df[final_columns]

total_end_time = time.time()
total_execution_time = total_end_time - total_start_time
print(f"Total execution time: {total_execution_time:.2f} seconds")

# Display the head of the final DataFrame
print("\nFinal Clustering Results:")
print(final_df.head().to_markdown(index=False))


# # Validate the clusters for each threshold
# for THRESHOLD_PROBABILITY in THRESHOLDS:
#     print(f"\nValidating clustering for threshold: {THRESHOLD_PROBABILITY}")

#     # Build the graph using NetworkX for the current threshold
#     edges_query = f"""
#     SELECT unique_id_l, unique_id_r
#     FROM edges
#     WHERE match_probability >= {THRESHOLD_PROBABILITY}
#     """
#     edges_df = duckdb.execute(edges_query).fetchdf()
#     G = nx.Graph()
#     G.add_edges_from(edges_df.values)

#     # Get the connected components from NetworkX
#     nx_components = list(nx.connected_components(G))
#     nx_cluster_df = pd.DataFrame(
#         [
#             (node, idx)
#             for idx, component in enumerate(nx_components)
#             for node in component
#         ],
#         columns=["unique_id", "nx_cluster_id"],
#     )

#     # Get our clustering results for the current threshold
#     col_name = f"cluster_id_at_{str(THRESHOLD_PROBABILITY).replace('.', '_')}"
#     our_clusters = final_df[["unique_id", col_name]].rename(
#         columns={col_name: "cluster_id"}
#     )

#     # Merge our clusters with NetworkX clusters
#     merged_clusters = our_clusters.merge(nx_cluster_df, on="unique_id")

#     # Check if the clusterings match
#     grouped_our_clusters = merged_clusters.groupby("cluster_id")[
#         "nx_cluster_id"
#     ].nunique()

#     if grouped_our_clusters.eq(1).all():
#         print(
#             f"Clustering matches with NetworkX connected components for threshold {THRESHOLD_PROBABILITY}."
#         )
#     else:
#         print(
#             f"Clustering does not match with NetworkX connected components for threshold {THRESHOLD_PROBABILITY}."
#         )
#         print("Mismatched clusters:")
#         mismatched = grouped_our_clusters[grouped_our_clusters != 1]
#         print(mismatched)
