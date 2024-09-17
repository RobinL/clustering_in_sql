# THIS CURRENT EXECUTES BUT DOESN'T REALLY ACHIEVE THE
# AIM OF USING THE EXISTING CLUSTERS TO MAKE IT FASTER

import random
import string
import time

import duckdb
import pandas as pd

import generate_random_graphs as gen

random.seed(42)  # Set a fixed seed for reproducibility

# Define the thresholds
THRESHOLDS = list([i / 100 for i in range(0, 100, 5)])


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

    if idx == 0:
        # For the first threshold, no cluster constraint
        # Create the filtered edges table, adding self-loops and filtering by threshold probability
        filtered_edges_table = f"filtered_edges_{idx}"
        sql = f"""
        CREATE OR REPLACE TABLE {filtered_edges_table} AS
        SELECT
            e.unique_id_l,
            e.unique_id_r
        FROM edges AS e
        WHERE e.unique_id_l <> e.unique_id_r
          AND e.match_probability >= {THRESHOLD_PROBABILITY}

        UNION

        SELECT unique_id AS unique_id_l, unique_id AS unique_id_r
        FROM nodes
        """
        duckdb.execute(sql)

        # Build the neighbours table without cluster_id
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

        # Initialize the representatives: min of neighbor
        representatives_table = f"representatives_{idx}"
        initial_representatives_query = f"""
        CREATE OR REPLACE TABLE {representatives_table} AS
        SELECT node_id, MIN(neighbour) AS representative
        FROM {neighbours_table}
        GROUP BY node_id
        """
        duckdb.execute(initial_representatives_query)
    else:
        # For higher thresholds, constrain edges to be within the same cluster
        # The representatives from the previous threshold
        prev_representatives_table = f"representatives_{idx - 1}"

        # Create the filtered edges table, adding self-loops, and filtering by threshold probability
        filtered_edges_table = f"filtered_edges_{idx}"
        sql = f"""
        CREATE OR REPLACE TABLE {filtered_edges_table} AS
        SELECT
            e.unique_id_l,
            e.unique_id_r,
            r_l.cluster_id
        FROM edges AS e
        JOIN {prev_representatives_table} AS r_l ON e.unique_id_l = r_l.node_id
        JOIN {prev_representatives_table} AS r_r ON e.unique_id_r = r_r.node_id
        WHERE e.unique_id_l <> e.unique_id_r
          AND e.match_probability >= {THRESHOLD_PROBABILITY}
          AND r_l.cluster_id = r_r.cluster_id

        UNION

        SELECT n.unique_id AS unique_id_l, n.unique_id AS unique_id_r, r.cluster_id
        FROM nodes AS n
        JOIN {prev_representatives_table} AS r ON n.unique_id = r.node_id
        """
        duckdb.execute(sql)

        # Build the neighbours table with cluster_id
        neighbours_table = f"neighbours_{idx}"
        create_neighbours_query = f"""
        CREATE OR REPLACE TABLE {neighbours_table} AS
        SELECT unique_id_l AS node_id, unique_id_r AS neighbour, cluster_id
        FROM {filtered_edges_table}

        UNION ALL

        SELECT unique_id_r AS node_id, unique_id_l AS neighbour, cluster_id
        FROM {filtered_edges_table}
        """
        duckdb.execute(create_neighbours_query)

        # Initialize the representatives
        representatives_table = f"representatives_{idx}"
        initial_representatives_query = f"""
        CREATE OR REPLACE TABLE {representatives_table} AS
        SELECT node_id, MIN(neighbour) AS representative, cluster_id
        FROM {neighbours_table}
        GROUP BY node_id, cluster_id
        """
        duckdb.execute(initial_representatives_query)

    iteration = 0
    changes = 1  # To enter the loop

    while changes > 0:
        iteration += 1

        if idx == 0:
            # Update representatives by taking min of representatives of neighbors
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
        else:
            # Update representatives by taking min of representatives of neighbors within the same cluster
            updated_representatives_table = f"updated_representatives_{idx}"
            update_query = f"""
            CREATE OR REPLACE TABLE {updated_representatives_table} AS
            SELECT
                n.node_id,
                MIN(r2.representative) AS representative,
                n.cluster_id
            FROM {neighbours_table} AS n
            LEFT JOIN {representatives_table} AS r2
            ON n.neighbour = r2.node_id AND n.cluster_id = r2.cluster_id
            GROUP BY n.node_id, n.cluster_id
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
                ON r.node_id = u.node_id AND r.cluster_id = u.cluster_id
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

    # After iterations, ensure representatives_table has cluster_id
    if idx == 0:
        # Add cluster_id column by setting it equal to the representative
        duckdb.execute(f"""
        CREATE OR REPLACE TABLE {representatives_table} AS
        SELECT node_id, representative, representative AS cluster_id
        FROM {representatives_table}
        """)

    # Store the final representatives for this threshold
    final_representatives_query = f"""
    SELECT node_id AS unique_id, representative AS representative, cluster_id
    FROM {representatives_table}
    ORDER BY unique_id
    """
    representatives_df = duckdb.execute(final_representatives_query).fetchdf()
    representatives_dict[THRESHOLD_PROBABILITY] = representatives_df
    print(
        f"Completed threshold {THRESHOLD_PROBABILITY} in {time.time() - start_time:.2f} seconds."
    )

total_end_time = time.time()
total_execution_time = total_end_time - total_start_time
print(f"Total execution time: {total_execution_time:.2f} seconds")

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

# Display the head of the final DataFrame
print("\nFinal Clustering Results:")
print(final_df.head().to_markdown(index=False))