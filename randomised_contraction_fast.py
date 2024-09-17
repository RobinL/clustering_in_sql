import random
import string
import time

import duckdb
import networkx as nx  # For validating answer
import pandas as pd

import generate_random_graphs as gen

random.seed(42)  # Set a fixed seed for reproducibility


def ascii_uid(length):
    """Generate a random ASCII string of specified length."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


nodes, edges_without_self_loops = gen.generate_chain_graph(10000)

# Register th
# Register the DataFrames with DuckDB
duckdb.register("nodes", nodes)
duckdb.register("edges_without_self_loops", edges_without_self_loops)

start_time = time.time()

# Create the edges table E
sql = """
CREATE OR REPLACE TABLE E AS
SELECT unique_id_l AS v, unique_id_r AS w
FROM edges_without_self_loops
WHERE unique_id_l <> unique_id_r
UNION ALL
SELECT unique_id_r AS v, unique_id_l AS w
FROM edges_without_self_loops
WHERE unique_id_l <> unique_id_r
"""
duckdb.execute(sql)

# Delete the function if it exists, then recreate it

try:
    sql = """
    CREATE OR REPLACE MACRO axb(a, x, b) AS (
        cast((cast(a as ubigint) * cast(x as ubigint) + cast(b as ubigint)) % 2**32 as ubigint)
    );
    """
    duckdb.execute(sql)
except Exception as e:
    print(f"Error creating function: {e}")
    pass

# Initialize variables
S = []
i = 0
rowcount = 1

while rowcount > 0:
    i += 1
    A = random.randint(1, 2**31 - 1)  # Ensuring A != 0
    B = random.randint(0, 2**32 - 1)
    S.append((A, B))

    # Compute representatives
    create_representatives_query = f"""
    CREATE OR REPLACE TABLE R{i} AS
    SELECT v, LEAST(axb({A}::bigint, v, {B}::bigint), MIN(axb({A}::bigint, w, {B}::bigint))) AS r
    FROM E
    GROUP BY v
    """
    duckdb.execute(create_representatives_query)

    # Contract by transforming edge table
    contract_query = f"""
    CREATE OR REPLACE TABLE T AS
    SELECT DISTINCT V.r AS v, W.r AS w
    FROM E, R{i} AS V, R{i} AS W
    WHERE E.v = V.v AND E.w = W.v AND V.r != W.r
    """
    duckdb.execute(contract_query)

    # Get rowcount
    rowcount = duckdb.execute("SELECT COUNT(*) FROM T").fetchone()[0]

    # Print progress
    print(f"Iteration {i}: Number of edges remaining: {rowcount}")

    # Update E
    duckdb.execute("DROP TABLE E")
    duckdb.execute("ALTER TABLE T RENAME TO E")

# Compose representative functions
A, B = 1, 0
while i > 1:
    i -= 1
    alpha, beta = S.pop()
    A, B = duckdb.execute(
        f"SELECT axb({A}::bigint, {alpha}::bigint, 0), axb({A}::bigint, {beta}::bigint, {B}::bigint)"
    ).fetchone()

    compose_query = f"""
    CREATE OR REPLACE TABLE T AS
    SELECT L.v, COALESCE(R.r, axb({A}::bigint, L.r, {B}::bigint)) AS r
    FROM R{i} AS L
    LEFT OUTER JOIN R{i+1} AS R ON (L.r = R.v)
    """

    duckdb.execute(compose_query)

    duckdb.execute(f"DROP TABLE R{i}")
    duckdb.execute(f"DROP TABLE R{i+1}")
    duckdb.execute(f"ALTER TABLE T RENAME TO R{i}")


# Final clustering results
final_query = f"""
SELECT v AS unique_id, r AS cluster_id
FROM R{i}
ORDER BY cluster_id, unique_id
"""
our_clusters = duckdb.execute(final_query).fetchdf()
print(our_clusters)

end_time = time.time()
execution_time = end_time - start_time
print(f"Core graph solving algorithm execution time: {execution_time:.2f} seconds")


# Validate the clusters using NetworkX
# Build the graph using NetworkX
edges_df = duckdb.execute(
    "SELECT unique_id_l, unique_id_r FROM edges_without_self_loops"
).fetchdf()
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
