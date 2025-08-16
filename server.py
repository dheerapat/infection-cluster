import io
import polars as pl
import networkx as nx
from datetime import timedelta
from fastapi import FastAPI, UploadFile
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:4200",
    "http://127.0.0.1:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/cluster")
async def generate_cluster(transfer_file: UploadFile, micro_file: UploadFile):
    transfers = pl.read_csv(
        io.BytesIO(await transfer_file.read()), try_parse_dates=True
    )
    micro = pl.read_csv(io.BytesIO(await micro_file.read()), try_parse_dates=True)

    micro = micro.filter(pl.col("result") == "positive")
    patient_visits = micro.join(transfers, on="patient_id", how="inner")

    pv1 = patient_visits.select(
        [
            pl.col("patient_id").alias("patient_id_1"),
            pl.col("collection_date").alias("collection_date_1"),
            pl.col("infection").alias("infection_1"),
            pl.col("location").alias("location_1"),
            pl.col("ward_in_time").alias("ward_in_time_1"),
            pl.col("ward_out_time").alias("ward_out_time_1"),
        ]
    )

    pv2 = patient_visits.select(
        [
            pl.col("patient_id").alias("patient_id_2"),
            pl.col("collection_date").alias("collection_date_2"),
            pl.col("infection").alias("infection_2"),
            pl.col("location").alias("location_2"),
            pl.col("ward_in_time").alias("ward_in_time_2"),
            pl.col("ward_out_time").alias("ward_out_time_2"),
        ]
    )

    contact_pairs = pv1.join(pv2, how="cross").filter(
        (pl.col("patient_id_1") != pl.col("patient_id_2"))
        & (pl.col("patient_id_1") < pl.col("patient_id_2"))
        & (pl.col("infection_1") == pl.col("infection_2"))
        & (pl.col("location_1") == pl.col("location_2"))
        & (
            pl.max_horizontal("ward_in_time_1", "ward_in_time_2")
            <= pl.col("collection_date_1") + timedelta(days=14)
        )
        & (
            pl.max_horizontal("ward_in_time_1", "ward_in_time_2")
            <= pl.col("collection_date_2") + timedelta(days=14)
        )
        & (
            pl.min_horizontal("ward_out_time_1", "ward_out_time_2")
            >= pl.col("collection_date_1") - timedelta(days=14)
        )
        & (
            pl.min_horizontal("ward_out_time_1", "ward_out_time_2")
            >= pl.col("collection_date_2") - timedelta(days=14)
        )
    )

    def find_simple_clusters(df: pl.DataFrame):
        G = nx.Graph()
        for row in df.iter_rows():
            G.add_edge(
                row[0],
                row[6],
                organism=row[2],
                location=row[3],
            )
        clusters = list(nx.connected_components(G))
        return clusters, G

    clusters, G = find_simple_clusters(contact_pairs)

    def graph_to_json(G: nx.Graph, clusters):
        summary_lines = []
        for i, cluster in enumerate(clusters, start=1):
            if len(cluster) >= 2:
                cluster_nodes = list(cluster)
                summary_lines.append(
                    f"\nCluster {i}: {cluster_nodes} (size: {len(cluster_nodes)})"
                )

            # Get all edges in this cluster and their attributes
            cluster_edges = G.subgraph(cluster).edges(data=True)
            for u, v, attrs in cluster_edges:
                summary_lines.append(f"\tContact: {u} ↔ {v}")
                summary_lines.append(f"\t\tOrganism: {attrs['organism']}")
                summary_lines.append(f"\t\tLocation: {attrs['location']}")

        summary_text = "\n".join(summary_lines)
        graph_data = {
            "nodes": [{"id": node} for node in G.nodes()],
            "edges": [
                {"source": u, "target": v, **attrs}
                for u, v, attrs in G.edges(data=True)
            ],
            "clusters": [
                {
                    "id": i + 1,
                    "size": len(cluster),
                    "nodes": list(cluster),
                    "edges": [
                        {
                            "source": u,
                            "target": v,
                            **attrs,
                        }
                        for u, v, attrs in G.subgraph(cluster).edges(data=True)
                    ],
                }
                for i, cluster in enumerate(clusters)
                if len(cluster) >= 2
            ],
            "summary": summary_text,
        }
        return graph_data

    return graph_to_json(G, clusters)
