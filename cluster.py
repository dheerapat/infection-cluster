import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    from datetime import timedelta

    transfers = pl.read_csv("transfers.csv", try_parse_dates=True)
    print(transfers)
    micro = pl.read_csv("microbiology.csv", try_parse_dates=True)

    micro = micro.filter(pl.col("result") == "positive")
    print(micro)
    return micro, pl, timedelta, transfers


@app.cell
def _(micro, transfers):
    patient_visits = micro.join(transfers, on="patient_id", how="inner")
    print(patient_visits)
    return (patient_visits,)


@app.cell
def _(patient_visits, pl, timedelta):
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
        # different patient && deduplication
        (pl.col("patient_id_1") != pl.col("patient_id_2"))
        & (pl.col("patient_id_1") < pl.col("patient_id_2"))
        &
        # same infection
        (pl.col("infection_1") == pl.col("infection_2"))
        &
        # same ward
        (pl.col("location_1") == pl.col("location_2"))
        &
        # overlapping ward stay within +- 14 days from collection date
        (
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

    contact_pairs.write_csv("contact.csv")

    with pl.Config() as contact_cfg:
        contact_cfg.set_tbl_cols(-1)
        print(contact_pairs)
        print(contact_pairs.iter_rows())
    return (contact_pairs,)


@app.cell
def _(contact_pairs):
    import networkx as nx


    def find_simple_clusters(df):
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

    print(f"\nFound {len(clusters)} clusters:")
    for i, cluster in enumerate(clusters, start=1):
        if len(cluster) >= 2:
            cluster_nodes = list(cluster)
            print(f"\nCluster {i}: {cluster_nodes} (size: {len(cluster_nodes)})")

            # Get all edges in this cluster and their attributes
            cluster_edges = G.subgraph(cluster).edges(data=True)
            for u, v, attrs in cluster_edges:
                print(f"\tContact: {u} ↔ {v}")
                print(f"\t\tOrganism: {attrs['organism']}")
                print(f"\t\tLocation: {attrs['location']}")
    return (G,)


@app.cell
def _(G):
    from pyvis.network import Network

    nt = Network(notebook=True, height="700px", width="700px", cdn_resources='remote')
    nt.from_nx(G)
    nt.show("clusters.html")
    return


if __name__ == "__main__":
    app.run()
