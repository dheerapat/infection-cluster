import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    from datetime import timedelta

    transfers = pl.read_csv(
        "transfers.csv", 
        try_parse_dates=True
    )
    print(transfers)
    micro = pl.read_csv(
        "microbiology.csv",
        try_parse_dates=True
    )

    micro = micro.filter(pl.col("result") == "positive")
    print(micro)
    return micro, pl, timedelta, transfers


@app.cell
def _(micro, pl, timedelta, transfers):
    patient_visits = micro.join(
        transfers,
        on="patient_id",
        how="left"
    )

    patient_visits = patient_visits.filter(
        (pl.col("ward_out_time") >= pl.col("collection_date") - timedelta(days=14)) &
        (pl.col("ward_in_time") <= pl.col("collection_date") + timedelta(days=14))
    )
    print(patient_visits)
    return (patient_visits,)


@app.cell
def _(patient_visits, pl):
    pv1 = patient_visits.select([
        pl.col("patient_id").alias("patient_id_1"),
        pl.col("collection_date").alias("collection_date_1"),
        pl.col("infection").alias("infection_1"),
        pl.col("location").alias("location_1"),
        pl.col("ward_in_time").alias("ward_in_time_1"),
        pl.col("ward_out_time").alias("ward_out_time_1")
    ])

    pv2 = patient_visits.select([
        pl.col("patient_id").alias("patient_id_2"),
        pl.col("collection_date").alias("collection_date_2"),
        pl.col("infection").alias("infection_2"),
        pl.col("location").alias("location_2"),
        pl.col("ward_in_time").alias("ward_in_time_2"),
        pl.col("ward_out_time").alias("ward_out_time_2")
    ])

    contact_pairs = pv1.join(pv2, how="cross").filter(
        pl.col("patient_id_1") != pl.col("patient_id_2")
    )

    with pl.Config() as contact_cfg:
        contact_cfg.set_tbl_cols(-1)
        print(contact_pairs)
    return


if __name__ == "__main__":
    app.run()
