from pyiceberg.expressions import And, GreaterThan, LessThan


def test_vo():
    from pyiceberg.catalog import load_catalog

    cat = load_catalog("local")

    tbl = cat.load_table("nyc.taxis")

    df = tbl.scan(
        row_filter=And(
            GreaterThan("tpep_pickup_datetime", "2021-12-02T12:00:00+00:00"),
            LessThan("tpep_pickup_datetime", "2021-12-20T12:00:00+00:00"),
        )
    ).to_arrow()

    print(df)
