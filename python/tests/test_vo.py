def test_vo():
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("local")

    tbl = catalog.load_table("pyiceberg.taxis")

    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

    df = (
        tbl.scan(
            row_filter=And(
                GreaterThanOrEqual("tpep_pickup_datetime", "2021-04-01T00:00:00.000000+00:00"),
                LessThan("tpep_pickup_datetime", "2021-05-01T00:00:00.000000+00:00"),
            )
        )
        .to_arrow()
        .to_pandas()
    )
