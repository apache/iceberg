from pyiceberg.expressions import GreaterThan, LessThan

from pyiceberg.catalog import load_catalog


def test_vo():
    cat = load_catalog("rest")

    tbl = cat.load_table("nyc.taxis")

    df = tbl.scan(row_filter=LessThan("tpep_pickup_datetime", "2022-01-01T12:12:00+00:00")).to_arrow()

    print(df)
