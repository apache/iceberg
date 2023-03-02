from pyiceberg.catalog import load_catalog


def test_vo():
    cat = load_catalog("local")

    tbl = cat.load_table("nyc.taxis")

    df = tbl.scan().to_arrow()

    print(df)
