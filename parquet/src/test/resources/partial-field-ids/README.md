# Partial field IDs fixture

`a.parquet` is copied unchanged from Apache Iceberg verification PR #27,
commit `9542d5291c100cb036648beb11920feb1b5f69c1`:
https://github.com/apache/iceberg-verification/blob/9542d5291c100cb036648beb11920feb1b5f69c1/reference-tables/v2/partial-field-ids-parquet-data/data/a.parquet

The fixture was contributed by Sylwester Lachiewicz. Its source README describes
the schema and the three rows. Top-level and struct fields carry IDs; the list
element and map key/value do not. Physical field names and order differ from the
table schema. These tests explore the proposed behavior in Iceberg PR #19037;
they do not claim that its still-proposed semantics are already normative.
