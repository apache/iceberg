# Testing V4 Iceberg with Spark

## Build the Iceberg Spark runtime jar

```bash
git checkout v4-amt
./gradlew :iceberg-spark:iceberg-spark-runtime-4.1_2.13:shadowJar
```

The jar is at:
```
spark/v4.1/spark-runtime/build/libs/iceberg-spark-runtime-4.1_2.13-1.11.0-SNAPSHOT.jar
```

## Download Spark 4.1.1

```bash
curl -L -o spark-4.1.1-bin-hadoop3.tgz \
  https://archive.apache.org/dist/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3.tgz
tar xzf spark-4.1.1-bin-hadoop3.tgz
```

## Start spark-sql

```bash
spark-4.1.1-bin-hadoop3/bin/spark-sql \
  --jars /path/to/iceberg-spark-runtime-4.1_2.13-1.11.0-SNAPSHOT.jar \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=file:///tmp/iceberg-warehouse
```

## Create a v4 table and query it

```sql
CREATE TABLE local.default.test (id bigint, data string)
  USING iceberg TBLPROPERTIES ('format-version' = '4');

INSERT INTO local.default.test VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT * FROM local.default.test ORDER BY id;
```

## Inspect the metadata

All paths in v4 metadata are stored as relative:

```bash
# metadata JSON -- manifest-list and metadata-log use relative paths
python3 -m json.tool /tmp/iceberg-warehouse/default/test/metadata/v2.metadata.json

# root manifest and leaf manifests are Parquet -- read with spark-sql
# (replace the UUID with the actual filename)
SELECT * FROM parquet.`file:///tmp/iceberg-warehouse/default/test/metadata/*-root-*.parquet`;
SELECT * FROM parquet.`file:///tmp/iceberg-warehouse/default/test/metadata/*-m0.parquet`;
```

## What's implemented

- V4 Adaptive Metadata Tree: root manifest (Parquet) replaces Avro manifest list
- Relative paths at all levels: metadata JSON, root manifest, leaf manifests
- Metadata deletion vectors (inline bitmaps on tracking struct)
- V4 scan path through ManifestExpander (bypasses ManifestGroup)
- FastAppend write path (INSERT INTO)

## Limitations

- Only FastAppend is wired for v4 (INSERT INTO). Overwrites, deletes, and
  compaction still use the v2/v3 path.
- Data deletion vectors (colocated with data files) are not yet implemented.
- Metadata deletion vectors are applied on read but there is no write path
  that produces them yet.
