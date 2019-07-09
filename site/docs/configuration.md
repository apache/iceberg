# Configuration

## Table properties

Iceberg tables support table properties to configure table behavior, like the default split size for readers.

### Read properties

| Property                     | Default            | Description                                            |
| ---------------------------- | ------------------ | ------------------------------------------------------ |
| read.split.target-size       | 134217728 (128 MB) | Target size when combining input splits                |
| read.split.planning-lookback | 10                 | Number of bins to consider when combining input splits |


### Write properties

| Property                           | Default            | Description                                        |
| ---------------------------------- | ------------------ | -------------------------------------------------- |
| write.format.default               | parquet            | Default file format for the table; parquet or avro |
| write.parquet.row-group-size-bytes | 134217728 (128 MB) | Parquet row group size                             |
| write.parquet.page-size-bytes      | 1048576 (1 MB)     | Parquet page size                                  |
| write.parquet.dict-size-bytes      | 2097152 (2 MB)     | Parquet dictionary page size                       |
| write.parquet.compression-codec    | gzip               | Parquet compression codec                          |
| write.avro.compression-codec       | gzip               | Avro compression codec                             |


### Table behavior properties

| Property                           | Default          | Description                                                   |
| ---------------------------------- | ---------------- | ------------------------------------------------------------- |
| commit.retry.num-retries           | 4                | Number of times to retry a commit before failing              |
| commit.retry.min-wait-ms           | 100              | Minimum time in milliseconds to wait before retrying a commit |
| commit.retry.max-wait-ms           | 60000 (1 min)    | Maximum time in milliseconds to wait before retrying a commit |
| commit.retry.total-timeout-ms      | 1800000 (30 min) | Maximum time in milliseconds to wait before retrying a commit |
| commit.manifest.target-size-bytes  | 8388608 (8 MB)   | Target size when merging manifest files                       |
| commit.manifest.min-count-to-merge | 100              | Minimum number of manifests to accumulate before merging      |


## Spark options

### Read options

Spark read options are passed when configuring the DataFrameReader, like this:

```scala
// time travel
spark.read
    .format("iceberg")
    .option("snapshot-id", 10963874102873L)
    .load("db.table")
```

| Spark option    | Default  | Description                                                                               |
| --------------- | -------- | ----------------------------------------------------------------------------------------- |
| snapshot-id     | (latest) | Snapshot ID of the table snapshot to read                                                 |
| as-of-timestamp | (latest) | A timestamp in milliseconds; the snapshot used will be the snapshot current at this time. |

### Write options

Spark write options are passed when configuring the DataFrameWriter, like this:

```scala
// write with Avro instead of Parquet
df.write
    .format("iceberg")
    .option("write-format", "avro")
    .save("db.table")
```

| Spark option | Default                    | Description                                                  |
| ------------ | -------------------------- | ------------------------------------------------------------ |
| write-format | Table write.format.default | File format to use for this write operation; parquet or avro |

