---
title: "GCS"
url: gcs
menu:
    main:
        parent: Integrations
        identifier: gcs_integration
        weight: 0
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements. See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License. You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Iceberg GSC Integration

Google Cloud Storage (GCS) is a scalable object storage service known for its durability, throughput, and availability. It is designed to handle large amounts of unstructured data, making it an excellent choice for significant data operations. When GCS's storage capabilities are combined with Apache Iceberg's handling of large tabular datasets, a powerful tool for extensive data management is created. This combination allows for storing vast amounts of data and the execution of complex data operations directly on the data stored in GCS.
  
## Configuring Apache Iceberg to Use GCS with Spark

To configure Apache Iceberg to use GCS with Spark, you can follow these steps:

- **Add a catalog**: Iceberg supports multiple catalog back-ends for tracking tables. In this case, we will use the SparkSessionCatalog. You can configure the catalog by setting the following properties:
- `spark.sql.catalog.spark_catalog`: Set this property to `org.apache.iceberg.spark.SparkSessionCatalog` to use the SparkSessionCatalog.
- `spark.sql.catalog.spark_catalog.type`: Set this property to `hive` to use the Hive catalog type.
- `spark.sql.catalog.local`: Set this property to `org.apache.iceberg.spark.SparkCatalog` to use the SparkCatalog.
- `spark.sql.catalog.local.type`: Set this property to `hadoop` to use the Hadoop catalog type.
- `spark.sql.catalog.local.warehouse`: Set this property to the path where you want to store the Iceberg tables in GCS.

Here is an example of how to configure the catalog using the Spark SQL command line:

```bash
shell spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
--conf spark.sql.catalog.spark_catalog.type=hive
--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.local.type=hadoop
--conf spark.sql.catalog.local.warehouse=<GCS_PATH>
```

- **Add Iceberg to Spark**: If you already have a Spark environment, you can add Iceberg by specifying the `--packages` option when starting Spark. This will download the required Iceberg package and make it available in your Spark session. Here is an example:

```bash
shell spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1

```

- **Use Iceberg in Spark**: Once Iceberg is configured, you can use it in your Spark application or Spark SQL queries. Check [Spark Configuration](spark-configuration.md) for more details.

## Loading Data into Iceberg Tables

### Add Iceberg to Spark environment

To load data into Iceberg tables using Apache Spark, you must first add Iceberg to your Spark environment. It can be done using the `--packages` option when starting the Spark shell or Spark SQL:

```bash
spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1
```

### Configure Spark Catalogs

Catalogs in Iceberg are used to track tables. They can be configured using properties under `spark.sql.catalog.(catalog_name)`. Here is an example of how to configure a catalog:

```bash
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hive \
    --conf spark.sql.catalog.local.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
    --conf spark.sql.catalog.local.warehouse=gs://your_bucket/warehouse \
    --conf spark.sql.defaultCatalog=local
```

Please replace `gs://your_bucket/warehouse` with the actual path of your GCS bucket.

This configuration creates a path-based catalog named local for tables under `gs://your_bucket/warehouse` and adds support for Iceberg tables to Spark's built-in catalog.

### Load data into Iceberg Tables

- **CSV format**:

```java
val csvDF = spark.read.format("csv").option("header", "true").load("path_to_your_csv")
csvDF.write.format("iceberg").mode("append").save("local.db.your_table")
```

- **Parquet format**:

```java
val parquetDF = spark.read.format("parquet").load("path_to_your_parquet")
parquetDF.write.format("iceberg").mode("append").save("local.db.your_table")
```

Specify `path_to_your_csv` and `path_to_your_parquet` with the actual paths of your CSV and Parquet files, respectively. Replace `your_table` with the name of the table you want to create in Iceberg.

> Please note that Iceberg table names are case-sensitive; hence ensure the terms you use in the above code are case-correct.

## Querying Data from Iceberg Tables

### Using DataFrameReader API

You can use Spark's `read.format("iceberg")` method to read an Iceberg table into a DataFrame. Here is an example:

```java
// Read an Iceberg table into a DataFrame
var df = spark.read.format("iceberg").load("local.db.one");

// Perform operations on the DataFrame
df.show();
df.filter(df.col("column_name").gt(100)).show();
```

In the above example, you can replace `"local.db.one"` with the name of your Iceberg table. The `show()` method is used to display the contents of the DataFrame. The `filter()` method is used to filter the DataFrame based on a condition.

### Using Spark SQL

You can also use Spark SQL to query data from Iceberg tables. Here is an example:

```bash
// Query data from an Iceberg table using Spark SQL
spark.sql("SELECT * FROM local.db.one").show();
spark.sql("SELECT column_name, count(*) FROM local.db.one GROUP BY column_name").show();
```

In the above example, you can replace `"local.db.one"` with the name of your Iceberg table. The `sql()` method performs SQL queries on the Iceberg table.
