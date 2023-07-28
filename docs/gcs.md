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

## Setting Up Google Cloud Storage (GCS)

### Setting Up a Bucket in GCS

Here's how to create a bucket in GCS:

- **Initialize the Google Cloud CLI**: Run the gcloud init command to start the initialization process of Google Cloud CLI, which sets up the Google Cloud environment on your local machine.

- **Create a Cloud Storage bucket**: Navigate to the Cloud Storage Buckets page in the Google Cloud Console. Click "Create bucket", enter your details, and click "Create".
  
## Configuring Apache Iceberg to Use GCS

Apache Iceberg uses the GCSFileIO to read and write data from/to GCS. To configure this:

- **Initialize `GCSFileIO`**: Create an instance of `GCSFileIO` by calling its constructor and supplying a `com.google.cloud.storage.Storage` instance and `GCPProperties` instance. This Storage instance will be used as the storage service engine, and GCPProperties hold GCP-specific configurations.

```java
GCSFileIO gcsFileIO = new GCSFileIO(storageSupplier, gcpProperties);
```

- **Configure GCSFileIO**: Once you have the `GCSFileIO` object, you can configure it to use your GCS bucket. You do this by calling the initialize method of `GCSFileIO` and passing a map of properties. This map holds the GCS bucket name and other required GCS settings.

```java
Map<String, String> properties = new HashMap<>();
properties.put("inputDataLocation", "gs://my_bucket/data/");
properties.put("metadataLocation", "gs://my_bucket/metadata/");
gcsFileIO.initialize(properties);
```

Within these property key-value pairs, inputDataLocation and metadataLocation are the locations in your GCS bucket where your data and metadata are stored. Update `"gs://my_bucket/data/" ` and `"gs://my_bucket/metadata/" ` to reflect the corresponding paths of your GCS bucket.

### Example Use of GCSFileIO

Once `GCSFileIO` is initialized and configured, you can interact with the data housed on GCS. Below, we will demonstrate how to create and access an `InputFile` and an `OutputFile`.

- **Creating an InputFile**: To create an `InputFile` for reading data from your GCS bucket, you can use the `newInputFile` method.

```java
InputFile inputFile = gcsFileIO.newInputFile("gs://my_bucket/data/my_data.parquet");

```

Replace `"gs://my_bucket/data/my_data.parquet"` with the path of the data you want to read.

- **Creating an OutputFile**: To write data to your GCS bucket, you would establish an OutputFile using the newOutputFile method.

```java
OutputFile outputFile = gcsFileIO.newOutputFile("gs://my_bucket/data/my_output.parquet");
```

Again, replace `"gs://my_bucket/data/my_output.parquet"` with the path where you'd like to write your data.

These steps will allow you to set up GCS as your storage layer for Apache Iceberg and interact with the data stored in GCS using the `GCSFileIO` class.

## Loading Data into Iceberg Tables

### Add Iceberg to Spark environment

To load data into Iceberg tables using Apache Spark, you must first add Iceberg to your Spark environment. It can be done using the `--packages` option when starting the Spark shell or Spark SQL:

```bash
spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.3.1
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.3.1
```

### Configure Spark Catalogs

Catalogs in Iceberg are used to track tables. They can be configured using properties under `spark.sql.catalog.(catalog_name)`. Here is an example of how to configure a catalog:

```bash
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.3.1\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse \
    --conf spark.sql.defaultCatalog=local
```

This configuration creates a path-based catalog named local for tables under `$PWD/warehouse` and adds support for Iceberg tables to Spark's built-in catalog.

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
