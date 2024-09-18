---
title: "GCP-BigQuery"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->
 
# Iceberg on Bigquery

Bigquery uses BigLake storage engine to access Iceberg on GCP.

## What is BigLake?

BigLake is a storage engine that provides a unified interface for analytics and AI engines to query multiformat, multicloud, and multimodal data in a secure, governed, and performant manner.

Key features:

Fine grained security controls
BigLake eliminates the need to grant file level access to end users. Apply table, row, column level security policies on object store tables similar to existing BigQuery tables.

Multi-compute analytics
Maintain a single copy of structured and unstructured data and make it uniformly accessible across Google Cloud and open source engines, including BigQuery, Vertex AI, Dataflow, Spark, Presto, Trino, and Hive using BigLake connectors. Centrally manage security policies in one place, and have it consistently enforced across the query engines by the API interface built into the connectors.

Multicloud governance
Discover all BigLake tables, including those defined over Amazon S3, Azure data lake Gen 2 in Data Catalog. Configure fine grained access control and have it enforced across clouds when querying with BigQuery Omni.

Built for artificial intelligence (AI)
Object tables enable use of multimodal data for governed AI workloads. Easily build AI use cases using BigQuery SQL and its Vertex AI integrations. 

Built on open formats
Supports open table and file formats including Parquet, Avro, ORC, CSV, JSON. The API serves multiple compute engines through Apache Arrow. Table format natively supports Apache Iceberg, Delta, and Hudi via manifest.
https://cloud.google.com/biglake


### BigLake Overview - BigLake Managed X Self Managed Tables

<table>
  <tr>
    <th rowspan="2">Item</th>
    <th colspan="3">BigLake</th>
  </tr>
  <tr>
    <th>Managed Table</th>
    <th>Self Managed Table</th>
    <th>Iceberg Tables via Metastore</th>
  </tr>
  <tr>
    <td>Storage Format</td>
    <td>Iceberg</td>
    <td>CSV, Delta, Hudi, Iceberg, Parquet, etc.</td>
    <td>Iceberg</td>
  </tr>
  <tr>
    <td>Storage Location</td>
    <td>Customer GCS</td>
    <td>Customer GCS</td>
    <td>Customer GCS</td>
  </tr>
  <tr>
    <td>Read/Write</td>
    <td>Create, Read, Update and Delete</td>
    <td>Read only from BQ / Updates via Spark</td>
    <td>Read only from BQ / Updates via Spark</td>
  </tr>
  <tr>
    <td>Row & Column Level Security / Data Masking</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
  </tr>
  <tr>
    <td>Fully Managed</td>
    <td>Yes (recluster, optimize, etc.)</td>
    <td>No</td>
    <td>No</td>
  </tr>
  <tr>
    <td>Partitioning</td>
    <td>Clustering</td>
    <td>Partition</td>
    <td>Partition</td>
  </tr>
  <tr>
    <td>Streaming (native)</td>
    <td>Yes</td>
    <td>No</td>
    <td>No</td>
  </tr>
  <tr>
    <td>Time Travel</td>
    <td>Soon</td>
    <td>Manual</td>
    <td>No</td>
  </tr>   

</table>



### BigLake - Create Managed Tables Example

Before start creating, you will need to create a Bigquery Cloud Resource connection in order to allow Bigquery to access the GCS bucket where the Iceberg table is and give the appropriate permissions to the service account created.
See more details here: https://cloud.google.com/bigquery/docs/create-cloud-resource-connection

#### Creating the Table

```sh
# Create a Bigquery Managed Iceberg table

CREATE OR REPLACE TABLE `bigquery_dataset.biglake_mt_taxi_trips`
(
TaxiCompany STRING,
Vendor_Id INT64,
Pickup_DateTime TIMESTAMP,
Dropoff_DateTime TIMESTAMP,
Store_And_Forward STRING,
Rate_Code_Id INT64,
PULocationID INT64,
DOLocationID INT64,
Passenger_Count INT64,
Trip_Distance FLOAT64,
Fare_Amount FLOAT64,
Surcharge FLOAT64,
MTA_Tax FLOAT64,
Tip_Amount FLOAT64,
Tolls_Amount FLOAT64,
Improvement_Surcharge FLOAT64,
Total_Amount FLOAT64,
Payment_Type_Id INT64,
Congestion_Surcharge FLOAT64,
Trip_Type INT64,
Ehail_Fee FLOAT64,
PartitionDate DATE
)
CLUSTER BY PartitionDate
WITH CONNECTION `project_id.bigquery_location.biglake_connection_id`
OPTIONS (
file_format = 'PARQUET',
table_format = 'ICEBERG',
storage_uri = 'gs://bucket_name/..../biglake_mt_taxi_trips');
```

#### Loading the Table

```sh
# Load Bigquery Managed Iceberg table

LOAD DATA INTO `bigquery_dataset.biglake_mt_taxi_trips`
FROM FILES (
uris=['gs://bucket_name/source_files/parquet/*.parquet'],
format='parquet'
);
```

#### Alter, Read, Update and Delete the Table

```sh
# Altering Bigquery Managed Iceberg table

ALTER TABLE `bigquery_dataset.biglake_mt_taxi_trips`
ADD COLUMN Taxi_New_Column STRING;

UPDATE `bigquery_dataset.biglake_mt_taxi_trips`
SET Taxi_New_Column = 'Yes'
WHERE TRUE;

DELETE FROM `bigquery_dataset.biglake_mt_taxi_trips`
WHERE Total_Amount < 0;

INSERT INTO `bigquery_dataset.biglake_mt_taxi_trips`
SELECT *
FROM `bigquery_dataset.biglake_mt_taxi_trips`;

```

After any change, you will need to export the Metadata back to the GCS Bucket in order to update the Iceberg Snapshot info

```sh

bq --project_id=project_id query --nouse_legacy_sql --nouse_cache "EXPORT TABLE METADATA FROM bigquery_dataset.biglake_mt_taxi_trips"

```


### BigLake - Create Self Managed Tables Example

BigLake Metadata file - read-only support of an Iceberg table. Requires manual updates of metadata.
BigLake Metastore - read-only support of an Iceberg table while Spark provides read/write support. Metadata is kept up to date.

We will only demonstrate how to create Iceberg Tables using BigLake Metastore Mode

Before start creating, you will need to create a Bigquery Spark connection in order to allow Bigquery to run Spark coding inside the service and give the appropriate permissions to the service account created.
See more details here: https://cloud.google.com/bigquery/docs/connect-to-spark
You can also check a step-by-step example here: https://cloud.google.com/bigquery/docs/iceberg-tables#before_you_begin

#### Creating the BigQuery Procedure to Run a Spark Job  

```sh
 # Creates a stored procedure that initializes BLMS and database.
 # Creates a table in the database and populates a few rows of data.
 CREATE OR REPLACE PROCEDURE bigquery_dataset.procedure_name ()
 WITH CONNECTION `PROCEDURE_CONNECTION_PROJECT_ID.PROCEDURE_CONNECTION_REGION.PROCEDURE_CONNECTION_ID`
 OPTIONS(engine="SPARK",
 jar_uris=["gs://spark-lib/biglake/biglake-catalog-iceberg1.2.0-0.1.0-with-dependencies.jar"],
 properties=[
 ("spark.jars.packages","org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0"),
 ("spark.sql.catalog.CATALOG", "org.apache.iceberg.spark.SparkCatalog"),
 ("spark.sql.catalog.CATALOG.catalog-impl", "org.apache.iceberg.gcp.biglake.BigLakeCatalog"),
 ("spark.sql.catalog.CATALOG.hms_uri", "HMS_URI"),
 ("spark.sql.catalog.CATALOG.gcp_project", "PROJECT_ID"),
 ("spark.sql.catalog.CATALOG.gcp_location", "LOCATION"),
 ("spark.sql.catalog.CATALOG.blms_catalog", "CATALOG"),
 ("spark.sql.catalog.CATALOG.warehouse", "DATA_WAREHOUSE_URI")
 ]
 )
 LANGUAGE PYTHON AS R'''
 from pyspark.sql import SparkSession

 spark = SparkSession \
   .builder \
   .appName("BigLake Iceberg Example") \
   .enableHiveSupport() \
   .getOrCreate()

 spark.sql("CREATE NAMESPACE IF NOT EXISTS CATALOG;")
 spark.sql("CREATE DATABASE IF NOT EXISTS CATALOG.CATALOG_DB;")
 spark.sql("DROP TABLE IF EXISTS CATALOG.CATALOG_DB.CATALOG_TABLE;")

 /* Create a BigLake Metastore table and a BigQuery Iceberg table. */
 spark.sql("CREATE TABLE IF NOT EXISTS CATALOG.CATALOG_DB.CATALOG_TABLE (id bigint, demo_name string)
           USING iceberg
           TBLPROPERTIES(bq_table='BQ_DATASET.BQ_TABLE', bq_connection='TABLE_CONNECTION_PROJECT_ID.TABLE_CONNECTION_REGION.TABLE_CONNECTION_ID');
           ")

 /* Copy a Hive Metastore table to BigLake Metastore. Can be used together with
    TBLPROPERTIES `bq_table` to create a BigQuery Iceberg table. */
 spark.sql("CREATE TABLE CATALOG.CATALOG_DB.CATALOG_TABLE (id bigint, demo_name string)
            USING iceberg
            TBLPROPERTIES(hms_table='HMS_DB.HMS_TABLE');")
 ''';
```



