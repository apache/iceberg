---
title: "Estuary"
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

# Estuary

[Estuary Flow](https://estuary.dev) is a real-time and batch ETL tool that allows you to set up data pipelines between various source and destination systems. Plug-and-play connectors can be set up in a web UI for quick pipelines, with the option of a `flowctl` CLI for more advanced customization.

Estuary can simplify implementing the Apache Iceberg™ table format with its Iceberg data connectors. These connectors automate data transformation into parquet files with planned support for automatic table maintenance as well.

## Connection options

Estuary supports Apache Iceberg as a destination with two different materialization connectors.

The regular [**Apache Iceberg**](https://docs.estuary.dev/reference/Connectors/materialization-connectors/apache-iceberg/) connector, also identified as the "standard updates" Iceberg connector, orchestrates Spark jobs to merge updates from source datasets into Iceberg tables. This connector is recommended for most use cases when working with Iceberg.

The [**Apache Iceberg Tables in S3**](https://docs.estuary.dev/reference/Connectors/materialization-connectors/amazon-s3-iceberg/) connector runs in _delta updates_ mode: each update takes a snapshot of the current source data and appends it to the Iceberg tables, rather than merging updates into existing data. This results in more of a batch-style workflow.

While delta updates can materialize data more quickly, since there are fewer compute processes involved, you will need to perform any desired reductions yourself.

## Requirements

Each Iceberg connector has different prerequisites for setup. Both currently require AWS resources, with planned support for GCP and Azure in the future.

### Standard updates connector

To use this connector, you'll need:

* An Iceberg catalog that implements the Apache Iceberg REST Catalog API
* An AWS EMR Serverless Application with the Spark runtime
* An S3 bucket for staging data files to be merged into tables
* A dedicated IAM role for executing jobs on the EMR Serverless Application
* An AWS IAM user for submitting jobs to the EMR Serverless Application

The connector currently only supports REST catalogs.

### Delta updates connector

To use this connector, you'll need:

* An S3 bucket to write files to
* An AWS IAM user with credentials for S3 and, optionally, Glue
* URI and warehouse name if using a REST catalog

The connector currently supports REST and AWS Glue catalogs.

## Setup

Since both Iceberg options with Estuary are destination connectors, using either will first require setting up source data connectors.

First [capture](https://docs.estuary.dev/concepts/captures/) data from one or more sources.

You can then optionally [transform](https://docs.estuary.dev/concepts/derivations/) data using SQL or TypeScript.

To materialize data to Iceberg using Estuary:

1. [Sign in](https://dashboard.estuary.dev/) to the Estuary dashboard.

2. From the **Destinations** tab, create a **New Materialization**.

3. Search for "Iceberg" and choose between standard or delta updates.

4. Fill out the connector details:

    * **Standard updates**
        * **URL:** The base URL for your REST catalog
        * **Warehouse:** The warehouse to connect to
        * **Namespace:** Namespace for bound collection tables
        * **Catalog authentication:** Credentials or access tokens depending on whether the catalog uses OAuth 2.0 or AWS SigV4 authentication
        * **Compute details:** Authentication, application, and storage for the Spark jobs that merge incoming data with existing data; currently supports AWS EMR Serverless as a compute type

    * **Delta updates**
        * **AWS access key ID** and **AWS secret access key:** Credentials for an IAM user
        * **Bucket:** The name of the S3 bucket to write to
        * **Region:** AWS region where the bucket lives
        * **Namespace:** Namespace for bound collection tables
        * **Catalog details:** Depending on the catalog, you will either need to provide at least a URI and warehouse (REST) or simply ensure your IAM user has the proper permissions (Glue)

5. Link to your source data by either adding all collections associated with a capture (using the **Source from Capture** button) or adding collections individually.

6. Click **Next**, optionally review the generated Advanced Specification, and then **Save and Publish** once you're satisfied with your configuration.

Your materialization will be created and you will be able to view associated usage and logs.

For additional help, such as specific steps to set up an EMR application, see [Estuary's documentation](https://docs.estuary.dev/reference/Connectors/materialization-connectors/apache-iceberg/).
