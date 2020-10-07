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

# Cloud Integration

This section describes the cloud integrations for Iceberg.

## HDFS Connector

Iceberg uses `HadoopFileIO` to write files for all the catalogs by default. 
Therefore, Iceberg supports writing to any cloud storage that has an HDFS connector. 
Here is a list of commonly used cloud storage connectors:

| Storage                 | URI scheme | Documentation |
|-------------------------|------------|---------------|
| Amazon S3               | s3a        | [link](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html) |
| Amazon EMR File System  | s3         | [link](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-fs.html) |
| Azura Data Lake Storage | adl        | [link](https://hadoop.apache.org/docs/current/hadoop-azure-datalake/index.html) |
| Azura Blob Storage      | wasb       | [link](http://hadoop.apache.org/docs/r2.7.1/hadoop-azure/index.html) |
| Google Cloud Storage    | gs         | [link](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) |

For example, to use S3 with `HadoopCatalog`, you can use the following configurations in Spark:

```
spark.sql.catalog.hadoop_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hadoop_prod.type = hadoop
spark.sql.catalog.hadoop_prod.warehouse = s3a://my-bucket/my-file-path
```

Please refer to the specific documentations for the limitations and setups required to use each cloud storage.

## AWS Integrations

All the AWS supports are in the `iceberg-aws` submodule. Please add the dependency to use the integration features.

### Using Hive-compatible AWS Glue catalog

The `iceberg-aws` module provides `IcebergGlueMetastoreClient`, which is an implementation of the Hive 
`IMetaStoreClient` interface for users to use AWS Glue as their serverless Hive metastore.
Because AWS Glue does not support Hive's transaction and locking interface, the Glue client currently uses DynamoDB
to provide this additional support.

To enable Glue, set the Hadoop configuration `iceberg.hive.client-impl` to 
`org.apache.iceberg.aws.glue.metastore.IcebergGlueMetastoreClient`. 
Then just use `HiveCatalog` as usual, and you do not need to specify the Hive Thrift URI anymore.

The client uses the [default AWS credential chain](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html)
to read IAM credentials.
All configurable settings of this client can be found in `org.apache.iceberg.aws.glue.util.AWSGlueConfig`.

For more details about AWS Glue data catalog, please refer to the following docs:
- [AWS Glue Introduction](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)
- [Using Glue on EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive-metastore-glue.html)
