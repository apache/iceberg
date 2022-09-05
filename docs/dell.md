---
title: "Dell"
url: dell
menu:
    main:
        parent: Integrations
        weight: 0
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


# Iceberg Dell Integration

## Dell ECS Integration

Iceberg can be used with Dell's Enterprise Object Storage (ECS) by using the ECS catalog since 0.15.0.

See [Dell ECS](https://www.dell.com/en-us/dt/storage/ecs/index.htm) for more information on Dell ECS.

### Parameters

When using Dell ECS with Iceberg, these configuration parameters are required:

| Name                     | Description                       |
| ------------------------ | --------------------------------- |
| ecs.s3.endpoint          | ECS S3 service endpoint           |
| ecs.s3.access-key-id     | ECS Username                      |
| ecs.s3.secret-access-key | S3 Secret Key                     |
| warehouse                | The location of data and metadata |

The warehouse should use the following formats:

| Example                    | Description                                                     |
| -------------------------- | --------------------------------------------------------------- |
| ecs://bucket-a             | Use the whole bucket as the data                                |
| ecs://bucket-a/            | Use the whole bucket as the data. The last `/` is ignored.      |
| ecs://bucket-a/namespace-a | Use a prefix to access the data only in this specific namespace |

The Iceberg `runtime` jar supports different versions of Spark and Flink. You should pick the correct version.

Even though the [Dell ECS client](https://github.com/EMCECS/ecs-object-client-java) jar is backward compatible, Dell EMC still recommends using the latest version of the client.

### Spark

To use the Dell ECS catalog with Spark 3.2.1, you should create a Spark session like:

```python
from pyspark.sql import SparkSession

jars = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.15.0",
    "org.apache.iceberg:iceberg-dell:0.15.0",
    "com.emc.ecs:object-client-bundle:3.3.2"
])

spark = SparkSession.builder
    .config("spark.jars.packages", jars)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.my_catalog.warehouse", "ecs://bucket-a/namespace-a")
    .config("spark.sql.catalog.my_catalog.catalog-impl", "org.apache.iceberg.dell.ecs.EcsCatalog")
    .config("spark.sql.catalog.my_catalog.ecs.s3.endpoint", "http://10.x.x.x:9020")
    .config("spark.sql.catalog.my_catalog.ecs.s3.access-key-id", "<Your-ecs-s3-access-key>")
    .config("spark.sql.catalog.my_catalog.ecs.s3.secret-access-key", "<Your-ecs-s3-secret-access-key>")
    .getOrCreate()
```

Then, use `my_catalog` to access the data in ECS. You can use `SHOW NAMESPACES IN my_catalog` and `SHOW TABLES IN my_catalog` to fetch the namespaces and tables of the catalog.

The related problems of catalog usage:

1. The `pyspark.sql.SparkSession.catalog` won't access the 3rd-party catalog of Spark, so please use DDL SQL to list all tables and namespaces.


### Flink

Use the Dell ECS catalog with Flink, you first must create a Flink environment.

```python
import requests
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# Set your workspace
work_space = "<your_work_space>"

jars = {
    "iceberg-flink-runtime-1.14-0.15.0.jar" : "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.14/0.15.0/iceberg-flink-runtime-1.14-0.15.0.jar",
    "iceberg-dell.jar" : "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-dell/0.15.0/iceberg-dell-0.15.0.jar",
    "object-client-bundle-3.3.2.jar" : "https://repo1.maven.org/maven2/com/emc/ecs/object-client-bundle/3.3.2/object-client-bundle-3.3.2.jar",
}

# Download libraries
for jar, link in jars.items():
    with open(f"{work_space}/{jar}", "wb") as f:
        f.write(requests.get(link).content)

pipeline_jars = [f"file://{work_space}/{jar}" for jar in jars]

# Setup Flink session
env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars(*pipeline_jars)
t_env = StreamTableEnvironment.create(env)
```

Then, use Flink SQL to create a catalog named `my_catalog`:

```SQL
CREATE CATALOG my_catalog WITH (
    'type'='iceberg',
    'warehouse' = 'ecs://bucket-a/namespace-a',
    'catalog-impl'='org.apache.iceberg.dell.ecs.EcsCatalog',
    'ecs.s3.endpoint' = 'http://10.x.x.x:9020',
    'ecs.s3.access-key-id' = '<Your-ecs-s3-access-key>',
    'ecs.s3.secret-access-key' = '<Your-ecs-s3-secret-access-key>')
```

Then, you can run `USE CATALOG my_catalog`, `SHOW DATABASES`, and `SHOW TABLES` to fetch the namespaces and tables of the catalog.

### Limitations

When you use the catalog with Dell ECS only, you should care about these limitations:

1. `RENAME` statements are supported without other protections. When you try to rename a table, you need to guarantee all commits are finished in the original table.
2. `RENAME` statements only rename the table without moving any data files. This can lead to a table's data being stored in a path outside of the configured warehouse path.
3. The CAS operations used by table commits are based on the checksum of the object. There is a very small probability of a checksum conflict.
