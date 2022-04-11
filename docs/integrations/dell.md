---
title: "Dell"
url: dell
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


# Iceberg Dell Integrations

## Dell ECS Integrations

This session will show you how to use Iceberg with Dell ECS. Dell ECS provides several features that are more appropriate for Iceberg:

1. Append operation for file writer.
2. CAS operation for Table commit.

See [Dell ECS](https://www.dell.com/en-us/dt/storage/ecs/index.htm) for more information on Dell ECS.

### Connection parameters

When you try to connect Dell ECS with Iceberg, these connection parameters should be prepared:

| Name                     | Description            |
| ------------------------ | ---------------------- |
| ecs.s3.endpoint          | ECS S3 service endpint |
| ecs.s3.access-key-id     | User name              |
| ecs.s3.secret-access-key | S3 Secret Key          |

And for the catalog, you should provide a `warehouse` location that will store all data and metadata.

| Example                    | Description                                                     |
| -------------------------- | --------------------------------------------------------------- |
| ecs://bucket-a             | Use the whole bucket as the data                                |
| ecs://bucket-a/namespace-a | Use a prefix to access the data only in this specific namespace |

When you provide the `warehouse`, the last / will be ignored. The `ecs://bucket-a` is same with `ecs://bucket-a/`.

### Dependencies of runtime

The Iceberg `runtime` jar supports different versions of Spark and Flink. So if the version is not matched in the example, please check the related document of Spark and Flink.

The [Dell ECS client](https://github.com/EMCECS/ecs-object-client-java) jar is backward compatible. But Dell EMC still recommends using the latest version of the client.

### Spark

For example, to use the Dell ECS catalog with Spark 3.2.1, you can create a Spark session like:

```sh
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.14.0,com.emc.ecs:object-client-bundle:3.3.2 \
    --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=ecs://bucket-a/namespace-a \
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.dell.ecs.EcsCatalog \
    --conf spark.sql.catalog.my_catalog.ecs.s3.endpoint=http://10.x.x.x:9020 \
    --conf spark.sql.catalog.my_catalog.ecs.s3.access-key-id=test \
    --conf spark.sql.catalog.my_catalog.ecs.s3.secret-access-key=xxxxxxxxxxxxxxxx
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.14.0,com.emc.ecs:object-client-bundle:3.3.2")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")\
    .config("spark.sql.catalog.my_catalog.warehouse", "ecs://bucket-a/namespace-a")\
    .config("spark.sql.catalog.my_catalog.catalog-impl", "org.apache.iceberg.dell.ecs.EcsCatalog")\
    .config("spark.sql.catalog.my_catalog.ecs.s3.endpoint", "http://10.x.x.x:9020")\
    .config("spark.sql.catalog.my_catalog.ecs.s3.access-key-id", "test")\
    .config("spark.sql.catalog.my_catalog.ecs.s3.secret-access-key", "xxxxxxxxxxxxxxxx")\
    .getOrCreate()
```

Then, use `my_catalog` to access the data in ECS. You can use `SHOW NAMESPACES IN my_catalog` and `SHOW TABLES IN my_catalog` to fetch the namespaces and tables of the catalog.

The related problems of catalog usage:

1. The [pyspark.sql.SparkSession.catalog](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.catalog.html#pyspark.sql.SparkSession.catalog) won't access the 3rd-party catalog of Spark. So please use DDL SQL to list all tables and namespaces.


### Flink

For example, to use the Dell ECS catalog with Flink 1.14, you can create a Flink environment like:

```sh
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.14/0.14.0/iceberg-flink-runtime-1.14-0.14.0.jar
wget https://repo1.maven.org/maven2/com/emc/ecs/object-client-bundle/3.3.2/object-client-bundle-3.3.2.jar

sql-client.sh embedded -j iceberg-flink-runtime-1.14-0.14.0.jar -j object-client-bundle-3.3.2.jar shell
```

```python
import requests
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# Set your workspace
work_space = "/"

# Download libraries
maven_url="https://repo1.maven.org/maven2"
with open(f"{work_space}/iceberg-flink-runtime-1.14-0.14.0.jar", "wb") as f:
  f.write(requests.get(f"{maven_url}/org/apache/iceberg/iceberg-flink-runtime-1.14/0.14.0/iceberg-flink-runtime-1.14-0.14.0.jar").content)
with open(f"{work_space}/object-client-bundle-3.3.2.jar", "wb") as f:
  f.write(requests.get(f"{maven_url}/com/emc/ecs/object-client-bundle/3.3.2/object-client-bundle-3.3.2.jar").content)

jars = ["iceberg-flink-runtime-1.14-0.14.0.jar", "object-client-bundle-3.3.2.jar"]
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
    'ecs.s3.access-key-id' = 'test',
    'ecs.s3.secret-access-key' = 'xxxxxxxxxxxxxxxx')
```

Then, `USE CATALOG my_catalog`, `SHOW DATABASES`, and `SHOW TABLES` to fetch the namespaces and tables of the catalog.

### Limitations

When you use the catalog with Dell ECS only, you should care about these limitations:

1. The rename operation is supported without other protection. When you try to rename a table, please guarantee all commits are finished in the original table.
2. The rename operation only renames the table without moving any data file. The renamed table maybe store data objects in different paths even not in the `warehouse` that is configured in the catalog.
3. The CAS operations used by table commits are based on the checksum of the object. There is a very small probability of a checksum conflict.