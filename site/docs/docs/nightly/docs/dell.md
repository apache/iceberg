---
title: "Dell"
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

```bash
ICEBERG_VERSION=0.15.0
SPARK_VERSION=3.2_2.12
ECS_CLIENT_VERSION=3.3.2

DEPENDENCIES="org.apache.iceberg:iceberg-spark-runtime-${SPARK_VERSION}:${ICEBERG_VERSION},\
org.apache.iceberg:iceberg-dell:${ICEBERG_VERSION},\
com.emc.ecs:object-client-bundle:${ECS_CLIENT_VERSION}"

spark-sql --packages ${DEPENDENCIES} \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=ecs://bucket-a/namespace-a \
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.dell.ecs.EcsCatalog \
    --conf spark.sql.catalog.my_catalog.ecs.s3.endpoint=http://10.x.x.x:9020 \
    --conf spark.sql.catalog.my_catalog.ecs.s3.access-key-id=<Your-ecs-s3-access-key> \
    --conf spark.sql.catalog.my_catalog.ecs.s3.secret-access-key=<Your-ecs-s3-secret-access-key>
```

Then, use `my_catalog` to access the data in ECS. You can use `SHOW NAMESPACES IN my_catalog` and `SHOW TABLES IN my_catalog` to fetch the namespaces and tables of the catalog.

The related problems of catalog usage:

1. The `SparkSession.catalog` won't access the 3rd-party catalog of Spark in both Python and Scala, so please use DDL SQL to list all tables and namespaces.


### Flink

Use the Dell ECS catalog with Flink, you first must create a Flink environment.

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

# download Iceberg dependency
MAVEN_URL=https://repo1.maven.org/maven2
ICEBERG_VERSION=0.15.0
FLINK_VERSION=1.14
wget ${MAVEN_URL}/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar
wget ${MAVEN_URL}/org/apache/iceberg/iceberg-dell/${ICEBERG_VERSION}/iceberg-dell-${ICEBERG_VERSION}.jar

# download ECS object client
ECS_CLIENT_VERSION=3.3.2
wget ${MAVEN_URL}/com/emc/ecs/object-client-bundle/${ECS_CLIENT_VERSION}/object-client-bundle-${ECS_CLIENT_VERSION}.jar

# open the SQL client.
/path/to/bin/sql-client.sh embedded \
    -j iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar \
    -j iceberg-dell-${ICEBERG_VERSION}.jar \
    -j object-client-bundle-${ECS_CLIENT_VERSION}.jar \
    shell
```

Then, use Flink SQL to create a catalog named `my_catalog`:

```SQL
CREATE CATALOG my_catalog WITH (
    'type'='iceberg',
    'warehouse' = 'ecs://bucket-a/namespace-a',
    'catalog-impl'='org.apache.iceberg.dell.ecs.EcsCatalog',
    'ecs.s3.endpoint' = 'http://10.x.x.x:9020',
    'ecs.s3.access-key-id' = '<Your-ecs-s3-access-key>',
    'ecs.s3.secret-access-key' = '<Your-ecs-s3-secret-access-key>');
```

Then, you can run `USE CATALOG my_catalog`, `SHOW DATABASES`, and `SHOW TABLES` to fetch the namespaces and tables of the catalog.

### Limitations

When you use the catalog with Dell ECS only, you should care about these limitations:

1. `RENAME` statements are supported without other protections. When you try to rename a table, you need to guarantee all commits are finished in the original table.
2. `RENAME` statements only rename the table without moving any data files. This can lead to a table's data being stored in a path outside of the configured warehouse path.
3. The CAS operations used by table commits are based on the checksum of the object. There is a very small probability of a checksum conflict.
