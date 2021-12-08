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

# Iceberg Aliyun Integrations

Iceberg provides integration with different Aliyun services through the `iceberg-aliyun` module.
This section describes how to use Iceberg with [Aliyun](https://www.alibabacloud.com/).

## Enabling Aliyun Integration

The `iceberg-aliyun` module provides the work which integrates alibaba cloud services with apache iceberg. Currently, it
provides the bundled `iceberg-aliyun-runtime` for users to access the iceberg table backed in alibaba cloud services.
People only need to guarantee that the `iceberg-aliyun-runtime` jar is loaded into classpath correctly by the engines
such as Spark, Flink, Hive, Presto etc.

## Catalogs

[Aliyun DLF](https://www.aliyun.com/product/bigdata/dlf) is a core service from Alibaba Cloud that satisfies users'
needs for data asset management while creating data lakes. DLF provides unified metadata views and permission management
for data available in OSS ([Aliyun Object Storage Service](https://www.alibabacloud.com/product/object-storage-service))
. It also provides real-time lake migration and cleaning templates for data and production-level metadata services for
upper-layer data analysis engines.

### Engines Access.

All the engines (Spark, Hive, Flink, Presto) can access the iceberg table backed in aliyun cloud. There are following
examples to show how to access it.

The idea way to show the example is using Aliyun DLF Catalog to manage those iceberg tables, but we still don't finish
the iceberg + DLF integration work yet. Here we are showing the examples to manage iceberg tables in Hive Catalog.

### Spark

For example, to access apache iceberg tables stored in alibaba object storage service with Apache Spark 3.2.x:

```bash
# add Iceberg dependency
ICEBERG_VERSION=0.13.0
ALIYUN_OSS_ENDPOINT=******       # Your aliyun oss endpoint.
ALIYUN_ACCESS_KEY_ID=******      # Your aliyun access key id.
ALIYUN_ACCESS_KEY_SECRET=******  # Your aliyun access key secret.

DEPENDENCIES="org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:$ICEBERG_VERSION"
DEPENDENCIES+=",org.apache.iceberg:iceberg-aliyun-runtime:$ICEBERG_VERSION"

# start Spark SQL client shell
spark-sql --packages $DEPENDENCIES \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.uri=thrift://<host>:<port> \
    --conf spark.sql.catalog.my_catalog.warehouse=oss://my-bucket/my/key/prefix \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aliyun.oss.OSSFileIO \
    --conf spark.sql.catalog.my_catalog.oss.endpoint=$ALIYUN_OSS_ENDPOINT \
    --conf spark.sql.catalog.my_catalog.client.access-key-id=$ALIYUN_ACCESS_KEY_ID \
    --conf spark.sql.catalog.my_catalog.client.access-key-secret=$ALIYUN_ACCESS_KEY_SECRET
```

```sql
CREATE TABLE my_catalog.default.sample (
    id    BIGINT,
    data  STRING
)
USING iceberg
TBLPROPERTIES (
  'engine.hive.enabled' = 'true'
);
```

### Flink

Take the sample, to access apache iceberg tables stored in aliyun object storage service with Apache Flink 1.13: 

```bash
ICEBERG_VERSION=0.13.0
wget $ICEBERG_MAVEN_URL/iceberg-flink-runtime/$ICEBERG_VERSION/iceberg-flink-runtime-$ICEBERG_VERSION.jar


./bin/sql-client.sh embedded \
  -j /path/to/flink-sql-connector-hive-2.3.6_2.12-1.13.2.jar \
  -j /path/to/iceberg-aliyun-runtime-$ICEBERG_VERSION.jar \
  -j /path/to/iceberg-flink-1.13-runtime-$ICEBERG_VERSION.jar \
  shell
```

```sql
CREATE CATALOG hive WITH (
    'type' = 'iceberg',
    'uri' = 'thrift://localhost:9083',
    'warehouse' = 'oss://my-bucket/my-object',
    'io-impl' = 'org.apache.iceberg.aliyun.oss.OSSFileIO',
    'oss.endpoint' = '<your-oss-endpoint-address>',
    'client.access-key-id' = '<your-aliyun-access-key>',
    'client.access-key-secret' = '<your-aliyun-access-secret>'
);

CREATE TABLE `hive`.`default`.`sample` (
    id   BIGINT,
    data STRING
) WITH (
    'engine.hive.enabled' = 'true'
);

INSERT INTO `hive`.`default`.`sample` VALUES (1, 'AAA');
```

## Aliyun Integration Tests

To verify all the `iceberg-aliyun` features work fine with the integration tests, we can follow the below scripts to test:

```bash
# Export the aliyun access key pair.
export ALIYUN_TEST_ACCESS_KEY_ID=<YOUR_TEST_ACCESS_KEY_ID>
export ALIYUN_TEST_ACCESS_KEY_SECRET=<YOUR_TEST_ACCESS_KEY_SECRET>

# Export aliyun oss configuration keys.
export ALIYUN_TEST_OSS_TEST_RULE_CLASS=org.apache.iceberg.aliyun.oss.OSSIntegrationTestRule
export ALIYUN_TEST_BUCKET=<YOUR_TEST_OSS_BUCKET>
export ALIYUN_TEST_OSS_WAREHOUSE=oss://<YOUR_TEST_OSS_BUCKET>/<YOUR_TEST_OSS_OBJECT>
export ALIYUN_TEST_OSS_ENDPOINT=<YOUR_TEST_OSS_ENDPOINT>

# Run the iceberg-aliyun integration tests
git clone git@github.com:apache/iceberg.git
cd iceberg
./gradlew iceberg-aliyun:build -Pquick=true -x javadoc
```