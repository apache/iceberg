---
title: "JDBC"
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

# Iceberg JDBC Integration

## JDBC Catalog

Iceberg supports using a table in a relational database to manage Iceberg tables through JDBC.
The database that JDBC connects to must support atomic transaction to allow the JDBC catalog implementation to 
properly support atomic Iceberg table commits and read serializable isolation.

### Configurations

Because each database and database service provider might require different configurations,
the JDBC catalog allows arbitrary configurations through:

| Property             | Default                           | Description                                            |
| -------------------- | --------------------------------- | ------------------------------------------------------ |
| uri                  |                                   | the JDBC connection string |
| jdbc.<property_key\> |                                   | any key value pairs to configure the JDBC connection | 

### Examples


#### Spark

You can start a Spark session with a MySQL JDBC connection using the following configurations:

```shell
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:{{ icebergVersion }} \
    --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://my-bucket/my/key/prefix \
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
    --conf spark.sql.catalog.my_catalog.uri=jdbc:mysql://test.1234567890.us-west-2.rds.amazonaws.com:3306/default \
    --conf spark.sql.catalog.my_catalog.jdbc.verifyServerCertificate=true \
    --conf spark.sql.catalog.my_catalog.jdbc.useSSL=true \
    --conf spark.sql.catalog.my_catalog.jdbc.user=admin \
    --conf spark.sql.catalog.my_catalog.jdbc.password=pass
```

#### Java API

```java
Class.forName("com.mysql.cj.jdbc.Driver"); // ensure JDBC driver is at runtime classpath
Map<String, String> properties = new HashMap<>();
properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
properties.put(CatalogProperties.URI, "jdbc:mysql://localhost:3306/test");
properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", "admin");
properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "pass");
properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse/path");
Configuration hadoopConf = new Configuration(); // configs if you use HadoopFileIO
JdbcCatalog catalog = CatalogUtil.buildIcebergCatalog("test_jdbc_catalog", properties, hadoopConf);
```
