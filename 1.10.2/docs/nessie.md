---
title: "Nessie"
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

# Iceberg Nessie Integration

Iceberg provides integration with Nessie through the `iceberg-nessie` module.
This section describes how to use Iceberg with Nessie. Nessie provides several key features on top of Iceberg:

* multi-table transactions
* git-like operations (eg branches, tags, commits)
* hive-like metastore capabilities

See [Project Nessie](https://projectnessie.org) for more information on Nessie. Nessie requires a server to run, see
[Getting Started](https://projectnessie.org/try/) to start a Nessie server.

## Enabling Nessie Catalog

The `iceberg-nessie` module is bundled with Spark and Flink runtimes for all versions from `0.11.0`. To get started
with Nessie (with spark-3.5) and Iceberg simply add the Iceberg runtime to your process. Eg: `spark-sql --packages
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{{ icebergVersion }}`. 

## Spark SQL Extensions

Nessie SQL extensions can be used to manage the Nessie repo as shown below.
Example for Spark 3.5 with scala 2.12:

```
bin/spark-sql 
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{{ icebergVersion }},org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:{{ nessieVersion }}"
  --conf spark.sql.extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
  --conf <other settings>
```
Please refer [Nessie SQL extension document](https://projectnessie.org/tools/sql/) to learn more about it.

## Nessie Catalog

One major feature introduced in release `0.11.0` is the ability to easily interact with a [Custom Catalog](custom-catalog.md) from Spark and Flink. See [Spark Configuration](spark-configuration.md#catalog-configuration)
  and [Flink Configuration](flink.md#custom-catalog) for instructions for adding a custom catalog to Iceberg. 

To use the Nessie Catalog the following properties are required:

* `warehouse`. Like most other catalogs the warehouse property is a file path to where this catalog should store tables.
* `uri`. This is the Nessie server base uri. Eg `http://localhost:19120/api/v2`.
* `ref` (optional). This is the Nessie branch or tag you want to work in.

To run directly in Java this looks like:

``` java
Map<String, String> options = new HashMap<>();
options.put("warehouse", "/path/to/warehouse");
options.put("ref", "main");
options.put("uri", "https://localhost:19120/api/v2");
Catalog nessieCatalog = CatalogUtil.loadCatalog("org.apache.iceberg.nessie.NessieCatalog", "nessie", options, hadoopConfig);
```

and in Spark:

``` java
conf.set("spark.sql.catalog.nessie.warehouse", "/path/to/warehouse");
conf.set("spark.sql.catalog.nessie.uri", "http://localhost:19120/api/v2")
conf.set("spark.sql.catalog.nessie.ref", "main")
conf.set("spark.sql.catalog.nessie.type", "nessie")
conf.set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
```
This is how it looks in Flink via the Python API (additional details can be found [here](flink.md#preparation-when-using-flinks-python-api)):
```python
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
iceberg_flink_runtime_jar = os.path.join(os.getcwd(), "iceberg-flink-runtime-{{ icebergVersion }}.jar")
env.add_jars("file://{}".format(iceberg_flink_runtime_jar))
table_env = StreamTableEnvironment.create(env)

table_env.execute_sql("CREATE CATALOG nessie_catalog WITH ("
                      "'type'='iceberg', "
                      "'type'='nessie', "
                      "'uri'='http://localhost:19120/api/v2', "
                      "'ref'='main', "
                      "'warehouse'='/path/to/warehouse')")
```

There is nothing special above about the `nessie` name. A spark catalog can have any name, the important parts are the 
settings for the `type` or `catalog-impl` and the required config to start Nessie correctly.
Once you have a Nessie catalog you have access to your entire Nessie repo. You can then perform create/delete/merge
operations on branches and perform commits on branches. Each Iceberg table in a Nessie Catalog is identified by an
arbitrary length namespace and table name (eg `data.base.name.table`). These namespaces must be explicitly created 
as mentioned [here](https://projectnessie.org/blog/namespace-enforcement/).
Any transaction on a Nessie enabled Iceberg table is a single commit in Nessie. Nessie commits
can encompass an arbitrary number of actions on an arbitrary number of tables, however in Iceberg this will be limited
to the set of single table transactions currently available.

Further operations such as merges, viewing the commit log or diffs are performed by direct interaction with the
`NessieClient` in java or by using the python client or cli. See [Nessie CLI](https://projectnessie.org/tools/cli/) for
more details on the CLI and [Spark Guide](https://projectnessie.org/tools/iceberg/spark/) for a more complete description of 
Nessie functionality.

## Nessie and Iceberg

For most cases Nessie acts just like any other Catalog for Iceberg: providing a logical organization of a set of tables
and providing atomicity to transactions. However, using Nessie opens up other interesting possibilities. When using Nessie with
Iceberg every Iceberg transaction becomes a Nessie commit. This history can be listed, merged or cherry-picked across branches.

### Loosely coupled transactions

By creating a branch and performing a set of operations on that branch you can approximate a multi-table transaction.
A sequence of commits can be performed on the newly created branch and then merged back into the main branch atomically.
This gives the appearance of a series of connected changes being exposed to the main branch simultaneously. While downstream
consumers will see multiple transactions appear at once this isn't a true multi-table transaction on the database. It is 
effectively a fast-forward merge of multiple commits (in git language) and each operation from the branch is its own distinct
transaction and commit. This is different from a real multi-table transaction where all changes would be in the same commit.
This does allow multiple applications to take part in modifying a branch and for this distributed set of transactions to be 
exposed to the downstream users simultaneously.

 
### Experimentation

Changes to a table can be tested in a branch before merging back into main. This is particularly useful when performing
large changes like schema evolution or partition evolution. A partition evolution could be performed in a branch and you
would be able to test out the change (eg performance benchmarks) before merging it. This provides great flexibility in
performing on-line table modifications and testing without interrupting downstream use cases. If the changes are
incorrect or not performant the branch can be dropped without being merged.

### Further use cases

Please see the [Nessie Documentation](https://projectnessie.org/features/) for further descriptions of 
Nessie features.

!!! danger
    Regular table maintenance in Iceberg is complicated when using nessie. Please consult
    [Management Services](https://projectnessie.org/features/management/) before performing any 
    [table maintenance](maintenance.md).


## Example 

Please have a look at the [Nessie Demos repo](https://github.com/projectnessie/nessie-demos)
for different examples of Nessie and Iceberg in action together.

## Future Improvements

* Iceberg multi-table transactions. Changes to multiple Iceberg tables in the same transaction, isolation levels etc
