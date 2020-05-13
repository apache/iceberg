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

# Getting Started

## Using Iceberg in Spark

The latest version of Iceberg is [0.8.0-incubating](../releases).

To use Iceberg in a Spark shell, use the `--packages` option:

```sh
spark-shell --packages org.apache.iceberg:iceberg-spark-runtime:0.8.0-incubating
```

You can also build Iceberg locally, and add the jar using `--jars`. This can be helpful to test unreleased features or while developing something new:

```sh
./gradlew assemble
spark-shell --jars spark-runtime/build/libs/iceberg-spark-runtime-8c05a2f.jar
```

## Installing with Spark

If you want to include Iceberg in your Spark installation, add the [`iceberg-spark-runtime` Jar][spark-runtime-jar] to Spark's `jars` folder.

Where you have to replace `8c05a2f` with the git hash that you're using.

[spark-runtime-jar]: https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.8.0-incubating/iceberg-spark-runtime-0.8.0-incubating.jar

## Creating a table

Spark 2.4 is limited to reading and writing existing Iceberg tables. Use the [Iceberg API](../api) to create Iceberg tables.

Here's how to create your first Iceberg table in Spark, using a source Dataset

First, import Iceberg classes and create a catalog client:

```scala
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.spark.SparkSchemaUtil

val catalog = new HiveCatalog(spark.sparkContext.hadoopConfiguration)
```

Next, create a dataset to write into your table and get an Iceberg schema for it:

```scala
val data = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
val schema = SparkSchemaUtil.convert(data.schema)
```

Finally, create a table using the schema:

```scala
val name = TableIdentifier.of("default", "test_table")
val table = catalog.createTable(name, schema)
```

### Reading and writing

Once your table is created, you can use it in `load` and `save` in Spark 2.4:

```scala
// write the dataset to the table
data.write.format("iceberg").mode("append").save("default.test_table")

// read the table
spark.read.format("iceberg").load("default.test_table")
```

### Reading with SQL

You can also create a temporary view to use the table in SQL:

```scala
spark.read.format("iceberg").load("default.test_table").createOrReplaceTempView("test_table")
spark.sql("""SELECT count(1) FROM test_table""")
```

### Next steps

Next, you can learn more about the [Iceberg Table API](../api), or about [Iceberg tables in Spark](../spark)
