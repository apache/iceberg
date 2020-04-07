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

## Including Iceberg 

### Downloads

The latest version of Iceberg is [0.7.0-incubating](https://github.com/apache/incubator-iceberg/releases/tag/apache-iceberg-0.7.0-incubating).

* [0.7.0-incubating source tar.gz](https://www.apache.org/dyn/closer.cgi/incubator/iceberg/apache-iceberg-0.7.0-incubating/apache-iceberg-0.7.0-incubating.tar.gz) -- [signature](https://dist.apache.org/repos/dist/release/incubator/iceberg/apache-iceberg-0.7.0-incubating/apache-iceberg-0.7.0-incubating.tar.gz.asc) -- [sha512](https://dist.apache.org/repos/dist/release/incubator/iceberg/apache-iceberg-0.7.0-incubating/apache-iceberg-0.7.0-incubating.tar.gz.sha512)
* [0.7.0-incubating Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.7.0-incubating/iceberg-spark-runtime-0.7.0-incubating.jar)

One way to use Iceberg in Spark 2.4 is to download the runtime Jar and add it to the jars folder of your Spark install.

Spark 2.4 is limited to reading and writing existing Iceberg tables. Use the [Iceberg API](../api) to create Iceberg tables.

The recommended way is to include Iceberg's latest release using the `--packages` option:
```sh
spark-shell --packages org.apache.iceberg:iceberg-spark-runtime:0.7.0-incubating
```

You can also build Iceberg locally, and add the jar to Spark's classpath. This can be helpful to test unreleased features or while developing something new:

```sh
./gradlew assemble

spark-shell --jars spark-runtime/build/libs/iceberg-spark-runtime-93990904.jar
```

Where you have to replace `93990904` with the git hash that you're using.

### Gradle
To add a dependency on Iceberg in Gradle, add the following to `build.gradle`:
```
dependencies {
  compile 'org.apache.iceberg:iceberg-core:0.7.0-incubating'
}
```

### Maven 
If you'd like to try out Iceberg in a Maven project using the Spark Iceberg API, you can add the `iceberg-spark-runtime` dependency to your `pom.xml` file:
```xml
   <dependency>
     <groupId>org.apache.iceberg</groupId>
     <artifactId>iceberg-spark-runtime</artifactId>
     <version>${iceberg.version}</version>
   </dependency>
```

You'll also need `spark-sql` to read tables:
```xml
  <dependency> 
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.11</artifactId>
    <version>2.4.4</version>
  </dependency>
```

### Using the API 
For examples on how to use the Iceberg API see:

- [Spark](api-quickstart.md)
- [Java](java-api-quickstart.md)
