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

# How to Verify a Release

Each Apache Iceberg release is validated by the community by holding a vote. A community release manager
will prepare a release candidate and call a vote on the Iceberg
[dev list](https://iceberg.apache.org/#community/#mailing-lists).
To validate the release candidate, community members will test it out in their downstream projects and environments.
It's recommended to report the Java, Scala, Spark, Flink and Hive versions you have tested against when you vote.

In addition to testing in downstream projects, community members also check the release's signatures, checksums, and
license documentation.

## Validating a source release candidate

Release announcements include links to the following:

- **A source tarball**
- **A signature (.asc)**
- **A checksum (.sha512)**
- **KEYS file**
- **GitHub change comparison**

After downloading the source tarball, signature, checksum, and KEYS file, here are instructions on how to
verify signatures, checksums, and documentation.

### Verifying Signatures

First, import the keys.
```bash
curl https://dist.apache.org/repos/dist/dev/iceberg/KEYS -o KEYS
gpg --import KEYS
```

Next, verify the `.asc` file.
```bash
gpg --verify apache-iceberg-{{ extra.versions.iceberg }}.tar.gz.asc
```

### Verifying Checksums

```bash
shasum -a 512 apache-iceberg-{{ extra.versions.iceberg }}.tar.gz.sha512
```

### Verifying License Documentation

Untar the archive and change into the source directory.
```bash
tar xzf apache-iceberg-{{ extra.versions.iceberg }}.tar.gz
cd apache-iceberg-{{ extra.versions.iceberg }}
```

Run RAT checks to validate license headers.
```bash
dev/check-license
```

### Verifying Build and Test

To verify that the release candidate builds properly, run the following command.
```bash
./gradlew build
```

## Testing release binaries

Release announcements will also include a maven repository location. You can use this
location to test downstream dependencies by adding it to your maven or gradle build.

To use the release in your maven build, add the following to your `POM` or `settings.xml`:
```xml
...
  <repositories>
    <repository>
      <id>iceberg-release-candidate</id>
      <name>Iceberg Release Candidate</name>
      <url>${MAVEN_URL}</url>
    </repository>
  </repositories>
...
```

To use the release in your gradle build, add the following to your `build.gradle`:
```groovy
repositories {
    mavenCentral()
    maven {
        url "${MAVEN_URL}"
    }
}
```

!!! Note
    Replace `${MAVEN_URL}` with the URL provided in the release announcement

### Verifying with Spark

To verify using spark, start a `spark-shell` with a command like the following command:
```bash
spark-shell \
    --conf spark.jars.repositories=${MAVEN_URL} \
    --packages org.apache.iceberg:iceberg-spark3-runtime:{{ extra.versions.iceberg }} \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=${LOCAL_WAREHOUSE_PATH} \
    --conf spark.sql.catalog.local.default-namespace=default \
    --conf spark.sql.defaultCatalog=local
```

### Verifying with Flink

To verify using Flink, start a Flink SQL Client with the following command:
```bash
wget ${MAVEN_URL}/iceberg-flink-runtime/{{ extra.versions.iceberg }}/iceberg-flink-runtime-{{ extra.versions.iceberg }}.jar

sql-client.sh embedded \
    -j iceberg-flink-runtime-{{ extra.versions.iceberg }}.jar \
    -j ${FLINK_CONNECTOR_PACKAGE}-${HIVE_VERSION}_${SCALA_VERSION}-${FLINK_VERSION}.jar \
    shell
```

## Voting

Votes are cast by replying to the release candidate announcement email on the dev mailing list
with either `+1`, `0`, or `-1`.

> [ ] +1 Release this as Apache Iceberg {{ extra.versions.iceberg }}
[ ] +0  
[ ] -1 Do not release this because...  

In addition to your vote, it's customary to specify if your vote is binding or non-binding. Only members
of the Project Management Committee have formally binding votes. If you're unsure, you can specify that your
vote is non-binding. To read more about voting in the Apache framework, checkout the
[Voting](https://www.apache.org/foundation/voting.html) information page on the Apache foundation's website.