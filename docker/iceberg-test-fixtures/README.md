<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Iceberg Test Fixtures

For a standalone iceberg environment with bundled engine runtimes, Iceberg libraries, storage and catalogs services for 
testing and demo purposes.

## Build the Docker Images

When making changes to the local files and test them out, you can build the image locally:

```bash
# Build the project from iceberg root directory
./gradlew build -x test -x integrationTest

# Build the docker image
# ENGINE=[engine]-[version] STORAGE=[storage] CATALOG=[catalog]
make -f docker/iceberg-test-fixtures/Makefile ENGINE=spark-4.0.0 STORAGE=minio CATALOG=rest iceberg-engine-storage-catalog
```

## Interactive Session
```bash
docker run -d \
  --name apache/iceberg:latest-spark-4.0.0-minio-rest \
  iceberg-spark-minio-rest

docker exec -it iceberg-spark-minio-rest bash

# Run spark session
spark-sql --version
```

## Submit PySpark Script
```bash
docker cp provision.py iceberg-spark-minio-rest:/tmp/provision.py

docker exec -it iceberg-spark-minio-rest spark-submit /tmp/provision.py
```