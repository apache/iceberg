#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

SCALA_VERSION=2.12
FLINK_VERSIONS=1.14,1.15,1.16
SPARK_VERSIONS=2.4,3.1,3.2,3.3
HIVE_VERSIONS=2,3

./gradlew -Prelease -DscalaVersion=$SCALA_VERSION -DflinkVersions=$FLINK_VERSIONS -DsparkVersions=$SPARK_VERSIONS -DhiveVersions=$HIVE_VERSIONS publishApachePublicationToMavenRepository

# Also publish Scala 2.13 Artifacts for versions that support it.
# Flink does not yet support 2.13 (and is largely dropping a user-facing dependency on Scala). Hive doesn't need a Scala specification.
./gradlew -Prelease -DscalaVersion=2.13 -DsparkVersions=3.2 :iceberg-spark:iceberg-spark-3.2_2.13:publishApachePublicationToMavenRepository :iceberg-spark:iceberg-spark-extensions-3.2_2.13:publishApachePublicationToMavenRepository :iceberg-spark:iceberg-spark-runtime-3.2_2.13:publishApachePublicationToMavenRepository
./gradlew -Prelease -DscalaVersion=2.13 -DsparkVersions=3.3 :iceberg-spark:iceberg-spark-3.3_2.13:publishApachePublicationToMavenRepository :iceberg-spark:iceberg-spark-extensions-3.3_2.13:publishApachePublicationToMavenRepository :iceberg-spark:iceberg-spark-runtime-3.3_2.13:publishApachePublicationToMavenRepository

