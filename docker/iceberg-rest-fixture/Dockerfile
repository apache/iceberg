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

FROM azul/zulu-openjdk:17-jre-headless

# Set up the user and group
RUN  set -xeu && \
     groupadd iceberg --gid 1000 && \
     useradd iceberg --uid 1000 --gid 1000 --create-home && \
     apt-get update && \
     apt-get install -y --no-install-recommends curl && \
     rm -rf /var/lib/apt/lists/*

# Working directory for the application
WORKDIR /usr/lib/iceberg-rest

# Copy the JAR file directly to the target location
COPY --chown=iceberg:iceberg open-api/build/libs/iceberg-open-api-test-fixtures-runtime-*.jar /usr/lib/iceberg-rest/iceberg-rest-adapter.jar

ENV CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog
ENV CATALOG_URI=jdbc:sqlite:/tmp/iceberg_catalog.db
ENV CATALOG_JDBC_USER=user
ENV CATALOG_JDBC_PASSWORD=password
ENV CATALOG_JDBC_STRICT__MODE=true
ENV REST_PORT=8181

# Healthcheck for the iceberg rest service
HEALTHCHECK --retries=10 --interval=1s \
  CMD curl --fail http://localhost:$REST_PORT/v1/config || exit 1

EXPOSE $REST_PORT
USER iceberg:iceberg
ENV LANG=en_US.UTF-8
CMD ["java", "-jar", "iceberg-rest-adapter.jar"]
