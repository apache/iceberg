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

ARG BASE_IMAGE
FROM ${BASE_IMAGE}

COPY open-api/build/libs/iceberg-open-api-test-fixtures-runtime-*.jar /usr/lib/iceberg-rest/iceberg-rest-adapter.jar

ENV CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog
ENV CATALOG_URI=jdbc:sqlite::memory:
ENV CATALOG_JDBC_USER=user
ENV CATALOG_JDBC_PASSWORD=password
ENV REST_PORT=8181

HEALTHCHECK --retries=10 --interval=1s \
  CMD curl --fail http://localhost:$REST_PORT/v1/config || exit 1

EXPOSE $REST_PORT

COPY ${BASE_PATH}/rest/entrypoint.sh .
RUN chmod u+x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]