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

RUN set -xeu && \
    apt-get update && \
    apt-get install -y --no-install-recommends curl wget bash python3 && \
    rm -rf /var/lib/apt/lists/*

ENV BASE_PATH=docker/iceberg-test-fixtures
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV SPARK_VERSION=4.0.0
ENV ICEBERG_SPARK_VERSION=v4.0

RUN mkdir -p ${SPARK_HOME} && mkdir -p /home/iceberg/spark-events

RUN curl --retry 5 -s -C - https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz \

COPY spark/${ICEBERG_SPARK_VERSION}/spark-runtime/build/libs/iceberg-spark-runtime-*.jar /opt/spark/jars/iceberg-spark-runtime.jar
COPY ${BASE_PATH}/spark-base/spark-defaults.conf /opt/spark/conf

ENV PATH="${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}"

RUN chmod u+x ${SPARK_HOME}/sbin/* && \
    chmod u+x ${SPARK_HOME}/bin/*

COPY ${BASE_PATH}/spark-base/entrypoint.sh .
COPY ${BASE_PATH}/spark-base/start-engine.sh .

RUN chmod u+x start-engine.sh
RUN chmod u+x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]