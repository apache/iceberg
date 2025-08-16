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

ENV MINIO_HOME=${MINIO_HOME:-"/opt/minio"}

RUN mkdir -p ${MINIO_HOME}

RUN wget https://dl.min.io/server/minio/release/linux-amd64/minio && \
    chmod u+x minio && \
    mv minio ${MINIO_HOME}/

RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod u+x mc && \
    mv mc ${MINIO_HOME}/

ENV PATH="${MINIO_HOME}:${PATH}"

ENV AWS_ACCESS_KEY_ID=admin
ENV AWS_SECRET_ACCESS_KEY=password
ENV AWS_REGION=us-east-1

ENV CATALOG_WAREHOUSE=s3://warehouse/
ENV CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
ENV CATALOG_S3_ENDPOINT=http://localhost:9000

COPY aws-bundle/build/libs/iceberg-aws-bundle-*.jar /opt/spark/jars/iceberg-aws.jar
COPY ${BASE_PATH}/minio/start-storage.sh .
COPY ${BASE_PATH}/minio/entrypoint.sh .

RUN chmod u+x start-storage.sh
RUN chmod u+x entrypoint.sh

# MinIO port
EXPOSE 9000
# MinIO client port
EXPOSE 9001

ENTRYPOINT ["./entrypoint.sh"]