# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM python:3.9-bullseye

RUN apt-get -qq update && \
    apt-get -qq install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get -qq clean && \
    rm -rf /var/lib/apt/lists/*

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME} && mkdir -p /home/iceberg/spark-events
WORKDIR ${SPARK_HOME}

# Download spark
RUN curl -s https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz -o spark-3.3.1-bin-hadoop3.tgz \
 && tar xzf spark-3.3.1-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.3.1-bin-hadoop3.tgz

# Download iceberg spark runtime
RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.1.0/iceberg-spark-runtime-3.3_2.12-1.1.0.jar -Lo iceberg-spark-runtime-3.3_2.12-1.1.0.jar \
 && mv iceberg-spark-runtime-3.3_2.12-1.1.0.jar /opt/spark/jars

# Download Java AWS SDK
RUN curl -s https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.17.165/bundle-2.17.165.jar -Lo bundle-2.17.165.jar \
 && mv bundle-2.17.165.jar /opt/spark/jars

# Download URL connection client required for S3FileIO
RUN curl -s https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.17.165/url-connection-client-2.17.165.jar -Lo url-connection-client-2.17.165.jar \
 && mv url-connection-client-2.17.165.jar /opt/spark/jars

COPY spark-defaults.conf /opt/spark/conf
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

COPY entrypoint.sh .
COPY provision.py .

COPY data/ data/

ENTRYPOINT ["./entrypoint.sh"]
CMD ["notebook"]
