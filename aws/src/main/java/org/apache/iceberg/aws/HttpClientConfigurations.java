/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.aws;

import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;

interface HttpClientConfigurations {

  <T extends AwsSyncClientBuilder> void applyConfigurations(T clientBuilder);

  default HttpClientConfigurations withConnectionTimeoutMs(Long connectionTimeoutMs) {
    throw new UnsupportedOperationException("Connection timeout is not supported");
  }

  default HttpClientConfigurations withSocketTimeoutMs(Long socketTimeoutMs) {
    throw new UnsupportedOperationException("Socket timeout is not supported");
  }

  default HttpClientConfigurations withConnectionAcquisitionTimeoutMs(Long acquisitionTimeoutMs) {
    throw new UnsupportedOperationException("Connection acquisition timeout is not supported");
  }

  default HttpClientConfigurations withConnectionMaxIdleTimeMs(Long connectionMaxIdleMs) {
    throw new UnsupportedOperationException("Connection max idle time is not supported");
  }

  default HttpClientConfigurations withConnectionTimeToLiveMs(Long timeToLiveMs) {
    throw new UnsupportedOperationException("Connection time to live is not supported");
  }

  default HttpClientConfigurations withExpectContinueEnabled(Boolean expectedContinueEnabled) {
    throw new UnsupportedOperationException("Expect continue is not supported");
  }

  default HttpClientConfigurations withMaxConnections(Integer maxConnections) {
    throw new UnsupportedOperationException("Max connections is not supported");
  }

  default HttpClientConfigurations withTcpKeepAliveEnabled(Boolean tcpKeepAlive) {
    throw new UnsupportedOperationException("TCP keep alive is not supported");
  }

  default HttpClientConfigurations withUseIdleConnectionReaperEnabled(
      Boolean useIdleConnectionReaper) {
    throw new UnsupportedOperationException("Use idle connection reaper is not supported");
  }
}
