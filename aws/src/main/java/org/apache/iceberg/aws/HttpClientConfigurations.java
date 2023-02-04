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

import java.util.Map;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;

interface HttpClientConfigurations {
  String CONNECTION_TIMEOUT_MS = "connection-timeout-ms";
  String SOCKET_TIMEOUT_MS = "socket-timeout-ms";
  String CONNECTION_ACQUISITION_TIMEOUT_MS = "connection-acquisition-timeout-ms";
  String CONNECTION_MAX_IDLE_TIME_MS = "connection-max-idle-time-ms";
  String CONNECTION_TIME_TO_LIVE_MS = "connection-time-to-live-ms";
  String EXPECT_CONTINUE_ENABLED = "expect-continue-enabled";
  String MAX_CONNECTIONS = "max-connections";
  String TCP_KEEP_ALIVE_ENABLED = "tcp-keep-alive-enabled";
  String USE_IDLE_CONNECTION_REAPER_ENABLED = "use-idle-connection-reaper-enabled";

  /**
   * Initialize the configurations from the given properties
   *
   * @param httpClientProperties properties for http client
   */
  void initialize(Map<String, String> httpClientProperties);

  /**
   * Configure http client builder for a given AWS client builder
   *
   * @param awsClientBuilder AWS client builder
   */
  <T extends AwsSyncClientBuilder> void configureHttpClientBuilder(T awsClientBuilder);
}
