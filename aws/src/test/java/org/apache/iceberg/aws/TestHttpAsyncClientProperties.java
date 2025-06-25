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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;

public class TestHttpAsyncClientProperties {

  @Test
  public void testNettyAsyncClientConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HttpAsyncClientProperties.NETTY_CONNECTION_TIMEOUT_MS, "200");
    properties.put(HttpAsyncClientProperties.NETTY_MAX_CONNECTIONS, "100");

    HttpAsyncClientProperties httpAsyncClientProperties = new HttpAsyncClientProperties(properties);
    S3AsyncClientBuilder s3AsyncClientBuilder = Mockito.mock(S3AsyncClientBuilder.class);

    httpAsyncClientProperties.applyHttpAsyncClientConfigurations(s3AsyncClientBuilder);

    Mockito.verify(s3AsyncClientBuilder).httpClientBuilder(Mockito.any());
  }

  @Test
  public void testCrtAsyncClientConfiguration() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HttpAsyncClientProperties.CRT_CONNECTION_TIMEOUT_MS, "200");
    properties.put(HttpAsyncClientProperties.CRT_MAX_CONCURRENCY, "50");

    HttpAsyncClientProperties httpAsyncClientProperties = new HttpAsyncClientProperties(properties);
    S3CrtAsyncClientBuilder s3CrtAsyncClientBuilder = Mockito.mock(S3CrtAsyncClientBuilder.class);

    httpAsyncClientProperties.applyHttpAsyncClientConfigurations(s3CrtAsyncClientBuilder);

    Mockito.verify(s3CrtAsyncClientBuilder).maxConcurrency(Mockito.any());
  }
}
