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
package org.apache.iceberg.connect.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.junit.jupiter.api.Test;

public class TestKafkaConnectHeadersGetter {

  private static final String TRACEPARENT =
      "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

  @Test
  public void testGetStringHeader() {
    Headers headers = new ConnectHeaders();
    headers.addString("traceparent", TRACEPARENT);

    assertThat(KafkaConnectHeadersGetter.INSTANCE.get(headers, "traceparent"))
        .isEqualTo(TRACEPARENT);
    assertThat(KafkaConnectHeadersGetter.INSTANCE.keys(headers)).contains("traceparent");
  }

  @Test
  public void testGetStringHeaderViaSchema() {
    Headers headers = new ConnectHeaders();
    headers.add("traceparent", TRACEPARENT, Schema.STRING_SCHEMA);

    assertThat(KafkaConnectHeadersGetter.INSTANCE.get(headers, "traceparent"))
        .isEqualTo(TRACEPARENT);
  }

  @Test
  public void testNullHeadersReturnsEmptyKeys() {
    assertThat(KafkaConnectHeadersGetter.INSTANCE.keys(null)).isEmpty();
    assertThat(KafkaConnectHeadersGetter.INSTANCE.get(null, "traceparent")).isNull();
  }
}
