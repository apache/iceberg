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
package org.apache.iceberg.connect.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class DmsTransformTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testDmsTransform() {
    try (DmsTransform<SinkRecord> smt = new DmsTransform<>()) {
      Map<String, Object> event = createDmsEvent("update");
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, event, 0);

      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Map.class);
      Map<String, Object> value = (Map<String, Object>) result.value();

      assertThat(value.get("account_id")).isEqualTo(1);

      Map<String, Object> cdcMetadata = (Map<String, Object>) value.get("_cdc");
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("source")).isEqualTo("db.tbl");
    }
  }

  @Test
  public void testDmsTransformNull() {
    try (DmsTransform<SinkRecord> smt = new DmsTransform<>()) {
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);
      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isNull();
    }
  }

  private Map<String, Object> createDmsEvent(String operation) {
    Map<String, Object> metadata =
        ImmutableMap.of(
            "timestamp", Instant.now().toString(),
            "record-type", "data",
            "operation", operation,
            "partition-key-type", "schema-table",
            "schema-name", "db",
            "table-name", "tbl",
            "transaction-id", 12345);

    Map<String, Object> data =
        ImmutableMap.of(
            "account_id", 1,
            "balance", 100,
            "last_updated", Instant.now().toString());

    return ImmutableMap.of(
        "metadata", metadata,
        "data", data);
  }
}
