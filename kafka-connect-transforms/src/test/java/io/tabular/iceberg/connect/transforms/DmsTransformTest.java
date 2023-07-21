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
package io.tabular.iceberg.connect.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
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

      assertEquals(value.get("account_id"), 1);
      assertEquals(value.get("_cdc_table"), "db.tbl");
      assertEquals(value.get("_cdc_op"), "U");
    }
  }

  @Test
  public void testDmsTransformNull() {
    try (DmsTransform<SinkRecord> smt = new DmsTransform<>()) {
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);
      SinkRecord result = smt.apply(record);
      assertNull(result.value());
    }
  }

  private Map<String, Object> createDmsEvent(String operation) {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("timestamp", Instant.now().toString());
    metadata.put("record-type", "data");
    metadata.put("operation", operation);
    metadata.put("partition-key-type", "schema-table");
    metadata.put("schema-name", "db");
    metadata.put("table-name", "tbl");
    metadata.put("transaction-id", 12345);

    Map<String, Object> data = new HashMap<>();
    data.put("account_id", 1);
    data.put("balance", 100);
    data.put("last_updated", Instant.now().toString());

    Map<String, Object> event = new HashMap<>();
    event.put("metadata", metadata);
    event.put("data", data);

    return event;
  }
}
