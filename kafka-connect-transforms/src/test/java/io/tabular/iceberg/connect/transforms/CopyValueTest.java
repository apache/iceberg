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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class CopyValueTest {

  @Test
  public void testCopyValueNull() {
    try (CopyValue<SinkRecord> smt = new CopyValue<>()) {
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);
      SinkRecord result = smt.apply(record);
      assertNull(result.value());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCopyValueSchemaless() {
    Map<String, String> props = Maps.newHashMap();
    props.put("source.field", "data");
    props.put("target.field", "data_copy");

    Map<String, Object> value = Maps.newHashMap();
    value.put("id", 123L);
    value.put("data", "foobar");

    try (CopyValue<SinkRecord> smt = new CopyValue<>()) {
      smt.configure(props);
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, value, 0);
      SinkRecord result = smt.apply(record);
      Map<String, Object> newValue = (Map<String, Object>) result.value();
      assertEquals(3, newValue.size());
      assertEquals("foobar", newValue.get("data_copy"));
    }
  }

  @Test
  public void testCopyValueWithSchema() {
    Map<String, String> props =
        ImmutableMap.of(
            "source.field", "data",
            "target.field", "data_copy");

    Schema schema =
        SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).field("data", Schema.STRING_SCHEMA);

    Struct value = new Struct(schema).put("id", 123L).put("data", "foobar");

    try (CopyValue<SinkRecord> smt = new CopyValue<>()) {
      smt.configure(props);
      SinkRecord record = new SinkRecord("topic", 0, null, null, schema, value, 0);
      SinkRecord result = smt.apply(record);

      Schema newSchema = result.valueSchema();
      assertEquals(3, newSchema.fields().size());
      assertEquals(Schema.STRING_SCHEMA, newSchema.field("data_copy").schema());

      Struct newValue = (Struct) result.value();
      assertEquals("foobar", newValue.get("data_copy"));
    }
  }
}
