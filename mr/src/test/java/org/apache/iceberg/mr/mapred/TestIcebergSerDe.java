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

package org.apache.iceberg.mr.mapred;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestIcebergSerDe {

  @Test
  public void testDeserializeWritable() {
    Schema schema = new Schema(required(1, "string_type", Types.StringType.get()),
        required(2, "int_type", Types.IntegerType.get()),
        required(3, "long_type", Types.LongType.get()),
        required(4, "boolean_type", Types.BooleanType.get()),
        required(5, "float_type", Types.FloatType.get()),
        required(6, "double_type", Types.DoubleType.get()),
        required(7, "binary_type", Types.BinaryType.get()),
        required(8, "date_type", Types.DateType.get()),
        required(9, "timestamp_with_zone_type", Types.TimestampType.withZone()),
        required(10, "timestamp_without_zone_type", Types.TimestampType.withoutZone()),
        required(11, "map_type", Types.MapType
            .ofRequired(12, 13, Types.IntegerType.get(), Types.StringType.get())),
        required(14, "list_type", Types.ListType.ofRequired(15, Types.LongType.get()))
    );
    Map<String, String> expected = ImmutableMap.of("foo", "bar");

    Record record = TestHelpers.createCustomRecord(schema, expected);
    IcebergWritable writable = new IcebergWritable();
    writable.wrapRecord(record);
    writable.wrapSchema(schema);

    IcebergSerDe serDe = new IcebergSerDe();
    List<Object> deserialized = (List<Object>) serDe.deserialize(writable);
    Map result = (Map) deserialized.get(0);

    assertEquals(expected, result);
  }

  @Test
  public void testDeserializeMapWithIntKeyType() {
    Schema schema = new Schema(required(1, "map_type", Types.MapType
        .ofRequired(18, 19, Types.IntegerType.get(), Types.StringType.get())));
    Map<Integer, String> expected = ImmutableMap.of(22, "bar");

    Record record = TestHelpers.createCustomRecord(schema, expected);
    IcebergWritable writable = new IcebergWritable();
    writable.wrapRecord(record);
    writable.wrapSchema(schema);

    IcebergSerDe serDe = new IcebergSerDe();
    List<Object> deserialized = (List<Object>) serDe.deserialize(writable);
    Map result = (Map) deserialized.get(0);

    assertEquals(expected, result);
  }

  @Test
  public void testDeserializeList() {
    Schema schema = new Schema(required(1, "list_type", Types.ListType.ofRequired(17, Types.LongType.get())));
    List<Long> expected = Arrays.asList(1000L, 2000L, 3000L);

    Record record = TestHelpers.createCustomRecord(schema, expected);
    IcebergWritable writable = new IcebergWritable();
    writable.wrapRecord(record);
    writable.wrapSchema(schema);

    IcebergSerDe serDe = new IcebergSerDe();
    List<Object> deserialized = (List<Object>) serDe.deserialize(writable);
    List result = (List) deserialized.get(0);

    assertEquals(expected, result);
  }

  @Test
  public void testDeserializePrimitives() {
    Schema schema = new Schema(required(1, "string_type", Types.StringType.get()),
        required(2, "int_type", Types.IntegerType.get()),
        required(3, "long_type", Types.LongType.get()),
        required(4, "boolean_type", Types.BooleanType.get()),
        required(5, "float_type", Types.FloatType.get()),
        required(6, "double_type", Types.DoubleType.get()),
        required(7, "date_type", Types.DateType.get()));

    List<?> expected = Arrays.asList("foo", 12, 3000L, true, 3.01F, 3.0D, "1998-11-13");

    Record record = TestHelpers.createCustomRecord(schema, "foo", 12, 3000L, true, 3.01F, 3.0D, "1998-11-13");
    IcebergWritable writable = new IcebergWritable();
    writable.wrapRecord(record);
    writable.wrapSchema(schema);

    IcebergSerDe serDe = new IcebergSerDe();
    List<Object> result = (List<Object>) serDe.deserialize(writable);

    assertEquals(expected, result);
  }

  @Test
  public void testDeserializeNestedList() {
    Schema schema = new Schema(required(1, "map_type", Types.MapType
        .ofRequired(18, 19, Types.StringType.get(), Types.ListType.ofRequired(17, Types.LongType.get()))));
    Map<String, List> expected = ImmutableMap.of("foo", Arrays.asList(1000L, 2000L, 3000L));

    Record record = TestHelpers.createCustomRecord(schema, expected);
    IcebergWritable writable = new IcebergWritable();
    writable.wrapRecord(record);
    writable.wrapSchema(schema);

    IcebergSerDe serDe = new IcebergSerDe();
    List<Object> deserialized = (List<Object>) serDe.deserialize(writable);
    Map result = (Map) deserialized.get(0);

    assertEquals(expected, result);
    assertTrue(result.containsKey("foo"));
    assertTrue(result.containsValue(Arrays.asList(1000L, 2000L, 3000L)));
  }
}
