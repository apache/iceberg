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
package org.apache.iceberg.mr.hive;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.HiveVersion;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergObjectInspector;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestDeserializer {
  private static final Schema CUSTOMER_SCHEMA =
      new Schema(
          optional(1, "customer_id", Types.LongType.get()),
          optional(2, "first_name", Types.StringType.get()));

  private static final StandardStructObjectInspector CUSTOMER_OBJECT_INSPECTOR =
      ObjectInspectorFactory.getStandardStructObjectInspector(
          Arrays.asList("customer_id", "first_name"),
          Arrays.asList(
              PrimitiveObjectInspectorFactory.writableLongObjectInspector,
              PrimitiveObjectInspectorFactory.writableStringObjectInspector));

  @Test
  public void testSchemaDeserialize() {
    StandardStructObjectInspector schemaObjectInspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            Arrays.asList("0:col1", "1:col2"),
            Arrays.asList(
                PrimitiveObjectInspectorFactory.writableLongObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector));

    Deserializer deserializer =
        new Deserializer.Builder()
            .schema(CUSTOMER_SCHEMA)
            .writerInspector((StructObjectInspector) IcebergObjectInspector.create(CUSTOMER_SCHEMA))
            .sourceInspector(schemaObjectInspector)
            .build();

    Record expected = GenericRecord.create(CUSTOMER_SCHEMA);
    expected.set(0, 1L);
    expected.set(1, "Bob");

    Record actual = deserializer.deserialize(new Object[] {new LongWritable(1L), new Text("Bob")});

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testStructDeserialize() {
    Deserializer deserializer =
        new Deserializer.Builder()
            .schema(CUSTOMER_SCHEMA)
            .writerInspector((StructObjectInspector) IcebergObjectInspector.create(CUSTOMER_SCHEMA))
            .sourceInspector(CUSTOMER_OBJECT_INSPECTOR)
            .build();

    Record expected = GenericRecord.create(CUSTOMER_SCHEMA);
    expected.set(0, 1L);
    expected.set(1, "Bob");

    Record actual = deserializer.deserialize(new Object[] {new LongWritable(1L), new Text("Bob")});

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testMapDeserialize() {
    Schema schema =
        new Schema(
            optional(
                1,
                "map_type",
                Types.MapType.ofOptional(2, 3, Types.LongType.get(), Types.StringType.get())));

    StructObjectInspector inspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            Arrays.asList("map_type"),
            Arrays.asList(
                ObjectInspectorFactory.getStandardMapObjectInspector(
                    PrimitiveObjectInspectorFactory.writableLongObjectInspector,
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector)));

    Deserializer deserializer =
        new Deserializer.Builder()
            .schema(schema)
            .writerInspector((StructObjectInspector) IcebergObjectInspector.create(schema))
            .sourceInspector(inspector)
            .build();

    Record expected = GenericRecord.create(schema);
    expected.set(0, Collections.singletonMap(1L, "Taylor"));

    MapWritable map = new MapWritable();
    map.put(new LongWritable(1L), new Text("Taylor"));
    Object[] data = new Object[] {map};
    Record actual = deserializer.deserialize(data);

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testListDeserialize() {
    Schema schema =
        new Schema(optional(1, "list_type", Types.ListType.ofOptional(2, Types.LongType.get())));

    StructObjectInspector inspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            Arrays.asList("list_type"),
            Arrays.asList(
                ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.writableLongObjectInspector)));

    Deserializer deserializer =
        new Deserializer.Builder()
            .schema(schema)
            .writerInspector((StructObjectInspector) IcebergObjectInspector.create(schema))
            .sourceInspector(inspector)
            .build();

    Record expected = GenericRecord.create(schema);
    expected.set(0, Collections.singletonList(1L));

    Object[] data = new Object[] {new Object[] {new LongWritable(1L)}};
    Record actual = deserializer.deserialize(data);

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testDeserializeEverySupportedType() {
    assumeThat(HiveVersion.min(HiveVersion.HIVE_3))
        .as("No test yet for Hive3 (Date/Timestamp creation)");

    Deserializer deserializer =
        new Deserializer.Builder()
            .schema(HiveIcebergTestUtils.FULL_SCHEMA)
            .writerInspector(
                (StructObjectInspector)
                    IcebergObjectInspector.create(HiveIcebergTestUtils.FULL_SCHEMA))
            .sourceInspector(HiveIcebergTestUtils.FULL_SCHEMA_OBJECT_INSPECTOR)
            .build();

    Record expected = HiveIcebergTestUtils.getTestRecord();
    Record actual = deserializer.deserialize(HiveIcebergTestUtils.valuesForTestRecord(expected));

    HiveIcebergTestUtils.assertEquals(expected, actual);
  }

  @Test
  public void testNullDeserialize() {
    Deserializer deserializer =
        new Deserializer.Builder()
            .schema(HiveIcebergTestUtils.FULL_SCHEMA)
            .writerInspector(
                (StructObjectInspector)
                    IcebergObjectInspector.create(HiveIcebergTestUtils.FULL_SCHEMA))
            .sourceInspector(HiveIcebergTestUtils.FULL_SCHEMA_OBJECT_INSPECTOR)
            .build();

    Record expected = HiveIcebergTestUtils.getNullTestRecord();

    Object[] nulls = new Object[HiveIcebergTestUtils.FULL_SCHEMA.columns().size()];
    Arrays.fill(nulls, null);

    Record actual = deserializer.deserialize(nulls);

    assertThat(actual).isEqualTo(expected);

    // Check null record as well
    assertThat(deserializer.deserialize(null)).isNull();
  }
}
