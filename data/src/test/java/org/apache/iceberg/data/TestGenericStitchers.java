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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.formats.Stitcher;
import org.apache.iceberg.formats.StitcherBuilder;
import org.apache.iceberg.formats.StitcherRegistry;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestGenericStitchers {
  private static final Types.NestedField ID = required(1, "id", Types.IntegerType.get());
  private static final Types.NestedField DATA = optional(2, "data", Types.StringType.get());
  private static final Types.NestedField POINT =
      optional(
          3,
          "point",
          Types.StructType.of(
              required(4, "x", Types.IntegerType.get()),
              required(5, "y", Types.IntegerType.get())));
  private static final Schema EXPECTED = new Schema(ID, DATA, POINT);
  private static final List<Schema> PARTS = List.of(new Schema(DATA), new Schema(ID, POINT));

  private final StitcherBuilder<Record> builder = StitcherRegistry.stitcherBuilder(Record.class);

  @Test
  void interleavesFieldsInProjectionOrder() {
    Record record = builder.build(EXPECTED, PARTS).stitch(parts(), 1);

    assertThat(record.struct()).isEqualTo(EXPECTED.asStruct());
    assertThat(record.size()).isEqualTo(3);
    assertThat(record.get(0, Integer.class)).isEqualTo(1);
    assertThat(record.get(1)).isEqualTo("a");
    assertThat(record.getField("point")).isEqualTo(point(1));
  }

  @Test
  void setsFields() {
    Record record = builder.build(EXPECTED, PARTS).stitch(parts(), 1);

    record.set(0, 2);
    record.setField("data", "b");

    assertThat(record.get(0)).isEqualTo(2);
    assertThat(record.getField("data")).isEqualTo("b");
  }

  @Test
  void copyIsIndependentOfParts() {
    List<Record> parts = parts();
    Record copy = builder.build(EXPECTED, PARTS).stitch(parts, 1).copy();

    parts.get(1).setField("id", 2);
    ((Record) parts.get(1).getField("point")).setField("x", 2);

    assertThat(copy)
        .isEqualTo(GenericRecord.create(EXPECTED).copy("id", 1, "data", "a", "point", point(1)));
  }

  @Test
  void doesNotRetainPartsList() {
    Stitcher<Record> stitcher = builder.build(EXPECTED, PARTS);
    List<Record> parts = parts();

    Record record = stitcher.stitch(parts, 1);
    parts.set(1, GenericRecord.create(PARTS.get(1)).copy("id", 2, "point", point(2)));

    assertThat(record.get(0)).isEqualTo(1);
  }

  private static List<Record> parts() {
    return Lists.newArrayList(
        GenericRecord.create(PARTS.get(0)).copy("data", "a"),
        GenericRecord.create(PARTS.get(1)).copy("id", 1, "point", point(1)));
  }

  private static Record point(int value) {
    return GenericRecord.create(POINT.type().asStructType()).copy("x", value, "y", value);
  }
}
