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
package org.apache.iceberg.spark.source;

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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

class TestSparkStitchers {
  private static final Types.NestedField ID = required(1, "id", Types.IntegerType.get());
  private static final Types.NestedField DATA = optional(2, "data", Types.StringType.get());
  private static final Types.NestedField CATEGORY = optional(3, "category", Types.StringType.get());
  private static final Schema EXPECTED = new Schema(ID, DATA, CATEGORY);
  private static final List<Schema> PARTS = List.of(new Schema(DATA), new Schema(ID, CATEGORY));

  private final StitcherBuilder<InternalRow> builder =
      StitcherRegistry.stitcherBuilder(InternalRow.class);

  @Test
  void interleavesFieldsInProjectionOrder() {
    InternalRow row = builder.build(EXPECTED, PARTS).stitch(parts(), 1);

    assertThat(row.numFields()).isEqualTo(3);
    assertThat(row.getInt(0)).isEqualTo(1);
    assertThat(row.getUTF8String(1).toString()).isEqualTo("a");
    assertThat(row.getUTF8String(2).toString()).isEqualTo("x");
  }

  @Test
  void doesNotRetainPartsList() {
    Stitcher<InternalRow> stitcher = builder.build(EXPECTED, PARTS);
    List<InternalRow> parts = parts();

    InternalRow row = stitcher.stitch(parts, 1);
    parts.set(1, new GenericInternalRow(new Object[] {2, UTF8String.fromString("y")}));

    assertThat(row.getInt(0)).isEqualTo(1);
  }

  @Test
  void copyIsIndependentOfParts() {
    List<InternalRow> parts = parts();
    InternalRow copy = builder.build(EXPECTED, PARTS).stitch(parts, 1).copy();

    parts.get(1).update(0, 2);

    assertThat(copy.getInt(0)).isEqualTo(1);
  }

  private static List<InternalRow> parts() {
    return Lists.newArrayList(
        new GenericInternalRow(new Object[] {UTF8String.fromString("a")}),
        new GenericInternalRow(new Object[] {1, UTF8String.fromString("x")}));
  }
}
