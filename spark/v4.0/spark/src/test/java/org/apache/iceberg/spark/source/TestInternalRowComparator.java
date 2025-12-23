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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Comparator;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

public class TestInternalRowComparator {

  @Test
  public void testAscendingInteger() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    Comparator<InternalRow> comparator = new InternalRowComparator(sortOrder, sparkSchema, schema);

    InternalRow row1 = createRow(1);
    InternalRow row2 = createRow(2);
    InternalRow row3 = createRow(1);

    assertThat(comparator.compare(row1, row2)).isLessThan(0);
    assertThat(comparator.compare(row2, row1)).isGreaterThan(0);
    assertThat(comparator.compare(row1, row3)).isEqualTo(0);
  }

  @Test
  public void testDescendingInteger() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).desc("id").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    Comparator<InternalRow> comparator = new InternalRowComparator(sortOrder, sparkSchema, schema);

    InternalRow row1 = createRow(1);
    InternalRow row2 = createRow(2);

    // DESC: higher values come first
    assertThat(comparator.compare(row1, row2)).isGreaterThan(0);
    assertThat(comparator.compare(row2, row1)).isLessThan(0);
  }

  @Test
  public void testNullsFirst() {
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id", NullOrder.NULLS_FIRST).build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    Comparator<InternalRow> comparator = new InternalRowComparator(sortOrder, sparkSchema, schema);

    InternalRow nullRow = createRow(new Object[] {null});
    InternalRow row1 = createRow(1);

    // NULLS_FIRST: null < 1
    assertThat(comparator.compare(nullRow, row1)).isLessThan(0);
    assertThat(comparator.compare(row1, nullRow)).isGreaterThan(0);
  }

  @Test
  public void testNullsLast() {
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id", NullOrder.NULLS_LAST).build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    Comparator<InternalRow> comparator = new InternalRowComparator(sortOrder, sparkSchema, schema);

    InternalRow nullRow = createRow(new Object[] {null});
    InternalRow row1 = createRow(1);

    // NULLS_LAST: null > 1
    assertThat(comparator.compare(nullRow, row1)).isGreaterThan(0);
    assertThat(comparator.compare(row1, nullRow)).isLessThan(0);
  }

  @Test
  public void testMultipleFields() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").asc("data").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    Comparator<InternalRow> comparator = new InternalRowComparator(sortOrder, sparkSchema, schema);

    InternalRow row1 = createRow(1, "a");
    InternalRow row2 = createRow(1, "b");
    InternalRow row3 = createRow(2, "a");

    // Same id, compare by data
    assertThat(comparator.compare(row1, row2)).isLessThan(0);
    assertThat(comparator.compare(row2, row1)).isGreaterThan(0);

    // Different id, id takes precedence
    assertThat(comparator.compare(row1, row3)).isLessThan(0);
    assertThat(comparator.compare(row3, row1)).isGreaterThan(0);
  }

  @Test
  public void testMixedSortDirections() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").desc("data").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    Comparator<InternalRow> comparator = new InternalRowComparator(sortOrder, sparkSchema, schema);

    InternalRow row1 = createRow(1, "a");
    InternalRow row2 = createRow(1, "b");

    // Same id, data is DESC: "b" < "a"
    assertThat(comparator.compare(row1, row2)).isGreaterThan(0);
    assertThat(comparator.compare(row2, row1)).isLessThan(0);
  }

  @Test
  public void testLongType() {
    Schema schema = new Schema(Types.NestedField.required(1, "value", Types.LongType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("value").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    Comparator<InternalRow> comparator = new InternalRowComparator(sortOrder, sparkSchema, schema);

    InternalRow row1 = createRow(100L);
    InternalRow row2 = createRow(200L);

    assertThat(comparator.compare(row1, row2)).isLessThan(0);
    assertThat(comparator.compare(row2, row1)).isGreaterThan(0);
  }

  @Test
  public void testDoubleType() {
    Schema schema = new Schema(Types.NestedField.required(1, "value", Types.DoubleType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("value").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    Comparator<InternalRow> comparator = new InternalRowComparator(sortOrder, sparkSchema, schema);

    InternalRow row1 = createRow(1.5);
    InternalRow row2 = createRow(2.5);

    assertThat(comparator.compare(row1, row2)).isLessThan(0);
    assertThat(comparator.compare(row2, row1)).isGreaterThan(0);
  }

  @Test
  public void testStringType() {
    Schema schema = new Schema(Types.NestedField.required(1, "name", Types.StringType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("name").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    Comparator<InternalRow> comparator = new InternalRowComparator(sortOrder, sparkSchema, schema);

    InternalRow row1 = createRow("alice");
    InternalRow row2 = createRow("bob");

    assertThat(comparator.compare(row1, row2)).isLessThan(0);
    assertThat(comparator.compare(row2, row1)).isGreaterThan(0);
  }

  @Test
  public void testBothNulls() {
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    Comparator<InternalRow> comparator = new InternalRowComparator(sortOrder, sparkSchema, schema);

    InternalRow nullRow1 = createRow(new Object[] {null});
    InternalRow nullRow2 = createRow(new Object[] {null});

    // Both null, should be equal
    assertThat(comparator.compare(nullRow1, nullRow2)).isEqualTo(0);
  }

  @Test
  public void testUnsortedOrderThrows() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SortOrder unsortedOrder = SortOrder.unsorted();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    assertThatThrownBy(() -> new InternalRowComparator(unsortedOrder, sparkSchema, schema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot create comparator for unsorted order");
  }

  // Helper methods to create test rows
  private InternalRow createRow(Object... values) {
    GenericInternalRow row = new GenericInternalRow(values.length);
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        row.setNullAt(i);
      } else if (values[i] instanceof Integer) {
        row.setInt(i, (Integer) values[i]);
      } else if (values[i] instanceof Long) {
        row.setLong(i, (Long) values[i]);
      } else if (values[i] instanceof Double) {
        row.setDouble(i, (Double) values[i]);
      } else if (values[i] instanceof String) {
        row.update(i, UTF8String.fromString((String) values[i]));
      } else {
        throw new IllegalArgumentException("Unsupported type: " + values[i].getClass());
      }
    }
    return row;
  }
}
