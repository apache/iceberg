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
package org.apache.iceberg.spark.data.vectorized;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.VariantType$;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.Test;

class TestColumnVectorWithFilter {

  @Test
  void intervalColumnVectorRemapsRows() {
    try (WritableColumnVector delegate =
        new OnHeapColumnVector(5, DataTypes.CalendarIntervalType)) {
      for (int rowId = 0; rowId < 5; rowId++) {
        delegate.putInterval(rowId, new CalendarInterval(rowId, rowId * 10, rowId * 100L));
      }

      ColumnVector filtered = new ColumnVectorWithFilter(delegate, new int[] {1, 3});

      assertInterval(filtered.getInterval(0), 1, 10, 100L);
      assertInterval(filtered.getInterval(1), 3, 30, 300L);
    }
  }

  @Test
  void variantColumnVectorRemapsRows() {
    try (WritableColumnVector delegate = new OnHeapColumnVector(5, VariantType$.MODULE$)) {
      WritableColumnVector values = (WritableColumnVector) delegate.getChild(0);
      WritableColumnVector metadata = (WritableColumnVector) delegate.getChild(1);
      for (int rowId = 0; rowId < 5; rowId++) {
        values.putByteArray(rowId, bytes("value-" + rowId));
        metadata.putByteArray(rowId, bytes("metadata-" + rowId));
      }

      ColumnVector filtered = new ColumnVectorWithFilter(delegate, new int[] {1, 3});

      assertVariant(filtered.getVariant(0), "value-1", "metadata-1");
      assertVariant(filtered.getVariant(1), "value-3", "metadata-3");
    }
  }

  @Test
  void arrayChildVectorUsesElementIndexes() {
    try (WritableColumnVector delegate =
        new OnHeapColumnVector(6, DataTypes.createArrayType(DataTypes.IntegerType))) {
      WritableColumnVector elements = (WritableColumnVector) delegate.getChild(0);
      for (int elementId = 0; elementId < 6; elementId++) {
        elements.putInt(elementId, (elementId + 1) * 10);
      }

      delegate.putArray(0, 0, 2);
      delegate.putArray(1, 2, 1);
      delegate.putArray(2, 3, 3);

      ColumnVector filtered = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      assertThat(filtered.getChild(0).getInt(1)).isEqualTo(20);
    }
  }

  @Test
  void mapChildVectorsUseElementIndexes() {
    try (WritableColumnVector delegate =
        new OnHeapColumnVector(
            3, DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType))) {
      WritableColumnVector keys = (WritableColumnVector) delegate.getChild(0);
      WritableColumnVector values = (WritableColumnVector) delegate.getChild(1);
      keys.putByteArray(0, bytes("a"));
      keys.putByteArray(1, bytes("b"));
      keys.putByteArray(2, bytes("c"));
      values.putInt(0, 1);
      values.putInt(1, 2);
      values.putInt(2, 3);

      delegate.putArray(0, 0, 1);
      delegate.putArray(1, 1, 1);
      delegate.putArray(2, 2, 1);

      ColumnVector filtered = new ColumnVectorWithFilter(delegate, new int[] {0, 2});

      assertThat(filtered.getChild(0).getUTF8String(1).toString()).isEqualTo("b");
      assertThat(filtered.getChild(1).getInt(1)).isEqualTo(2);
    }
  }

  private static void assertInterval(
      CalendarInterval interval, int months, int days, long microseconds) {
    assertThat(interval.months).isEqualTo(months);
    assertThat(interval.days).isEqualTo(days);
    assertThat(interval.microseconds).isEqualTo(microseconds);
  }

  private static void assertVariant(VariantVal variant, String value, String metadata) {
    assertThat(variant.getValue()).isEqualTo(bytes(value));
    assertThat(variant.getMetadata()).isEqualTo(bytes(metadata));
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
