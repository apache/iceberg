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
package org.apache.iceberg.spark;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.Row;
import org.assertj.core.api.Assertions;

public class SparkTestHelperBase {
  protected static final Object ANY = new Object();

  protected List<Object[]> rowsToJava(List<Row> rows) {
    return rows.stream().map(this::toJava).collect(Collectors.toList());
  }

  private Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }

              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              } else {
                return value;
              }
            })
        .toArray(Object[]::new);
  }

  protected void assertEquals(
      String context, List<Object[]> expectedRows, List<Object[]> actualRows) {
    Assertions.assertThat(actualRows)
        .as(context + ": number of results should match")
        .hasSameSizeAs(expectedRows);
    for (int row = 0; row < expectedRows.size(); row += 1) {
      Object[] expected = expectedRows.get(row);
      Object[] actual = actualRows.get(row);
      Assertions.assertThat(actual).as("Number of columns should match").hasSameSizeAs(expected);
      for (int col = 0; col < actualRows.get(row).length; col += 1) {
        String newContext = String.format("%s: row %d col %d", context, row + 1, col + 1);
        assertEquals(newContext, expected, actual);
      }
    }
  }

  protected void assertEquals(String context, Object[] expectedRow, Object[] actualRow) {
    Assertions.assertThat(actualRow)
        .as("Number of columns should match")
        .hasSameSizeAs(expectedRow);
    for (int col = 0; col < actualRow.length; col += 1) {
      Object expectedValue = expectedRow[col];
      Object actualValue = actualRow[col];
      if (expectedValue != null && expectedValue.getClass().isArray()) {
        String newContext = String.format("%s (nested col %d)", context, col + 1);
        if (expectedValue instanceof byte[]) {
          Assertions.assertThat(actualValue).as(newContext).isEqualTo(expectedValue);
        } else {
          assertEquals(newContext, (Object[]) expectedValue, (Object[]) actualValue);
        }
      } else if (expectedValue != ANY) {
        Assertions.assertThat(actualValue)
            .as(context + " contents should match")
            .isEqualTo(expectedValue);
      }
    }
  }
}
