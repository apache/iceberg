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

package org.apache.iceberg.spark.data;

import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;

interface SparkOrcValueWriter {

  /**
   * Take a value from the data and add it to the ORC output.
   *
   * @param rowId  the row id in the ColumnVector.
   * @param column the column number.
   * @param data   the data value to write.
   * @param output the ColumnVector to put the value into.
   */
  default void write(int rowId, int column, SpecializedGetters data, ColumnVector output) {
    if (data.isNullAt(column)) {
      output.noNulls = false;
      output.isNull[rowId] = true;
    } else {
      output.isNull[rowId] = false;
      nonNullWrite(rowId, column, data, output);
    }
  }

  void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output);

  /**
   * Returns a stream of {@link FieldMetrics} that this SparkOrcValueWriter keeps track of.
   * <p>
   * Since ORC keeps track of most metrics via column statistics, for now SparkOrcValueWriter only keeps track of NaN
   * counters, and only return non-empty stream if the writer writes double or float values either by itself or
   * transitively.
   */
  default Stream<FieldMetrics<?>> metrics() {
    return Stream.empty();
  }
}
