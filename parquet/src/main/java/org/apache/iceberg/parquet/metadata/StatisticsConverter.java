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
package org.apache.iceberg.parquet.metadata;

import java.util.Arrays;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public final class StatisticsConverter {

  private StatisticsConverter() {}

  @SuppressWarnings("CyclomaticComplexity")
  public static Statistics<?> fromParquetStatistics(
      org.apache.parquet.format.Statistics statistics, PrimitiveType type) {
    Statistics.Builder statsBuilder = Statistics.getBuilderForReading(type);
    if (statistics != null) {
      if (statistics.isSetMin_value() && statistics.isSetMax_value()) {
        byte[] min = statistics.min_value.array();
        byte[] max = statistics.max_value.array();
        if (isMinMaxStatsSupported(type) || Arrays.equals(min, max)) {
          statsBuilder.withMin(min);
          statsBuilder.withMax(max);
        }
      } else {
        boolean isSet = statistics.isSetMax() && statistics.isSetMin();
        boolean maxEqualsMin = isSet && Arrays.equals(statistics.getMin(), statistics.getMax());
        boolean sortOrdersMatch =
            org.apache.parquet.schema.Type.Repetition.REQUIRED == type.getRepetition()
                || isSigned(type.getPrimitiveTypeName());
        if (isSet && (sortOrdersMatch || maxEqualsMin)) {
          statsBuilder.withMin(statistics.min.array());
          statsBuilder.withMax(statistics.max.array());
        }
      }

      if (statistics.isSetNull_count()) {
        statsBuilder.withNumNulls(statistics.null_count);
      }
    }
    return statsBuilder.build();
  }

  private static boolean isMinMaxStatsSupported(PrimitiveType type) {
    PrimitiveTypeName typeName = type.getPrimitiveTypeName();
    return typeName != PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
        && typeName != PrimitiveTypeName.INT96;
  }

  private static boolean isSigned(PrimitiveTypeName typeName) {
    switch (typeName) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
        return true;
      default:
        return false;
    }
  }
}
