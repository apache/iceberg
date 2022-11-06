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
package org.apache.iceberg;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;

public class MetricsUtil {

  private MetricsUtil() {}

  /**
   * Construct mapping relationship between column id to NaN value counts from input metrics and
   * metrics config.
   */
  public static Map<Integer, Long> createNanValueCounts(
      Stream<FieldMetrics<?>> fieldMetrics, MetricsConfig metricsConfig, Schema inputSchema) {
    if (fieldMetrics == null || inputSchema == null) {
      return Maps.newHashMap();
    }

    return createNanValueCounts(fieldMetrics, metricsConfig, TypeUtil.indexNameById(inputSchema.asStruct()));
  }


  public static Map<Integer, Long> createNanValueCounts(
      Stream<FieldMetrics<?>> fieldMetrics, MetricsConfig metricsConfig, Map<Integer, String> idToColumn) {
    Preconditions.checkNotNull(metricsConfig, "metricsConfig is required");

    if (fieldMetrics == null || idToColumn == null || idToColumn.isEmpty()) {
      return Maps.newHashMap();
    }

    return fieldMetrics
        .filter(metrics -> metricsMode(idToColumn, metricsConfig, metrics.id()) != MetricsModes.None.get())
        .collect(Collectors.toMap(FieldMetrics::id, FieldMetrics::nanValueCount));
  }

  /** Extract MetricsMode for the given field id from metrics config. */
  public static MetricsModes.MetricsMode metricsMode(
      Schema inputSchema, MetricsConfig metricsConfig, int fieldId) {
    Preconditions.checkNotNull(inputSchema, "inputSchema is required");
    Preconditions.checkNotNull(metricsConfig, "metricsConfig is required");

    String columnName = inputSchema.findColumnName(fieldId);
    return metricsConfig.columnMode(columnName);
  }

  public static MetricsModes.MetricsMode metricsMode(Map<Integer, String> idToColumn, MetricsConfig metricsConfig, int fieldId) {
    Preconditions.checkNotNull(idToColumn, "inputSchema is required");
    Preconditions.checkNotNull(metricsConfig, "metricsConfig is required");

    return metricsConfig.columnMode(idToColumn.get(fieldId));
  }
}
