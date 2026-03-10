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
package org.apache.iceberg.lance;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

/**
 * Utility class to compute Iceberg {@link Metrics} from Lance data.
 *
 * <p>Collects statistics such as row count, column sizes, value counts, null counts, and
 * lower/upper bounds from written data for use in Iceberg metadata.
 */
public class LanceMetrics {

  private LanceMetrics() {}

  /**
   * Build Metrics from collected statistics.
   *
   * @param rowCount the number of rows written
   * @param schema the Iceberg schema of the data
   * @param columnSizes a map of field ID to column byte size
   * @param valueCounts a map of field ID to value count
   * @param nullCounts a map of field ID to null value count
   * @param lowerBounds a map of field ID to lower bound value
   * @param upperBounds a map of field ID to upper bound value
   * @return an Iceberg Metrics object
   */
  public static Metrics createMetrics(
      long rowCount,
      Schema schema,
      Map<Integer, Long> columnSizes,
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullCounts,
      Map<Integer, Object> lowerBounds,
      Map<Integer, Object> upperBounds) {

    Map<Integer, ByteBuffer> lowerBoundBytes = toByteBufferMap(schema, lowerBounds);
    Map<Integer, ByteBuffer> upperBoundBytes = toByteBufferMap(schema, upperBounds);

    return new Metrics(
        rowCount, columnSizes, valueCounts, nullCounts, null, lowerBoundBytes, upperBoundBytes);
  }

  /**
   * Create a simple Metrics with only the row count.
   *
   * @param rowCount the number of rows written
   * @return an Iceberg Metrics object with row count only
   */
  public static Metrics createMetrics(long rowCount) {
    return new Metrics(rowCount);
  }

  /**
   * Convert a map of typed lower/upper bounds to ByteBuffer representations required by Iceberg.
   */
  private static Map<Integer, ByteBuffer> toByteBufferMap(
      Schema schema, Map<Integer, Object> boundsMap) {
    if (boundsMap == null) {
      return null;
    }
    Map<Integer, ByteBuffer> result = Maps.newHashMapWithExpectedSize(boundsMap.size());
    for (Map.Entry<Integer, Object> entry : boundsMap.entrySet()) {
      int fieldId = entry.getKey();
      Object value = entry.getValue();
      if (value != null) {
        Types.NestedField field = schema.findField(fieldId);
        if (field != null) {
          result.put(fieldId, Conversions.toByteBuffer(field.type(), value));
        }
      }
    }
    return result;
  }

  /**
   * A mutable metrics collector that accumulates statistics during data writing.
   */
  public static class MetricsCollector {
    private long rowCount = 0;
    private final Map<Integer, Long> columnSizes = Maps.newHashMap();
    private final Map<Integer, Long> valueCounts = Maps.newHashMap();
    private final Map<Integer, Long> nullCounts = Maps.newHashMap();
    private final Map<Integer, Object> lowerBounds = Maps.newHashMap();
    private final Map<Integer, Object> upperBounds = Maps.newHashMap();

    /** Increment row count. */
    public void incrementRowCount() {
      rowCount++;
    }

    /** Get current row count. */
    public long rowCount() {
      return rowCount;
    }

    /**
     * Update column size for a given field.
     *
     * @param fieldId the Iceberg field ID
     * @param additionalBytes the additional bytes to add
     */
    public void addColumnSize(int fieldId, long additionalBytes) {
      columnSizes.merge(fieldId, additionalBytes, Long::sum);
    }

    /**
     * Update value count for a given field.
     *
     * @param fieldId the Iceberg field ID
     */
    public void incrementValueCount(int fieldId) {
      valueCounts.merge(fieldId, 1L, Long::sum);
    }

    /**
     * Update null count for a given field.
     *
     * @param fieldId the Iceberg field ID
     */
    public void incrementNullCount(int fieldId) {
      nullCounts.merge(fieldId, 1L, Long::sum);
    }

    /**
     * Update the lower bound for a given field.
     *
     * @param fieldId the Iceberg field ID
     * @param value the candidate lower bound value
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void updateLowerBound(int fieldId, Object value) {
      Object current = lowerBounds.get(fieldId);
      if (current == null || (value instanceof Comparable && ((Comparable) value).compareTo(current) < 0)) {
        lowerBounds.put(fieldId, value);
      }
    }

    /**
     * Update the upper bound for a given field.
     *
     * @param fieldId the Iceberg field ID
     * @param value the candidate upper bound value
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void updateUpperBound(int fieldId, Object value) {
      Object current = upperBounds.get(fieldId);
      if (current == null || (value instanceof Comparable && ((Comparable) value).compareTo(current) > 0)) {
        upperBounds.put(fieldId, value);
      }
    }

    /**
     * Build the final Metrics object from collected data.
     *
     * @param schema the Iceberg schema
     * @return a Metrics object
     */
    public Metrics toMetrics(Schema schema) {
      return LanceMetrics.createMetrics(
          rowCount, schema, columnSizes, valueCounts, nullCounts, lowerBounds, upperBounds);
    }
  }
}
