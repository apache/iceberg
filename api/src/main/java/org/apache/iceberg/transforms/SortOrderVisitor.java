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
package org.apache.iceberg.transforms;

import java.util.List;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public interface SortOrderVisitor<T> {

  T field(String sourceName, int sourceId, SortDirection direction, NullOrder nullOrder);

  T bucket(
      String sourceName, int sourceId, int width, SortDirection direction, NullOrder nullOrder);

  T truncate(
      String sourceName, int sourceId, int width, SortDirection direction, NullOrder nullOrder);

  T year(String sourceName, int sourceId, SortDirection direction, NullOrder nullOrder);

  T month(String sourceName, int sourceId, SortDirection direction, NullOrder nullOrder);

  T day(String sourceName, int sourceId, SortDirection direction, NullOrder nullOrder);

  T hour(String sourceName, int sourceId, SortDirection direction, NullOrder nullOrder);

  default T unknown(
      String sourceName,
      int sourceId,
      String transform,
      SortDirection direction,
      NullOrder nullOrder) {
    throw new UnsupportedOperationException(
        String.format("Unknown transform %s is not supported", transform));
  }

  /**
   * Visit the fields of a {@link SortOrder}.
   *
   * @param sortOrder a sort order to visit
   * @param visitor a sort order visitor
   * @param <R> return type of the visitor
   * @return a list of the result produced by visiting each sort field
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  static <R> List<R> visit(SortOrder sortOrder, SortOrderVisitor<R> visitor) {
    Schema schema = sortOrder.schema();
    List<R> results = Lists.newArrayListWithExpectedSize(sortOrder.fields().size());

    for (SortField field : sortOrder.fields()) {
      String sourceName = schema.findColumnName(field.sourceId());
      Transform<?, ?> transform = field.transform();

      if (transform == null || transform instanceof Identity) {
        results.add(
            visitor.field(sourceName, field.sourceId(), field.direction(), field.nullOrder()));
      } else if (transform instanceof Bucket) {
        int numBuckets = ((Bucket<?>) transform).numBuckets();
        results.add(
            visitor.bucket(
                sourceName, field.sourceId(), numBuckets, field.direction(), field.nullOrder()));
      } else if (transform instanceof Truncate) {
        int width = ((Truncate<?>) transform).width();
        results.add(
            visitor.truncate(
                sourceName, field.sourceId(), width, field.direction(), field.nullOrder()));
      } else if (transform == Dates.YEAR
          || transform == Timestamps.YEAR_FROM_MICROS
          || transform == Timestamps.YEAR_FROM_NANOS
          || transform instanceof Years) {
        results.add(
            visitor.year(sourceName, field.sourceId(), field.direction(), field.nullOrder()));
      } else if (transform == Dates.MONTH
          || transform == Timestamps.MONTH_FROM_MICROS
          || transform == Timestamps.MONTH_FROM_NANOS
          || transform instanceof Months) {
        results.add(
            visitor.month(sourceName, field.sourceId(), field.direction(), field.nullOrder()));
      } else if (transform == Dates.DAY
          || transform == Timestamps.DAY_FROM_MICROS
          || transform == Timestamps.DAY_FROM_NANOS
          || transform instanceof Days) {
        results.add(
            visitor.day(sourceName, field.sourceId(), field.direction(), field.nullOrder()));
      } else if (transform == Timestamps.HOUR_FROM_MICROS
          || transform == Timestamps.HOUR_FROM_NANOS
          || transform instanceof Hours) {
        results.add(
            visitor.hour(sourceName, field.sourceId(), field.direction(), field.nullOrder()));
      } else if (transform instanceof UnknownTransform) {
        results.add(
            visitor.unknown(
                sourceName,
                field.sourceId(),
                transform.toString(),
                field.direction(),
                field.nullOrder()));
      }
    }

    return results;
  }
}
