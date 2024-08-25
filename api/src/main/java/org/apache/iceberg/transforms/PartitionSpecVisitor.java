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
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public interface PartitionSpecVisitor<T> {
  default T identity(int fieldId, String sourceName, int sourceId) {
    return identity(sourceName, sourceId);
  }

  default T identity(String sourceName, int sourceId) {
    throw new UnsupportedOperationException("Identity transform is not supported");
  }

  default T bucket(int fieldId, String sourceName, int sourceId, int numBuckets) {
    return bucket(sourceName, sourceId, numBuckets);
  }

  default T bucket(String sourceName, int sourceId, int numBuckets) {
    throw new UnsupportedOperationException("Bucket transform is not supported");
  }

  default T truncate(int fieldId, String sourceName, int sourceId, int width) {
    return truncate(sourceName, sourceId, width);
  }

  default T truncate(String sourceName, int sourceId, int width) {
    throw new UnsupportedOperationException("Truncate transform is not supported");
  }

  default T year(int fieldId, String sourceName, int sourceId) {
    return year(sourceName, sourceId);
  }

  default T year(String sourceName, int sourceId) {
    throw new UnsupportedOperationException("Year transform is not supported");
  }

  default T month(int fieldId, String sourceName, int sourceId) {
    return month(sourceName, sourceId);
  }

  default T month(String sourceName, int sourceId) {
    throw new UnsupportedOperationException("Month transform is not supported");
  }

  default T day(int fieldId, String sourceName, int sourceId) {
    return day(sourceName, sourceId);
  }

  default T day(String sourceName, int sourceId) {
    throw new UnsupportedOperationException("Day transform is not supported");
  }

  default T hour(int fieldId, String sourceName, int sourceId) {
    return hour(sourceName, sourceId);
  }

  default T hour(String sourceName, int sourceId) {
    throw new UnsupportedOperationException("Hour transform is not supported");
  }

  default T alwaysNull(int fieldId, String sourceName, int sourceId) {
    throw new UnsupportedOperationException("Void transform is not supported");
  }

  default T unknown(int fieldId, String sourceName, int sourceId, String transform) {
    throw new UnsupportedOperationException(
        String.format("Unknown transform %s is not supported", transform));
  }

  /**
   * Visit the fields of a {@link PartitionSpec}.
   *
   * @param spec a partition spec to visit
   * @param visitor a partition spec visitor
   * @param <R> return type of the visitor
   * @return a list of the result produced by visiting each partition field
   */
  static <R> List<R> visit(PartitionSpec spec, PartitionSpecVisitor<R> visitor) {
    List<R> results = Lists.newArrayListWithExpectedSize(spec.fields().size());

    for (PartitionField field : spec.fields()) {
      results.add(visit(spec.schema(), field, visitor));
    }

    return results;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  static <R> R visit(Schema schema, PartitionField field, PartitionSpecVisitor<R> visitor) {
    String sourceName = schema.findColumnName(field.sourceId());
    Transform<?, ?> transform = field.transform();

    if (transform instanceof Identity) {
      return visitor.identity(field.fieldId(), sourceName, field.sourceId());
    } else if (transform instanceof Bucket) {
      int numBuckets = ((Bucket<?>) transform).numBuckets();
      return visitor.bucket(field.fieldId(), sourceName, field.sourceId(), numBuckets);
    } else if (transform instanceof Truncate) {
      int width = ((Truncate<?>) transform).width();
      return visitor.truncate(field.fieldId(), sourceName, field.sourceId(), width);
    } else if (transform == Dates.YEAR
        || transform == Timestamps.MICROS_TO_YEAR
        || transform == Timestamps.NANOS_TO_YEAR
        || transform instanceof Years) {
      return visitor.year(field.fieldId(), sourceName, field.sourceId());
    } else if (transform == Dates.MONTH
        || transform == Timestamps.MICROS_TO_MONTH
        || transform == Timestamps.NANOS_TO_MONTH
        || transform instanceof Months) {
      return visitor.month(field.fieldId(), sourceName, field.sourceId());
    } else if (transform == Dates.DAY
        || transform == Timestamps.MICROS_TO_DAY
        || transform == Timestamps.NANOS_TO_DAY
        || transform instanceof Days) {
      return visitor.day(field.fieldId(), sourceName, field.sourceId());
    } else if (transform == Timestamps.MICROS_TO_HOUR
        || transform == Timestamps.NANOS_TO_HOUR
        || transform instanceof Hours) {
      return visitor.hour(field.fieldId(), sourceName, field.sourceId());
    } else if (transform instanceof VoidTransform) {
      return visitor.alwaysNull(field.fieldId(), sourceName, field.sourceId());
    } else if (transform instanceof UnknownTransform) {
      return visitor.unknown(field.fieldId(), sourceName, field.sourceId(), transform.toString());
    }

    throw new UnsupportedOperationException(
        String.format("Unknown transform class %s", field.transform().getClass().getName()));
  }
}
