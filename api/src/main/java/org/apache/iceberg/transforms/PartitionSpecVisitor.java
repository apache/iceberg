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
  T identity(String sourceName, int sourceId);

  T bucket(String sourceName, int sourceId, int width);

  T truncate(String sourceName, int sourceId, int width);

  T year(String sourceName, int sourceId);

  T month(String sourceName, int sourceId);

  T day(String sourceName, int sourceId);

  T hour(String sourceName, int sourceId);

  static <R> List<R> visit(Schema schema, PartitionSpec spec, PartitionSpecVisitor<R> visitor) {
    List<R> results = Lists.newArrayListWithExpectedSize(spec.fields().size());

    for (PartitionField field : spec.fields()) {
      String sourceName = schema.findColumnName(field.sourceId());
      Transform<?, ?> transform = field.transform();

      if (transform instanceof Identity) {
        results.add(visitor.identity(sourceName, field.sourceId()));
      } else if (transform instanceof Bucket) {
        results.add(visitor.bucket(sourceName, field.sourceId(),
            ((Bucket<?>) transform).numBuckets()));
      } else if (transform instanceof Truncate) {
        results.add(visitor.truncate(sourceName, field.sourceId(),
            ((Truncate<?>) transform).width()));
      } else if (transform == Dates.YEAR || transform == Timestamps.YEAR) {
        results.add(visitor.year(sourceName, field.sourceId()));
      } else if (transform == Dates.MONTH || transform == Timestamps.MONTH) {
        results.add(visitor.month(sourceName, field.sourceId()));
      } else if (transform == Dates.DAY || transform == Timestamps.DAY) {
        results.add(visitor.day(sourceName, field.sourceId()));
      } else if (transform == Timestamps.HOUR) {
        results.add(visitor.hour(sourceName, field.sourceId()));
      }
    }

    return results;
  }
}
