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
package org.apache.iceberg.flink;

import org.apache.flink.table.expressions.TransformExpression;
import org.apache.iceberg.transforms.PartitionSpecVisitor;

/**
 * A visitor to convert Iceberg partition transforms to Flink TransformExpression objects. Adapted
 * from Spark's SpecTransformToSparkTransform for Flink's partition-aware optimizations.
 */
public class SpecTransformToFlinkTransform implements PartitionSpecVisitor<TransformExpression> {

  public SpecTransformToFlinkTransform() {}

  @Override
  public TransformExpression identity(int fieldId, String sourceName, int sourceId) {
    // TODO -- this maps to the "various" datatypes, are we sure we don't want to include 'identity'
    // as the name
    return new TransformExpression(sourceName, null, null);
  }

  @Override
  public TransformExpression bucket(int fieldId, String sourceName, int sourceId, int numBuckets) {
    return new TransformExpression(sourceName, "bucket", numBuckets);
  }

  @Override
  public TransformExpression truncate(int fieldId, String sourceName, int sourceId, int width) {
    // TODO: Implement truncate transform
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public TransformExpression year(int fieldId, String sourceName, int sourceId) {
    return new TransformExpression(sourceName, "year", null);
  }

  @Override
  public TransformExpression month(int fieldId, String sourceName, int sourceId) {
    return new TransformExpression(sourceName, "month", null);
  }

  @Override
  public TransformExpression day(int fieldId, String sourceName, int sourceId) {
    return new TransformExpression(sourceName, "day", null);
  }

  @Override
  public TransformExpression hour(int fieldId, String sourceName, int sourceId) {
    return new TransformExpression(sourceName, "hour", null);
  }

  @Override
  public TransformExpression alwaysNull(int fieldId, String sourceName, int sourceId) {
    // TODO: Implement alwaysNull transform (may return null like Spark)
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public TransformExpression unknown(
      int fieldId, String sourceName, int sourceId, String transform) {
    // TODO: Implement unknown transform
    throw new UnsupportedOperationException("Not implemented");
  }
}
