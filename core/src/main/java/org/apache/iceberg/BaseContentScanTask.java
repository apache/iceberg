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

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Ordering;

abstract class BaseContentScanTask<ThisT extends ContentScanTask<F>, F extends ContentFile<F>>
    implements ContentScanTask<F>, SplittableScanTask<ThisT> {

  private static final Ordering<Comparable<Long>> OFFSET_ORDERING = Ordering.natural();

  private final F file;
  private final String schemaString;
  private final String specString;
  private final ResidualEvaluator residuals;

  private transient volatile Schema schema = null;
  private transient volatile PartitionSpec spec = null;

  BaseContentScanTask(F file, String schemaString, String specString, ResidualEvaluator residuals) {
    this.file = file;
    this.schemaString = schemaString;
    this.specString = specString;
    this.residuals = residuals;
  }

  protected abstract ThisT self();

  protected abstract ThisT newSplitTask(ThisT parentTask, long offset, long length);

  @Override
  public F file() {
    return file;
  }

  protected Schema schema() {
    if (schema == null) {
      synchronized (this) {
        if (schema == null) {
          this.schema = SchemaParser.fromJson(schemaString);
        }
      }
    }

    return schema;
  }

  @Override
  public PartitionSpec spec() {
    if (spec == null) {
      synchronized (this) {
        if (spec == null) {
          this.spec = PartitionSpecParser.fromJson(schema(), specString);
        }
      }
    }
    return spec;
  }

  @Override
  public long start() {
    return 0;
  }

  @Override
  public long length() {
    return file.fileSizeInBytes();
  }

  @Override
  public Expression residual() {
    return residuals.residualFor(file.partition());
  }

  @Override
  public Iterable<ThisT> split(long targetSplitSize) {
    if (file.format().isSplittable()) {
      if (file.splitOffsets() != null && OFFSET_ORDERING.isOrdered(file.splitOffsets())) {
        return () ->
            new OffsetsAwareSplitScanTaskIterator<>(
                self(), length(), file.splitOffsets(), this::newSplitTask);
      } else {
        return () ->
            new FixedSizeSplitScanTaskIterator<>(
                self(), length(), targetSplitSize, this::newSplitTask);
      }
    }

    return ImmutableList.of(self());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("file", file().path())
        .add("partition_data", file().partition())
        .add("residual", residual())
        .toString();
  }
}
