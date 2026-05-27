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

import java.util.Optional;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class BasePartitionStatisticsScan implements PartitionStatisticsScan {

  private final Table table;
  private Long snapshotId;
  private Expression filter = Expressions.alwaysTrue();
  private Schema projection = null;
  private boolean caseSensitive = true;

  public BasePartitionStatisticsScan(Table table) {
    this.table = table;
  }

  @Override
  public PartitionStatisticsScan useSnapshot(long newSnapshotId) {
    Preconditions.checkArgument(
        table.snapshot(newSnapshotId) != null, "Cannot find snapshot with ID %s", newSnapshotId);

    this.snapshotId = newSnapshotId;
    return this;
  }

  @Override
  public PartitionStatisticsScan filter(Expression newFilter) {
    Preconditions.checkArgument(newFilter != null, "Filter expression cannot be null");
    this.filter = newFilter;
    return this;
  }

  @Override
  public PartitionStatisticsScan caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    return this;
  }

  @Override
  public PartitionStatisticsScan project(Schema newSchema) {
    Preconditions.checkArgument(newSchema != null, "Projection schema cannot be null");
    this.projection = newSchema;
    return this;
  }

  @Override
  public CloseableIterable<PartitionStatistics> scan() {
    if (snapshotId == null) {
      if (table.currentSnapshot() == null) {
        return CloseableIterable.empty();
      }

      snapshotId = table.currentSnapshot().snapshotId();
    }

    Optional<PartitionStatisticsFile> statsFile =
        table.partitionStatisticsFiles().stream()
            .filter(f -> f.snapshotId() == snapshotId)
            .findFirst();

    if (statsFile.isEmpty()) {
      return CloseableIterable.empty();
    }

    Types.StructType partitionType = Partitioning.partitionType(table);
    Schema fullSchema =
        PartitionStatistics.schema(partitionType, TableUtil.formatVersion(table));
    Schema readSchema = readSchema(fullSchema);

    FileFormat fileFormat = FileFormat.fromFileName(statsFile.get().path());
    Preconditions.checkNotNull(
        fileFormat != null, "Unable to determine format of file: %s", statsFile.get().path());

    CloseableIterable<PartitionStatistics> result =
        InternalData.read(fileFormat, table.io().newInputFile(statsFile.get().path()), filter)
            .project(readSchema)
            .setRootType(BasePartitionStatistics.class)
            .build();

    if (filter != Expressions.alwaysTrue()) {
      Evaluator evaluator = new Evaluator(readSchema.asStruct(), filter, caseSensitive);
      result = CloseableIterable.filter(result, evaluator::eval);
    }

    return result;
  }

  /** Returns the schema to read from the file: union of projected columns and filter-referenced columns. */
  private Schema readSchema(Schema fullSchema) {
    if (projection == null && filter == Expressions.alwaysTrue()) {
      return fullSchema;
    }

    Schema baseSchema = projection != null ? projection : fullSchema;

    if (filter == Expressions.alwaysTrue()) {
      return baseSchema;
    }

    // Include columns referenced by the filter expression so the evaluator has the data it needs.
    java.util.Set<Integer> filterFieldIds =
        Binder.boundReferences(fullSchema.asStruct(), java.util.Collections.singletonList(filter), caseSensitive);
    Schema filterSchema = TypeUtil.select(fullSchema, filterFieldIds);

    return TypeUtil.join(baseSchema, filterSchema);
  }
}
