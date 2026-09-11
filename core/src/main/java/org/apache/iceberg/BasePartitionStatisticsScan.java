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

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class BasePartitionStatisticsScan implements PartitionStatisticsScan {

  private final Table table;
  private Long snapshotId;
  private Schema projection;
  private Expression filter = Expressions.alwaysTrue();
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
    Preconditions.checkArgument(newFilter != null, "Invalid filter: null");
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
    Preconditions.checkArgument(newSchema != null, "Invalid projection schema: null");
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
    Schema schema = PartitionStatistics.schema(partitionType, TableUtil.formatVersion(table));
    Schema readSchema = readSchema(schema);

    FileFormat fileFormat = FileFormat.fromFileName(statsFile.get().path());
    Preconditions.checkNotNull(
        fileFormat != null, "Unable to determine format of file: %s", statsFile.get().path());

    CloseableIterable<PartitionStatistics> result =
        InternalData.read(fileFormat, table.io().newInputFile(statsFile.get().path()))
            .project(readSchema)
            .setRootType(BasePartitionStatistics.class)
            .build();

    if (filter != Expressions.alwaysTrue()) {
      Evaluator evaluator = new Evaluator(readSchema.asStruct(), filter, caseSensitive);
      result = CloseableIterable.filter(result, evaluator::eval);
    }

    return result;
  }

  /**
   * Resolves the schema to read. When a projection is set, all columns referenced by the filter are
   * added to the result not just the projected fields.
   */
  private Schema readSchema(Schema schema) {
    if (projection == null) {
      return schema;
    }

    Set<Integer> fieldIdsToRead = Sets.newHashSet();

    fieldIdsToRead.addAll(TypeUtil.getProjectedIds(projection));

    fieldIdsToRead.addAll(
        Binder.boundReferences(
            schema.asStruct(), Collections.singletonList(filter), caseSensitive));

    return TypeUtil.select(schema, fieldIdsToRead);
  }
}
