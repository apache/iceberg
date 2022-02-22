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

import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.StructType;

/**
 * Base class logic for files metadata tables
 */
abstract class BaseFilesTable extends BaseMetadataTable {

  BaseFilesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public Schema schema() {
    StructType partitionType = Partitioning.partitionType(table());
    Schema schema = new Schema(DataFile.getType(partitionType).fields());
    if (partitionType.fields().size() < 1) {
      // avoid returning an empty struct, which is not always supported. instead, drop the partition field
      return TypeUtil.selectNot(schema, Sets.newHashSet(DataFile.PARTITION_ID));
    } else {
      return schema;
    }
  }

  abstract static class BaseFilesTableScan extends BaseMetadataTableScan {
    private final Schema fileSchema;

    protected BaseFilesTableScan(TableOperations ops, Table table, Schema fileSchema) {
      super(ops, table, fileSchema);
      this.fileSchema = fileSchema;
    }

    protected BaseFilesTableScan(TableOperations ops, Table table, Schema schema, Schema fileSchema,
                           TableScanContext context) {
      super(ops, table, schema, context);
      this.fileSchema = fileSchema;
    }

    @Override
    public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
      throw new UnsupportedOperationException(
          String.format("Cannot incrementally scan table of type %s", MetadataTableType.FILES.name()));
    }

    @Override
    public TableScan appendsAfter(long fromSnapshotId) {
      throw new UnsupportedOperationException(
          String.format("Cannot incrementally scan table of type %s", MetadataTableType.FILES.name()));
    }

    protected CloseableIterable<ManifestFile> filterManifests(List<ManifestFile> manifests,
                                                              Expression rowFilter,
                                                              boolean caseSensitive) {
      CloseableIterable<ManifestFile> manifestIterable = CloseableIterable.withNoopClose(manifests);

      // use an inclusive projection to remove the partition name prefix and filter out any non-partition expressions
      Expression partitionFilter = Projections
          .inclusive(
              transformSpec(fileSchema, table().spec(), PARTITION_FIELD_PREFIX),
              caseSensitive)
          .project(rowFilter);

      ManifestEvaluator manifestEval = ManifestEvaluator.forPartitionFilter(
          partitionFilter, table().spec(), caseSensitive);

      return CloseableIterable.filter(manifestIterable, manifestEval::eval);
    }
  }
}
