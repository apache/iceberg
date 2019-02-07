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

package com.netflix.iceberg;

import com.google.common.collect.Iterables;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.iceberg.expressions.ResidualEvaluator.unpartitioned;

public class MetadataTableScan extends BaseTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataTableScan.class);
  private static final long TARGET_SPLIT_SIZE = 32 * 1024 * 1024; // 32 MB

  protected MetadataTableScan(TableOperations ops, Table table) {
    super(ops, table, ManifestEntry.getSchema(table.spec().partitionType()));
  }

  protected MetadataTableScan(TableOperations ops, Table table, Long snapshotId, Schema schema,
                              Expression rowFilter) {
    super(ops, table, snapshotId, schema, rowFilter);
  }

  @Override
  protected TableScan newRefinedScan(TableOperations ops, Table table, Long snapshotId,
                                     Schema schema, Expression rowFilter) {
    return new MetadataTableScan(ops, table, snapshotId, schema, rowFilter);
  }

  @Override
  protected long targetSplitSize(TableOperations ops) {
    return TARGET_SPLIT_SIZE;
  }

  @Override
  protected CloseableIterable<FileScanTask> planFiles(TableOperations ops, Snapshot snapshot,
                                                      Expression rowFilter) {
    CloseableIterable<ManifestFile> manifests;
    if (snapshot.manifestListLocation() != null) {
      manifests = Avro
          .read(ops.io().newInputFile(snapshot.manifestListLocation()))
          .rename("manifest_file", GenericManifestFile.class.getName())
          .rename("partitions", GenericPartitionFieldSummary.class.getName())
          .rename("r508", GenericPartitionFieldSummary.class.getName())
          .project(ManifestFile.schema())
          .reuseContainers(false)
          .build();
    } else {
      manifests = CloseableIterable.withNoopClose(snapshot.manifests());
    }

    String schemaString = SchemaParser.toJson(schema());
    String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());

    return CloseableIterable.wrap(manifests, files -> Iterables.transform(files,
        manifest -> new BaseFileScanTask(
            DataFiles.fromManifest(manifest), schemaString, specString, unpartitioned(rowFilter))));
  }
}
