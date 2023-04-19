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
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

/** A {@link Table} implementation that exposes a table's manifest files as rows. */
public class ManifestsTable extends BaseMetadataTable {
  private static final Schema SNAPSHOT_SCHEMA =
      new Schema(
          Types.NestedField.required(14, "content", Types.IntegerType.get()),
          Types.NestedField.required(1, "path", Types.StringType.get()),
          Types.NestedField.required(2, "length", Types.LongType.get()),
          Types.NestedField.required(3, "partition_spec_id", Types.IntegerType.get()),
          Types.NestedField.required(4, "added_snapshot_id", Types.LongType.get()),
          Types.NestedField.required(5, "added_data_files_count", Types.IntegerType.get()),
          Types.NestedField.required(6, "existing_data_files_count", Types.IntegerType.get()),
          Types.NestedField.required(7, "deleted_data_files_count", Types.IntegerType.get()),
          Types.NestedField.required(15, "added_delete_files_count", Types.IntegerType.get()),
          Types.NestedField.required(16, "existing_delete_files_count", Types.IntegerType.get()),
          Types.NestedField.required(17, "deleted_delete_files_count", Types.IntegerType.get()),
          Types.NestedField.required(
              8,
              "partition_summaries",
              Types.ListType.ofRequired(
                  9,
                  Types.StructType.of(
                      Types.NestedField.required(10, "contains_null", Types.BooleanType.get()),
                      Types.NestedField.optional(11, "contains_nan", Types.BooleanType.get()),
                      Types.NestedField.optional(12, "lower_bound", Types.StringType.get()),
                      Types.NestedField.optional(13, "upper_bound", Types.StringType.get())))));

  ManifestsTable(Table table) {
    this(table, table.name() + ".manifests");
  }

  ManifestsTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public TableScan newScan() {
    return new ManifestsTableScan(table());
  }

  @Override
  public Schema schema() {
    return SNAPSHOT_SCHEMA;
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.MANIFESTS;
  }

  protected DataTask task(TableScan scan) {
    FileIO io = table().io();
    String location = scan.snapshot().manifestListLocation();
    Map<Integer, PartitionSpec> specs = Maps.newHashMap(table().specs());

    return StaticDataTask.of(
        io.newInputFile(
            location != null ? location : table().operations().current().metadataFileLocation()),
        schema(),
        scan.schema(),
        scan.snapshot().allManifests(io),
        manifest -> {
          PartitionSpec spec = specs.get(manifest.partitionSpecId());
          return ManifestsTable.manifestFileToRow(spec, manifest);
        });
  }

  private class ManifestsTableScan extends StaticTableScan {
    ManifestsTableScan(Table table) {
      super(table, SNAPSHOT_SCHEMA, MetadataTableType.MANIFESTS, ManifestsTable.this::task);
    }
  }

  static StaticDataTask.Row manifestFileToRow(PartitionSpec spec, ManifestFile manifest) {
    return StaticDataTask.Row.of(
        manifest.content().id(),
        manifest.path(),
        manifest.length(),
        manifest.partitionSpecId(),
        manifest.snapshotId(),
        manifest.content() == ManifestContent.DATA ? manifest.addedFilesCount() : 0,
        manifest.content() == ManifestContent.DATA ? manifest.existingFilesCount() : 0,
        manifest.content() == ManifestContent.DATA ? manifest.deletedFilesCount() : 0,
        manifest.content() == ManifestContent.DELETES ? manifest.addedFilesCount() : 0,
        manifest.content() == ManifestContent.DELETES ? manifest.existingFilesCount() : 0,
        manifest.content() == ManifestContent.DELETES ? manifest.deletedFilesCount() : 0,
        partitionSummariesToRows(spec, manifest.partitions()));
  }

  static List<StaticDataTask.Row> partitionSummariesToRows(
      PartitionSpec spec, List<ManifestFile.PartitionFieldSummary> summaries) {
    if (summaries == null) {
      return null;
    }

    List<StaticDataTask.Row> rows = Lists.newArrayList();

    for (int i = 0; i < summaries.size(); i += 1) {
      ManifestFile.PartitionFieldSummary summary = summaries.get(i);
      rows.add(
          StaticDataTask.Row.of(
              summary.containsNull(),
              summary.containsNaN(),
              spec.fields()
                  .get(i)
                  .transform()
                  .toHumanString(
                      spec.partitionType().fields().get(i).type(),
                      Conversions.fromByteBuffer(
                          spec.partitionType().fields().get(i).type(), summary.lowerBound())),
              spec.fields()
                  .get(i)
                  .transform()
                  .toHumanString(
                      spec.partitionType().fields().get(i).type(),
                      Conversions.fromByteBuffer(
                          spec.partitionType().fields().get(i).type(), summary.upperBound()))));
    }

    return rows;
  }
}
