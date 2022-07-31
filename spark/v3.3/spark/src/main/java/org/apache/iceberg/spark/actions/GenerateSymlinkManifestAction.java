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
package org.apache.iceberg.spark.actions;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.GenerateSymlinkManifest;
import org.apache.iceberg.actions.GenerateSymlinkManifestActionResult;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GenerateSymlinkManifestAction extends BaseSparkAction<GenerateSymlinkManifestAction>
    implements GenerateSymlinkManifest {

  private final Table table;
  private boolean ignoreDeleteFiles = false;
  private String rootLocation;

  private static final String SYMLINK_MANIFEST_DEFAULT_DIRECTORY = "_symlink_format_manifest";

  protected GenerateSymlinkManifestAction(SparkSession spark, Table table) {
    super(spark);
    ValidationException.check(
        table.currentSnapshot() != null, "Cannot generate symlink manifest for empty table");
    this.table = table;
  }

  @Override
  public GenerateSymlinkManifestAction.Result execute() {
    JobGroupInfo info = newJobGroupInfo("GENERATE-SYMLINK-MANIFEST", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  @Override
  protected GenerateSymlinkManifestAction self() {
    return this;
  }

  @Override
  public GenerateSymlinkManifest ignoreDeleteFiles() {
    this.ignoreDeleteFiles = true;
    return this;
  }

  @Override
  public GenerateSymlinkManifest symlinkManifestRootLocation(String rootLocation) {
    this.rootLocation = rootLocation;
    return this;
  }

  private GenerateSymlinkManifest.Result doExecute() {
    long snapshot = table.currentSnapshot().snapshotId();
    Dataset<Row> entries =
        SparkTableUtil.loadMetadataTable(spark(), table, MetadataTableType.ENTRIES);
    // If we're not ignoring delete files, check if there are any delete manifests for the snapshot.
    if (!ignoreDeleteFiles) {
      boolean deletesExist = entries.filter("status < 2 and data_file.content > 0").isEmpty();
      if (deletesExist) {
        throw new UnsupportedOperationException(
            "Cannot generate symlink manifest when there are delete files. Set ignore delete files for generating a "
                + "symlink manifest of only data files");
      }
    }
    Dataset<Row> activeDataFiles = entries.filter("status < 2 AND data_file.content = 0");

    activeDataFiles.cache();
    long datafileCount = activeDataFiles.count();
    Types.StructType partitionType = Partitioning.partitionType(table);
    String[] partitionColumns =
        partitionType.fields().stream().map(Types.NestedField::name).toArray(String[]::new);

    String[] partitionColumnNames =
        Arrays.stream(partitionColumns)
            .map(name -> "data_file.partition." + name)
            .toArray(String[]::new);

    Dataset<Row> symlinkDataset =
        partitionColumns.length == 0
            ? activeDataFiles.select("data_file.file_path")
            : activeDataFiles.select("data_file.file_path", partitionColumnNames);
    DataFrameWriter<Row> writer = symlinkDataset.write().format("text");
    if (partitionColumns.length == 0) {
      writer.save(symlinkManifestRootLocation());
    } else {
      writer.partitionBy(partitionColumns).save(symlinkManifestRootLocation());
    }
    return new GenerateSymlinkManifestActionResult(snapshot, datafileCount);
  }

  private String symlinkManifestRootLocation() {
    if (rootLocation != null) {
      return rootLocation;
    }
    String tableLocation = table.location();
    long snapshot = table.currentSnapshot().snapshotId();
    StringBuilder sb = new StringBuilder();
    sb.append(tableLocation);
    if (!tableLocation.endsWith("/")) {
      sb.append("/");
    }
    sb.append(SYMLINK_MANIFEST_DEFAULT_DIRECTORY);
    sb.append("/");
    sb.append(snapshot);
    return sb.toString();
  }

  private String jobDesc() {
    List<String> options = Lists.newArrayList();

    options.add("ignoreDeleteFiles" + ignoreDeleteFiles);
    StringBuilder sb = new StringBuilder();
    sb.append("Generating symlink for table ").append(table.name()).append(" at snapshot ");
    sb.append(table.currentSnapshot().snapshotId()).append(".");
    if (ignoreDeleteFiles) {
      sb.append(" Ignoring delete files.");
    }
    return sb.toString();
  }
}
