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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ImmutableRemoveMissingFiles;
import org.apache.iceberg.actions.RemoveMissingFiles;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.SparkDeleteFile;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class RemoveMissingFilesSparkAction
    extends BaseSnapshotUpdateSparkAction<RemoveMissingFilesSparkAction>
    implements RemoveMissingFiles {

  private final Table table;

  RemoveMissingFilesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected RemoveMissingFilesSparkAction self() {
    return this;
  }

  @Override
  public RemoveMissingFiles.Result execute() {
    String jobDesc = String.format("Removing missing files from %s", table.name());
    JobGroupInfo info = newJobGroupInfo("REMOVE-MISSING-FILES", jobDesc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private static boolean missingFromStorage(Row row, FileIO io) {
    String filePath = row.getString(row.fieldIndex("file_path"));
    return !io.newInputFile(filePath).exists();
  }

  private RemoveMissingFiles.Result doExecute() {
    org.apache.iceberg.RemoveMissingFiles rmf = table.newRemoveFiles();

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    Broadcast<Table> tableBroadcast = jsc.broadcast(SerializableTableWithSize.copyOf(table));

    Dataset<Row> entries = loadMetadataTable(table, MetadataTableType.ENTRIES);
    Dataset<Row> dataEntries =
        entries
            .filter("data_file.content = 0 AND status < 2")
            .select("data_file.*")
            .filter(
                (FilterFunction<Row>) row -> missingFromStorage(row, tableBroadcast.value().io()));
    Dataset<Row> deleteEntries =
        entries
            .filter("data_file.content != 0 AND status < 2")
            .select("data_file.*")
            .filter(
                (FilterFunction<Row>) row -> missingFromStorage(row, tableBroadcast.value().io()));

    List<DataFile> dataFiles =
        dataEntries.collectAsList().stream()
            .map(row -> dataFileWrapper(dataEntries.schema(), row))
            .collect(Collectors.toList());
    List<DeleteFile> deleteFiles =
        deleteEntries.collectAsList().stream()
            .map(row -> deleteFileWrapper(deleteEntries.schema(), row))
            .collect(Collectors.toList());

    List<String> removedDataFiles = Lists.newArrayList();
    List<String> removedDeleteFiles = Lists.newArrayList();

    for (DataFile f : dataFiles) {
      removedDataFiles.add(f.location());
      rmf.deleteFile(f);
    }

    for (DeleteFile f : deleteFiles) {
      removedDeleteFiles.add(f.location());
      rmf.deleteFile(f);
    }

    if (!(dataFiles.isEmpty() && deleteFiles.isEmpty())) {
      commit(rmf);
    }

    return ImmutableRemoveMissingFiles.Result.builder()
        .removedDataFiles(removedDataFiles)
        .removedDeleteFiles(removedDeleteFiles)
        .build();
  }

  private DataFile dataFileWrapper(StructType sparkFileType, Row row) {
    int specId = row.getInt(row.fieldIndex("spec_id"));
    Types.StructType combinedFileType = DataFile.getType(Partitioning.partitionType(table));
    Types.StructType projection = DataFile.getType(table.specs().get(specId).partitionType());
    return new SparkDataFile(combinedFileType, projection, sparkFileType).wrap(row);
  }

  private DeleteFile deleteFileWrapper(StructType sparkFileType, Row row) {
    int specId = row.getInt(row.fieldIndex("spec_id"));
    Types.StructType combinedFileType = DataFile.getType(Partitioning.partitionType(table));
    Types.StructType projection = DataFile.getType(table.specs().get(specId).partitionType());
    return new SparkDeleteFile(combinedFileType, projection, sparkFileType).wrap(row);
  }
}
