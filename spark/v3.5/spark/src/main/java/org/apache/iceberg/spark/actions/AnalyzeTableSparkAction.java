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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.AnalyzeTable;
import org.apache.iceberg.actions.ImmutableAnalyzeTable;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Computes the statistic of the given columns and stores it as Puffin files. */
public class AnalyzeTableSparkAction extends BaseSparkAction<AnalyzeTableSparkAction>
    implements AnalyzeTable {

  private static final Logger LOG = LoggerFactory.getLogger(AnalyzeTableSparkAction.class);

  private final Table table;
  private Set<String> columns;
  private long snapshotId;

  AnalyzeTableSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    Snapshot snapshot = table.currentSnapshot();
    if (snapshot != null) {
      this.snapshotId = snapshot.snapshotId();
    }
    this.columns =
        table.schema().columns().stream().map(Types.NestedField::name).collect(Collectors.toSet());
  }

  @Override
  protected AnalyzeTableSparkAction self() {
    return this;
  }

  @Override
  public Result execute() {
    String desc = String.format("Analyzing table %s for snapshot id %s", table.name(), snapshotId);
    JobGroupInfo info = newJobGroupInfo("ANALYZE-TABLE", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    LOG.info("Starting analysis of {} for snapshot {}", table.name(), snapshotId);
    List<Blob> blobs = generateNDVBlobs();
    StatisticsFile statisticFile;
    try {
      statisticFile = writeAndCommitPuffin(blobs);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return ImmutableAnalyzeTable.Result.builder().statisticsFile(statisticFile).build();
  }

  private StatisticsFile writeAndCommitPuffin(List<Blob> blobs) throws Exception {
    LOG.info("Writing stats to puffin files for table {}", table.name());
    TableOperations operations = ((HasTableOperations) table).operations();
    FileIO fileIO = operations.io();
    String path = operations.metadataFileLocation(String.format("%s.stats", UUID.randomUUID()));
    OutputFile outputFile = fileIO.newOutputFile(path);
    GenericStatisticsFile statisticsFile;
    try (PuffinWriter writer =
        Puffin.write(outputFile).createdBy("Iceberg Analyze action").build()) {
      blobs.forEach(writer::add);
      writer.finish();
      statisticsFile =
          new GenericStatisticsFile(
              snapshotId,
              path,
              writer.fileSize(),
              writer.footerSize(),
              writer.writtenBlobsMetadata().stream()
                  .map(GenericBlobMetadata::from)
                  .collect(ImmutableList.toImmutableList()));
    }
    table.updateStatistics().setStatistics(snapshotId, statisticsFile).commit();
    return statisticsFile;
  }

  private List<Blob> generateNDVBlobs() {
    return NDVSketchGenerator.generateNDVSketchesAndBlobs(spark(), table, snapshotId, columns);
  }

  @Override
  public AnalyzeTable columns(String... columnNames) {
    Preconditions.checkArgument(
        columnNames != null && columnNames.length > 0, "Columns cannot be null/empty");
    for (String columnName : columnNames) {
      Types.NestedField field = table.schema().findField(columnName);
      if (field == null) {
        throw new ValidationException("No column with %s name in the table", columnName);
      }
    }
    this.columns = ImmutableSet.copyOf(columnNames);
    return this;
  }

  @Override
  public AnalyzeTable snapshot(long snapshotIdToAnalyze) {
    this.snapshotId = snapshotIdToAnalyze;
    return this;
  }
}
