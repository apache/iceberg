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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.ComputeTableStats;
import org.apache.iceberg.actions.ImmutableComputeTableStats;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Computes the statistics of the given columns and stores it as Puffin files. */
public class ComputeTableStatsSparkAction extends BaseSparkAction<ComputeTableStatsSparkAction>
    implements ComputeTableStats {

  private static final Logger LOG = LoggerFactory.getLogger(ComputeTableStatsSparkAction.class);
  private static final Result EMPTY_RESULT = ImmutableComputeTableStats.Result.builder().build();

  private final Table table;
  private Set<String> columns;
  private Snapshot snapshot;

  ComputeTableStatsSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.snapshot = table.currentSnapshot();
  }

  @Override
  protected ComputeTableStatsSparkAction self() {
    return this;
  }

  @Override
  public ComputeTableStats columns(String... newColumns) {
    Preconditions.checkArgument(
        newColumns != null && newColumns.length > 0, "Columns cannot be null/empty");
    this.columns = ImmutableSet.copyOf(newColumns);
    return this;
  }

  @Override
  public ComputeTableStats snapshot(long newSnapshotId) {
    Snapshot newSnapshot = table.snapshot(newSnapshotId);
    Preconditions.checkArgument(newSnapshot != null, "Snapshot not found: %s", newSnapshotId);
    this.snapshot = newSnapshot;
    return this;
  }

  @Override
  public Result execute() {
    JobGroupInfo info = newJobGroupInfo("COMPUTE-TABLE-STATS", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    if (snapshot == null) {
      return EMPTY_RESULT;
    }
    validateColumns();
    LOG.info(
        "Computing stats for columns {} in {} (snapshot {})",
        columns(),
        table.name(),
        snapshotId());
    List<Blob> blobs = generateNDVBlobs();
    StatisticsFile statisticsFile;
    statisticsFile = writeStatsFile(blobs);

    table.updateStatistics().setStatistics(snapshot.snapshotId(), statisticsFile).commit();
    return ImmutableComputeTableStats.Result.builder().statisticsFile(statisticsFile).build();
  }

  private StatisticsFile writeStatsFile(List<Blob> blobs) {
    LOG.info("Writing stats for table {} for snapshot {}", table.name(), snapshot.snapshotId());
    TableOperations operations = ((HasTableOperations) table).operations();
    OutputFile outputFile = operations.io().newOutputFile(outputPath());
    try (PuffinWriter writer = Puffin.write(outputFile).createdBy(appIdentifier()).build()) {
      blobs.forEach(writer::add);
      writer.finish();
      return new GenericStatisticsFile(
          snapshot.snapshotId(),
          outputFile.location(),
          writer.fileSize(),
          writer.footerSize(),
          GenericBlobMetadata.from(writer.writtenBlobsMetadata()));
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private List<Blob> generateNDVBlobs() {
    return NDVSketchGenerator.generateNDVSketchesAndBlobs(spark(), table, snapshot, columns());
  }

  private Set<String> columns() {
    Schema schema = snapshot == null ? table.schema() : table.schemas().get(snapshot.schemaId());
    if (columns == null) {
      columns = schema.columns().stream().map(Types.NestedField::name).collect(Collectors.toSet());
    }
    return columns;
  }

  private void validateColumns() {
    Schema schema = table.schemas().get(snapshot.schemaId());
    for (String columnName : columns) {
      Types.NestedField field = schema.findField(columnName);
      Preconditions.checkArgument(field != null, "Can't find column %s in %s", columnName, schema);
      Preconditions.checkArgument(
          field.type().isPrimitiveType(),
          "Can't compute stats on non-primitive type column: %s (%s)",
          columnName,
          field.type());
    }
  }

  private String appIdentifier() {
    String icebergVersion = IcebergBuild.fullVersion();
    String sparkVersion = spark().version();
    return String.format("Iceberg %s Spark %s", icebergVersion, sparkVersion);
  }

  private Long snapshotId() {
    return snapshot != null ? snapshot.snapshotId() : null;
  }

  private String jobDesc() {
    return String.format(
        "Computing table stats for %s (snapshot_id=%s, columns=%s)",
        table.name(), snapshotId(), columns());
  }

  private String outputPath() {
    TableOperations operations = ((HasTableOperations) table).operations();
    String fileName = String.format("%s-%s.stats", snapshotId(), UUID.randomUUID());
    return operations.metadataFileLocation(fileName);
  }
}
