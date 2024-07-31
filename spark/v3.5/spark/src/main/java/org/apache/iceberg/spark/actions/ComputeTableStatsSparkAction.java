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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
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
  public Result execute() {
    String desc = String.format("Computing stats for %s", table.name());
    JobGroupInfo info = newJobGroupInfo("COMPUTE-TABLE-STATS", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    if (snapshot == null) {
      return EMPTY_RESULT;
    }
    LOG.info("Computing stats of {} for snapshot {}", table.name(), snapshot.snapshotId());
    List<Blob> blobs = generateNDVBlobs();
    StatisticsFile statisticsFile;
    try {
      statisticsFile = writeStatsFile(blobs);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
    table.updateStatistics().setStatistics(snapshot.snapshotId(), statisticsFile).commit();
    return ImmutableComputeTableStats.Result.builder().statisticsFile(statisticsFile).build();
  }

  private StatisticsFile writeStatsFile(List<Blob> blobs) throws IOException {
    LOG.info("Writing stats for table {} for snapshot {}", table.name(), snapshot.snapshotId());
    TableOperations operations = ((HasTableOperations) table).operations();
    String path = operations.metadataFileLocation(String.format("%s.stats", UUID.randomUUID()));
    OutputFile outputFile = operations.io().newOutputFile(path);
    try (PuffinWriter writer = Puffin.write(outputFile).createdBy(appIdentifier()).build()) {
      blobs.forEach(writer::add);
      writer.finish();
      return new GenericStatisticsFile(
          snapshot.snapshotId(),
          path,
          writer.fileSize(),
          writer.footerSize(),
          writer.writtenBlobsMetadata().stream()
              .map(GenericBlobMetadata::from)
              .collect(ImmutableList.toImmutableList()));
    }
  }

  private List<Blob> generateNDVBlobs() {
    return NDVSketchGenerator.generateNDVSketchesAndBlobs(spark(), table, snapshot, columns());
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

  private Set<String> columns() {
    Schema schema = table.schemas().get(snapshot.schemaId());
    if (columns == null) {
      columns = schema.columns().stream().map(Types.NestedField::name).collect(Collectors.toSet());
    }
    validateColumns(schema);
    return columns;
  }

  private void validateColumns(Schema schema) {
    for (String columnName : columns) {
      Types.NestedField field = schema.findField(columnName);
      if (field == null) {
        throw new IllegalArgumentException(
            String.format("No column with name %s in the table", columnName));
      }
      if (!field.type().isPrimitiveType()) {
        throw new IllegalArgumentException(
            String.format(
                "Stats computation not supported on non-primitive type column: %s", columnName));
      }
    }
  }

  private String appIdentifier() {
    String icebergVersion = IcebergBuild.fullVersion();
    String sparkVersion = spark().version();
    return String.format("Iceberg %s Spark %s", icebergVersion, sparkVersion);
  }
}
