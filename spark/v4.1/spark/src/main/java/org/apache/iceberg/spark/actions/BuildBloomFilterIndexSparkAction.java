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
import java.util.UUID;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A minimal Spark action that builds a Bloom-filter-based file index for a single column and stores
 * it as a Puffin statistics file.
 *
 * <p>This is intentionally narrow in scope and intended only as a proof of concept. It computes a
 * Bloom filter per data file for a given column and writes all Bloom blobs into a single statistics
 * file that is attached to the table metadata for the chosen snapshot.
 */
public class BuildBloomFilterIndexSparkAction
    extends BaseSparkAction<BuildBloomFilterIndexSparkAction> {

  private static final Logger LOG = LoggerFactory.getLogger(BuildBloomFilterIndexSparkAction.class);

  public static class Result {
    private final List<StatisticsFile> statisticsFiles;

    Result(List<StatisticsFile> statisticsFiles) {
      this.statisticsFiles = statisticsFiles;
    }

    public List<StatisticsFile> statisticsFiles() {
      return statisticsFiles;
    }

    public List<org.apache.iceberg.DataFile> rewrittenDataFiles() {
      return ImmutableList.of();
    }

    public List<org.apache.iceberg.DataFile> addedDataFiles() {
      return ImmutableList.of();
    }

    public List<org.apache.iceberg.DeleteFile> rewrittenDeleteFiles() {
      return ImmutableList.of();
    }

    public List<org.apache.iceberg.DeleteFile> addedDeleteFiles() {
      return ImmutableList.of();
    }

    @Override
    public String toString() {
      return String.format("BuildBloomFilterIndexResult(statisticsFiles=%s)", statisticsFiles);
    }
  }

  private final Table table;
  private Snapshot snapshot;
  private String column;

  BuildBloomFilterIndexSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.snapshot = table.currentSnapshot();
  }

  @Override
  protected BuildBloomFilterIndexSparkAction self() {
    return this;
  }

  public BuildBloomFilterIndexSparkAction column(String columnName) {
    Preconditions.checkArgument(
        columnName != null && !columnName.isEmpty(), "Column name must not be null/empty");
    this.column = columnName;
    return this;
  }

  public BuildBloomFilterIndexSparkAction snapshot(long snapshotId) {
    Snapshot newSnapshot = table.snapshot(snapshotId);
    Preconditions.checkArgument(newSnapshot != null, "Snapshot not found: %s", snapshotId);
    this.snapshot = newSnapshot;
    return this;
  }

  public Result execute() {
    Preconditions.checkNotNull(column, "Column must be set before executing Bloom index build");
    if (snapshot == null) {
      LOG.info("No snapshot to index for table {}", table.name());
      return new Result(ImmutableList.of());
    }

    JobGroupInfo info = newJobGroupInfo("BUILD-BLOOM-INDEX", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    LOG.info(
        "Building Bloom index for column {} in {} (snapshot {})",
        column,
        table.name(),
        snapshot.snapshotId());

    List<Blob> blobs =
        BloomFilterIndexUtil.buildBloomBlobsForColumn(spark(), table, snapshot, column);

    if (blobs.isEmpty()) {
      LOG.info(
          "No Bloom blobs generated for column {} in table {} (snapshot {}), skipping write",
          column,
          table.name(),
          snapshot.snapshotId());
      return new Result(ImmutableList.of());
    }

    StatisticsFile statsFile = writeStatsFile(blobs);
    table.updateStatistics().setStatistics(statsFile).commit();

    return new Result(ImmutableList.of(statsFile));
  }

  private StatisticsFile writeStatsFile(List<Blob> blobs) {
    LOG.info(
        "Writing Bloom index stats for table {} for snapshot {} ({} blob(s))",
        table.name(),
        snapshot.snapshotId(),
        blobs.size());
    OutputFile outputFile = table.io().newOutputFile(outputPath());
    try (PuffinWriter writer =
        Puffin.write(outputFile)
            .createdBy(appIdentifier())
            .compressBlobs(org.apache.iceberg.puffin.PuffinCompressionCodec.ZSTD)
            .build()) {
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

  private String appIdentifier() {
    String icebergVersion = IcebergBuild.fullVersion();
    String sparkVersion = spark().version();
    return String.format("Iceberg %s Spark %s (BloomIndexPOC)", icebergVersion, sparkVersion);
  }

  private String jobDesc() {
    return String.format(
        "Building Bloom index for %s (snapshot_id=%s, column=%s)",
        table.name(), snapshot.snapshotId(), column);
  }

  private String outputPath() {
    TableOperations operations = ((HasTableOperations) table).operations();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < column.length(); i++) {
      char ch = column.charAt(i);
      sb.append(Character.isLetterOrDigit(ch) ? ch : '_');
    }
    String sanitizedCol = sb.toString();
    String fileName =
        String.format(
            "%s-%s-bloom-%s.stats", snapshot.snapshotId(), UUID.randomUUID(), sanitizedCol);
    return operations.metadataFileLocation(fileName);
  }
}
