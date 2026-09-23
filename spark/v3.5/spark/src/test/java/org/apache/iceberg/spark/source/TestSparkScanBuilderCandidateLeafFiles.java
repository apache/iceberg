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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.index.TrackingFileEntry;
import org.apache.iceberg.index.TrackingFileWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Focused unit test for {@link SparkScanBuilder#collectCandidateLeafFiles}, the piece of {@code
 * tryPruneUsingScalarIndex} responsible for deduping candidate leaf files across an {@code IN}
 * predicate's separate per-value target ranges. Verified directly against a real tracking file
 * (via {@link TrackingFileWriter}) rather than through a full Spark SQL query: the SQL-level
 * result is a {@code Set<String>} of resolved paths regardless of whether dedup happened
 * upstream, so a black-box query test cannot actually distinguish "dedup worked" from "dedup is
 * missing but the result is still correct by coincidence." Testing this method's return value
 * directly is the only way to actually prove the dedup, not just correctness under collision.
 */
public class TestSparkScanBuilderCandidateLeafFiles {

  @TempDir private File dir;

  private FileIO io;
  private String trackingFileLocation;

  @BeforeEach
  public void setup() {
    this.io = new HadoopFileIO(new Configuration());
    this.trackingFileLocation = new File(dir, "tracking.avro").toURI().toString();
  }

  private void writeTrackingFile(TrackingFileEntry... entries) {
    try (TrackingFileWriter writer = new TrackingFileWriter(io.newOutputFile(trackingFileLocation))) {
      for (TrackingFileEntry entry : entries) {
        writer.add(entry);
      }
    }
  }

  private static TrackingFileEntry entry(String location, long lower, long upper) {
    return TrackingFileEntry.builder()
        .location(location)
        .fileFormat("parquet")
        .recordCount(1)
        .fileSizeInBytes(10)
        .transformValueLowerBound(lower)
        .transformValueUpperBound(upper)
        .build();
  }

  @Test
  public void dedupesWhenTwoTargetRangesMatchTheSameLeafFileExactly() {
    // Simulates two IN-predicate values that both hash to the same HASH bucket.
    writeTrackingFile(entry("leaf-a.parquet", 0, 0), entry("leaf-b.parquet", 1, 1));

    List<TrackingFileEntry> candidates =
        SparkScanBuilder.collectCandidateLeafFiles(
            io,
            trackingFileLocation,
            ImmutableList.of(
                new SparkScanBuilder.TransformValueRange(0, 0),
                new SparkScanBuilder.TransformValueRange(0, 0)));

    assertThat(candidates).hasSize(1);
    assertThat(candidates.get(0).location()).isEqualTo("leaf-a.parquet");
  }

  @Test
  public void dedupesWhenTwoTargetRangesOverlapTheSameWiderLeafFile() {
    // A single leaf file spanning a wider bucket range [0, 5] can be the correct candidate for
    // several distinct target points within that range -- still one physical file to read once.
    writeTrackingFile(entry("leaf-wide.parquet", 0, 5));

    List<TrackingFileEntry> candidates =
        SparkScanBuilder.collectCandidateLeafFiles(
            io,
            trackingFileLocation,
            ImmutableList.of(
                new SparkScanBuilder.TransformValueRange(1, 1),
                new SparkScanBuilder.TransformValueRange(4, 4)));

    assertThat(candidates).hasSize(1);
    assertThat(candidates.get(0).location()).isEqualTo("leaf-wide.parquet");
  }

  @Test
  public void doesNotMergeDistinctLeafFilesFromDifferentRanges() {
    // Two target ranges landing in genuinely different leaf files must both be returned --
    // dedup is by location, not by collapsing every range into one result.
    writeTrackingFile(entry("leaf-a.parquet", 0, 0), entry("leaf-b.parquet", 1, 1));

    List<TrackingFileEntry> candidates =
        SparkScanBuilder.collectCandidateLeafFiles(
            io,
            trackingFileLocation,
            ImmutableList.of(
                new SparkScanBuilder.TransformValueRange(0, 0),
                new SparkScanBuilder.TransformValueRange(1, 1)));

    assertThat(candidates)
        .extracting(TrackingFileEntry::location)
        .containsExactlyInAnyOrder("leaf-a.parquet", "leaf-b.parquet");
  }
}
