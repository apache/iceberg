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
package org.apache.iceberg.index;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises the full SCALAR index chain as one flow: build leaf files, commit them through {@link
 * ScalarIndexCommitter}, then look up a key exactly the way a planner would -- compute its
 * transform value, narrow to candidate leaf files via the tracking file, then resolve the exact
 * row within those leaf files. Everything upstream and downstream of this (a real source table, a
 * real Spark build job, real query planning) is out of scope; this only proves the pieces we've
 * built work together correctly, not that they're wired into any engine yet.
 */
public class TestScalarIndexEndToEnd {

  @TempDir private Path tempDir;

  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("db"), "orders");
  private static final IndexIdentifier IDX = IndexIdentifier.of(TABLE, "order_id_idx");
  private static final String TABLE_UUID = "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94";
  private static final Types.NestedField KEY_FIELD =
      Types.NestedField.required(3, "order_id", Types.StringType.get());
  private static final int NUM_BUCKETS = 4;

  /** A row in the (fake) source table this index points into. */
  private record SourceRow(String orderId, String sourceFilePath, long position) {}

  @Test
  void buildCommitAndLookUpExactMatch() {
    List<SourceRow> sourceRows =
        List.of(
            new SourceRow("order-aaa", "s3://warehouse/orders/data-0.parquet", 10L),
            new SourceRow("order-bbb", "s3://warehouse/orders/data-0.parquet", 55L),
            new SourceRow("order-ccc", "s3://warehouse/orders/data-1.parquet", 3L),
            new SourceRow("order-ddd", "s3://warehouse/orders/data-1.parquet", 71L),
            new SourceRow("order-eee", "s3://warehouse/orders/data-2.parquet", 8L));

    HashTransform transform = new HashTransform(NUM_BUCKETS);
    String indexLocation = tempDir.toFile().getAbsolutePath() + "/index/order_id_idx";
    FileIO fileIO = new LocalFileIO();

    List<LeafFileEntry> allEntries =
        sourceRows.stream()
            .map(
                row ->
                    LeafFileEntry.builder()
                        .keyValue(row.orderId())
                        .transformValue(transform.apply(row.orderId()))
                        .filePath(row.sourceFilePath())
                        .position(row.position())
                        .build())
            .sorted(
                Comparator.comparingLong(LeafFileEntry::transformValue)
                    .thenComparing(e -> (String) e.keyValue()))
            .toList();

    // Split into two leaf files by transform value, the way a real build job would shard buckets
    // across multiple leaf files rather than writing one file per index.
    long splitPoint = NUM_BUCKETS / 2;
    List<LeafFileEntry> leafA =
        allEntries.stream().filter(e -> e.transformValue() < splitPoint).toList();
    List<LeafFileEntry> leafB =
        allEntries.stream().filter(e -> e.transformValue() >= splitPoint).toList();

    LeafFileMetadata metaA = writeLeafFile(fileIO, indexLocation, "leaf-a.parquet", leafA);
    LeafFileMetadata metaB = writeLeafFile(fileIO, indexLocation, "leaf-b.parquet", leafB);
    List<LeafFileMetadata> leafFiles = Lists.newArrayList(metaA, metaB);

    InMemoryIndexCatalog catalog = new InMemoryIndexCatalog();
    ScalarIndexCommitter committer = new ScalarIndexCommitter(catalog, fileIO);
    committer.commit(
        IDX,
        TABLE_UUID,
        1000L,
        "SCALAR",
        "HASH",
        ImmutableList.of(KEY_FIELD.fieldId()),
        indexLocation,
        leafFiles);

    // --- Look up "order-ddd" exactly the way a planner would ---
    String targetKey = "order-ddd";
    long targetBucket = transform.apply(targetKey);

    IndexMetadata metadata = catalog.loadIndex(IDX);
    String trackingPath = metadata.currentSnapshot().trackingFile();
    List<TrackingFileEntry> candidateLeafFiles =
        TrackingFileReader.readMatching(
            fileIO.newInputFile(trackingPath), targetBucket, targetBucket);
    assertThat(candidateLeafFiles).isNotEmpty();

    List<LeafFileEntry> matches = Lists.newArrayList();
    for (TrackingFileEntry leaf : candidateLeafFiles) {
      matches.addAll(
          LeafFileReader.readMatching(
              fileIO.newInputFile(leaf.location()),
              KEY_FIELD,
              Expressions.equal(KEY_FIELD.name(), targetKey)));
    }

    assertThat(matches).hasSize(1);
    assertThat(matches.get(0).keyValue()).isEqualTo("order-ddd");
    assertThat(matches.get(0).filePath()).isEqualTo("s3://warehouse/orders/data-1.parquet");
    assertThat(matches.get(0).position()).isEqualTo(71L);
  }

  @Test
  void lookUpMissingKeyReturnsNoMatch() {
    List<SourceRow> sourceRows =
        List.of(new SourceRow("order-aaa", "s3://warehouse/orders/data-0.parquet", 10L));

    HashTransform transform = new HashTransform(NUM_BUCKETS);
    String indexLocation = tempDir.toFile().getAbsolutePath() + "/index/missing_key_idx";
    FileIO fileIO = new LocalFileIO();

    List<LeafFileEntry> entries =
        sourceRows.stream()
            .map(
                row ->
                    LeafFileEntry.builder()
                        .keyValue(row.orderId())
                        .transformValue(transform.apply(row.orderId()))
                        .filePath(row.sourceFilePath())
                        .position(row.position())
                        .build())
            .sorted(
                Comparator.comparingLong(LeafFileEntry::transformValue)
                    .thenComparing(e -> (String) e.keyValue()))
            .toList();

    LeafFileMetadata leafMeta = writeLeafFile(fileIO, indexLocation, "leaf-0.parquet", entries);

    InMemoryIndexCatalog catalog = new InMemoryIndexCatalog();
    ScalarIndexCommitter committer = new ScalarIndexCommitter(catalog, fileIO);
    committer.commit(
        IDX,
        TABLE_UUID,
        1000L,
        "SCALAR",
        "HASH",
        ImmutableList.of(KEY_FIELD.fieldId()),
        indexLocation,
        Lists.newArrayList(leafMeta));

    String missingKey = "order-does-not-exist";
    long bucket = transform.apply(missingKey);

    IndexMetadata metadata = catalog.loadIndex(IDX);
    List<TrackingFileEntry> candidates =
        TrackingFileReader.readMatching(
            fileIO.newInputFile(metadata.currentSnapshot().trackingFile()), bucket, bucket);

    List<LeafFileEntry> matches = Lists.newArrayList();
    for (TrackingFileEntry leaf : candidates) {
      matches.addAll(
          LeafFileReader.readMatching(
              fileIO.newInputFile(leaf.location()),
              KEY_FIELD,
              Expressions.equal(KEY_FIELD.name(), missingKey)));
    }

    assertThat(matches).isEmpty();
  }

  private LeafFileMetadata writeLeafFile(
      FileIO fileIO, String indexLocation, String fileName, List<LeafFileEntry> entries) {
    String path = indexLocation + "/data/" + fileName;
    try (LeafFileWriter writer = new LeafFileWriter(fileIO.newOutputFile(path), KEY_FIELD)) {
      writer.addAll(entries);
    }
    long size = new File(path).length();
    long tvMin = entries.stream().mapToLong(LeafFileEntry::transformValue).min().orElse(0);
    long tvMax = entries.stream().mapToLong(LeafFileEntry::transformValue).max().orElse(0);
    return new LeafFileMetadata(path, "parquet", entries.size(), size, tvMin, tvMax);
  }

  /** Minimal local-filesystem FileIO, backed by real files since Parquet needs seekable I/O. */
  private static class LocalFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      return Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
      new File(path).delete();
    }
  }
}
