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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

public class TestManifestExpander {
  private final FileIO io = new InMemoryFileIO();

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  private static final Map<Integer, PartitionSpec> SPECS_BY_ID =
      ImmutableMap.of(SPEC.specId(), SPEC);

  private static final long SNAPSHOT_ID = 100L;
  private static final long FIRST_ROW_ID = 0L;

  @Test
  public void testExpandSingleLeafManifest() throws IOException {
    ManifestFile leaf = writeLeafManifest("a.parquet", "b.parquet", "c.parquet");
    List<FileScanTask> tasks = expand(writeRootManifest(leaf, null));

    assertThat(tasks).hasSize(3);
    assertThat(filePaths(tasks)).containsExactly("a.parquet", "b.parquet", "c.parquet");
  }

  @Test
  public void testExpandMultipleLeafManifests() throws IOException {
    ManifestFile leaf1 = writeLeafManifest("a.parquet");
    ManifestFile leaf2 = writeLeafManifest("b.parquet", "c.parquet");
    String rootPath = writeRootManifestMulti(ImmutableList.of(leaf1, leaf2), ImmutableList.of());

    List<FileScanTask> tasks = expand(rootPath);

    assertThat(tasks).hasSize(3);
    assertThat(filePaths(tasks)).containsExactly("a.parquet", "b.parquet", "c.parquet");
  }

  @Test
  public void testMetadataDVFiltersSinglePosition() throws IOException {
    ManifestFile leaf = writeLeafManifest("a.parquet", "b.parquet", "c.parquet");
    List<FileScanTask> tasks = expand(writeRootManifest(leaf, bitmap(1)));

    assertThat(tasks).hasSize(2);
    assertThat(filePaths(tasks)).containsExactly("a.parquet", "c.parquet");
  }

  @Test
  public void testMetadataDVFiltersMultiplePositions() throws IOException {
    ManifestFile leaf = writeLeafManifest("a.parquet", "b.parquet", "c.parquet", "d.parquet");
    List<FileScanTask> tasks = expand(writeRootManifest(leaf, bitmap(0, 2)));

    assertThat(tasks).hasSize(2);
    assertThat(filePaths(tasks)).containsExactly("b.parquet", "d.parquet");
  }

  @Test
  public void testMetadataDVDeletesAllPositions() throws IOException {
    ManifestFile leaf = writeLeafManifest("a.parquet", "b.parquet");
    List<FileScanTask> tasks = expand(writeRootManifest(leaf, bitmap(0, 1)));

    assertThat(tasks).isEmpty();
  }

  @Test
  public void testMetadataDVOnOneLeafNotAnother() throws IOException {
    ManifestFile leaf1 = writeLeafManifest("a.parquet", "b.parquet");
    ManifestFile leaf2 = writeLeafManifest("c.parquet", "d.parquet");

    // delete position 0 from leaf1, no DV on leaf2
    String rootPath =
        writeRootManifestMulti(ImmutableList.of(leaf1, leaf2), Lists.newArrayList(bitmap(0), null));

    List<FileScanTask> tasks = expand(rootPath);

    assertThat(tasks).hasSize(3);
    assertThat(filePaths(tasks)).containsExactly("b.parquet", "c.parquet", "d.parquet");
  }

  // --- helpers ---

  private List<FileScanTask> expand(String rootPath) {
    ManifestFile rootManifestFile = rootManifestFile(rootPath);
    ManifestExpander expander =
        new ManifestExpander(io, ImmutableList.of(rootManifestFile), SPECS_BY_ID);
    return Lists.newArrayList(expander.planFiles());
  }

  private List<String> filePaths(List<FileScanTask> tasks) {
    return Lists.transform(tasks, t -> t.file().location());
  }

  private ManifestFile rootManifestFile(String rootPath) {
    return new GenericManifestFile(
        rootPath,
        io.newInputFile(rootPath).getLength(),
        0,
        ManifestContent.DATA,
        1L,
        0L,
        SNAPSHOT_ID,
        null,
        null,
        0,
        0L,
        0,
        0L,
        0,
        0L,
        null);
  }

  private ManifestFile writeLeafManifest(String... filenames) throws IOException {
    String path = FileFormat.PARQUET.addExtension("leaf-" + System.nanoTime());
    ManifestWriter<DataFile> writer =
        ManifestFiles.newWriter(
            4,
            SPEC,
            PlaintextEncryptionManager.instance().encrypt(io.newOutputFile(path)),
            SNAPSHOT_ID,
            FIRST_ROW_ID);
    try {
      for (String filename : filenames) {
        writer.add(
            DataFiles.builder(SPEC)
                .withPath(filename)
                .withFileSizeInBytes(1024)
                .withRecordCount(10)
                .build());
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  private String writeRootManifest(ManifestFile leaf, ByteBuffer deletedPositions)
      throws IOException {
    return writeRootManifestMulti(ImmutableList.of(leaf), Lists.newArrayList(deletedPositions));
  }

  private String writeRootManifestMulti(
      List<ManifestFile> leaves, List<ByteBuffer> deletedPositionsList) throws IOException {
    String rootPath = FileFormat.PARQUET.addExtension("root-" + System.nanoTime());
    OutputFile rootOutput = io.newOutputFile(rootPath);
    Schema schema = V4Metadata.entrySchema(Types.StructType.of());

    try (FileAppender<StructLike> writer =
        InternalData.write(FileFormat.PARQUET, rootOutput)
            .schema(schema)
            .named("tracked_file")
            .meta("format-version", "4")
            .meta("content", "root")
            .overwrite()
            .build()) {
      for (int i = 0; i < leaves.size(); i = i + 1) {
        ManifestFile leaf = leaves.get(i);
        ByteBuffer deleted = i < deletedPositionsList.size() ? deletedPositionsList.get(i) : null;
        writer.add(buildRootEntry(leaf, deleted, schema));
      }
    }

    return rootPath;
  }

  private static TrackedFileStruct buildRootEntry(
      ManifestFile leaf, ByteBuffer deletedPositions, Schema schema) {
    TrackingStruct tracking = new TrackingStruct();
    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, SNAPSHOT_ID);
    tracking.set(2, 1L);
    tracking.set(3, 1L);
    if (deletedPositions != null) {
      tracking.set(6, deletedPositions);
    }

    int totalEntries =
        intOrZero(leaf.addedFilesCount())
            + intOrZero(leaf.existingFilesCount())
            + intOrZero(leaf.deletedFilesCount());

    ManifestInfoStruct info = new ManifestInfoStruct();
    info.set(0, intOrZero(leaf.addedFilesCount()));
    info.set(1, intOrZero(leaf.existingFilesCount()));
    info.set(2, intOrZero(leaf.deletedFilesCount()));
    info.set(3, 0);
    info.set(4, longOrZero(leaf.addedRowsCount()));
    info.set(5, longOrZero(leaf.existingRowsCount()));
    info.set(6, longOrZero(leaf.deletedRowsCount()));
    info.set(7, 0L);
    info.set(8, 1L);

    TrackedFileStruct tf = new TrackedFileStruct(schema.asStruct());
    tf.set(0, tracking);
    tf.set(1, FileContent.DATA_MANIFEST.id());
    tf.set(2, leaf.path());
    tf.set(3, FileFormat.PARQUET.toString());
    tf.set(4, (long) totalEntries);
    tf.set(5, leaf.length());
    tf.set(6, leaf.partitionSpecId());
    tf.set(9, info);

    return tf;
  }

  private static ByteBuffer bitmap(int... positions) throws IOException {
    RoaringBitmap bm = new RoaringBitmap();
    for (int pos : positions) {
      bm.add(pos);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    bm.serialize(new DataOutputStream(baos));
    return ByteBuffer.wrap(baos.toByteArray());
  }

  private static int intOrZero(Integer value) {
    return value != null ? value : 0;
  }

  private static long longOrZero(Long value) {
    return value != null ? value : 0L;
  }
}
