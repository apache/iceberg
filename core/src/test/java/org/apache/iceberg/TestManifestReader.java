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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestManifestReader extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestManifestReader(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testManifestReaderWithEmptyInheritableMetadata() throws IOException {
    ManifestFile manifest = writeManifest(1000L, manifestEntry(Status.EXISTING, 1000L, FILE_A));
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      Assert.assertEquals(Status.EXISTING, entry.status());
      Assert.assertEquals(FILE_A.path(), entry.file().path());
      Assert.assertEquals(1000L, (long) entry.snapshotId());
      Assertions.assertThat(entry.file().schemaId()).isNull();
    }
  }

  @Test
  public void testReaderWithFilterWithoutSelect() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE_A, FILE_B, FILE_C);
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO).filterRows(Expressions.equal("id", 0))) {
      List<String> files =
          Streams.stream(reader).map(file -> file.path().toString()).collect(Collectors.toList());

      // note that all files are returned because the reader returns data files that may match, and
      // the partition is
      // bucketing by data, which doesn't help filter files
      Assert.assertEquals(
          "Should read the expected files",
          Lists.newArrayList(FILE_A.path(), FILE_B.path(), FILE_C.path()),
          files);
    }
  }

  @Test
  public void testInvalidUsage() throws IOException {
    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    Assertions.assertThatThrownBy(() -> ManifestFiles.read(manifest, FILE_IO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot read from ManifestFile with null (unassigned) snapshot ID");
  }

  @Test
  public void testManifestReaderWithPartitionMetadata() throws IOException {
    ManifestFile manifest = writeManifest(1000L, manifestEntry(Status.EXISTING, 123L, FILE_A));
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      Assert.assertEquals(123L, (long) entry.snapshotId());

      List<Types.NestedField> fields =
          ((PartitionData) entry.file().partition()).getPartitionType().fields();
      Assert.assertEquals(1, fields.size());
      Assert.assertEquals(1000, fields.get(0).fieldId());
      Assert.assertEquals("data_bucket", fields.get(0).name());
      Assert.assertEquals(Types.IntegerType.get(), fields.get(0).type());
    }
  }

  @Test
  public void testManifestReaderWithUpdatedPartitionMetadataForV1Table() throws IOException {
    PartitionSpec spec =
        PartitionSpec.builderFor(table.schema()).bucket("id", 8).bucket("data", 16).build();
    table.ops().commit(table.ops().current(), table.ops().current().updatePartitionSpec(spec));

    ManifestFile manifest = writeManifest(1000L, manifestEntry(Status.EXISTING, 123L, FILE_A));
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      Assert.assertEquals(123L, (long) entry.snapshotId());

      List<Types.NestedField> fields =
          ((PartitionData) entry.file().partition()).getPartitionType().fields();
      Assert.assertEquals(2, fields.size());
      Assert.assertEquals(1000, fields.get(0).fieldId());
      Assert.assertEquals("id_bucket", fields.get(0).name());
      Assert.assertEquals(Types.IntegerType.get(), fields.get(0).type());

      Assert.assertEquals(1001, fields.get(1).fieldId());
      Assert.assertEquals("data_bucket", fields.get(1).name());
      Assert.assertEquals(Types.IntegerType.get(), fields.get(1).type());
    }
  }

  @Test
  public void testDataFilePositions() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE_A, FILE_B, FILE_C);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      long expectedPos = 0L;
      for (DataFile file : reader) {
        Assert.assertEquals("Position should match", (Long) expectedPos, file.pos());
        Assert.assertEquals(
            "Position from field index should match", expectedPos, ((BaseFile) file).get(18));
        expectedPos += 1;
      }
    }
  }

  @Test
  public void testDeleteFilePositions() throws IOException {
    Assume.assumeTrue("Delete files only work for format version 2", formatVersion == 2);
    ManifestFile manifest =
        writeDeleteManifest(formatVersion, 1000L, FILE_A_DELETES, FILE_B_DELETES);
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, FILE_IO, null)) {
      long expectedPos = 0L;
      for (DeleteFile file : reader) {
        Assert.assertEquals("Position should match", (Long) expectedPos, file.pos());
        Assert.assertEquals(
            "Position from field index should match", expectedPos, ((BaseFile) file).get(18));
        expectedPos += 1;
      }
    }
  }

  @Test
  public void testDataFileWithSchemaId() throws IOException {
    DataFile fileA = newDataFile("data_bucket=0");
    DataFile fileB = newDataFile("data_bucket=1");

    // test copy
    DataFile fileACopy = fileA.copy(true);
    assertFileEqual(fileA, fileACopy);
    DataFile fileBCopy = fileB.copy(true);
    assertFileEqual(fileB, fileBCopy);

    ManifestFile manifest = writeManifest(1000L, fileA, fileB);
    Map<Integer, PartitionSpec> specsById = ImmutableMap.of(SPEC.specId(), SPEC);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, specsById)) {
      List<DataFile> files = Lists.newArrayList(reader);
      Assertions.assertThat(files.size()).isEqualTo(2);
      assertFileEqual(fileA, files.get(0));
      assertFileEqual(fileB, files.get(1));
    }
  }

  @Test
  public void testDeleteFileWithSchemaId() throws IOException {
    Assume.assumeTrue("Delete files only work for format version 2", formatVersion == 2);
    DeleteFile deleteFileA = newDeleteFile(table.spec().specId(), "data_bucket=0");
    DeleteFile deleteFileB = newDeleteFile(table.spec().specId(), "data_bucket=1");

    // test copy
    DeleteFile deleteFileACopy = deleteFileA.copy(true);
    assertFileEqual(deleteFileA, deleteFileACopy);
    DeleteFile deleteFileBCopy = deleteFileB.copy(true);
    assertFileEqual(deleteFileB, deleteFileBCopy);

    ManifestFile manifest = writeDeleteManifest(formatVersion, 1000L, deleteFileA, deleteFileB);
    Map<Integer, PartitionSpec> specsById = ImmutableMap.of(SPEC.specId(), SPEC);
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, FILE_IO, specsById)) {
      List<DeleteFile> files = Lists.newArrayList(reader);
      Assertions.assertThat(files.size()).isEqualTo(2);
      assertFileEqual(deleteFileA, files.get(0));
      assertFileEqual(deleteFileB, files.get(1));
    }
  }

  @Test
  public void testReadOldManifestFile() throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "timestamp", Types.TimestampType.withZone()),
            required(3, "category", Types.StringType.get()),
            required(4, "data", Types.StringType.get()),
            required(5, "double", Types.DoubleType.get()));

    PartitionSpec spec =
        PartitionSpec.builderFor(schema)
            .identity("category")
            .hour("timestamp")
            .bucket("id", 16)
            .build();
    PartitionData partition = DataFiles.data(spec, "category=cheesy/timestamp_hour=10/id_bucket=3");

    String path = "s3://bucket/table/category=cheesy/timestamp_hour=10/id_bucket=3/file.avro";
    long fileSize = 150972L;
    FileFormat format = FileFormat.AVRO;

    Metrics metrics =
        new Metrics(
            1587L,
            ImmutableMap.of(1, 15L, 2, 122L, 3, 4021L, 4, 9411L, 5, 15L), // sizes
            ImmutableMap.of(1, 100L, 2, 100L, 3, 100L, 4, 100L, 5, 100L), // value counts
            ImmutableMap.of(1, 0L, 2, 0L, 3, 0L, 4, 0L, 5, 0L), // null value counts
            ImmutableMap.of(5, 10L), // nan value counts
            ImmutableMap.of(
                1, Conversions.toByteBuffer(Types.IntegerType.get(), 1)), // lower bounds
            ImmutableMap.of(
                1, Conversions.toByteBuffer(Types.IntegerType.get(), 1))); // upper bounds
    Integer sortOrderId = 2;

    String fileName = String.format("OldManifestFileV%s.avro", formatVersion);
    InputFile file =
        FILE_IO.newInputFile(getClass().getClassLoader().getResource(fileName).getPath());
    Map<Integer, PartitionSpec> specsById = ImmutableMap.of(spec.specId(), spec);
    try (ManifestReader<DataFile> reader =
        new ManifestReader<>(
            file,
            spec.specId(),
            specsById,
            InheritableMetadataFactory.empty(),
            ManifestReader.FileType.DATA_FILES)) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      DataFile dataFile = entry.file();
      Assertions.assertThat(dataFile.path()).isEqualTo(path);
      Assertions.assertThat(dataFile.format()).isEqualTo(format);
      Assertions.assertThat(dataFile.fileSizeInBytes()).isEqualTo(fileSize);
      Assertions.assertThat(dataFile.partition()).isEqualTo(partition);
      Assertions.assertThat(dataFile.keyMetadata()).isNull();
      Assertions.assertThat(dataFile.recordCount()).isEqualTo(metrics.recordCount());
      Assertions.assertThat(dataFile.columnSizes()).isEqualTo(metrics.columnSizes());
      Assertions.assertThat(dataFile.valueCounts()).isEqualTo(metrics.valueCounts());
      Assertions.assertThat(dataFile.nullValueCounts()).isEqualTo(metrics.nullValueCounts());
      Assertions.assertThat(dataFile.nanValueCounts()).isEqualTo(metrics.nanValueCounts());
      Assertions.assertThat(dataFile.lowerBounds()).isEqualTo(metrics.lowerBounds());
      Assertions.assertThat(dataFile.upperBounds()).isEqualTo(metrics.upperBounds());
      Assertions.assertThat(dataFile.sortOrderId()).isEqualTo(sortOrderId);
      Assertions.assertThat(dataFile.schemaId()).isNull();
    }
  }

  private void assertFileEqual(ContentFile<?> expected, ContentFile<?> actual) {
    Assertions.assertThat(actual.schemaId()).isEqualTo(expected.schemaId());
    Assertions.assertThat(actual.content()).isEqualTo(expected.content());
    Assertions.assertThat(actual.path().toString()).isEqualTo(expected.path().toString());
    Assertions.assertThat(actual.format()).isEqualTo(expected.format());
    Assertions.assertThat(actual.partition()).isEqualTo(expected.partition());
    Assertions.assertThat(actual.keyMetadata()).isEqualTo(expected.keyMetadata());
    Assertions.assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
    Assertions.assertThat(actual.columnSizes()).isEqualTo(expected.columnSizes());
    Assertions.assertThat(actual.valueCounts()).isEqualTo(expected.valueCounts());
    Assertions.assertThat(actual.nullValueCounts()).isEqualTo(expected.nullValueCounts());
    Assertions.assertThat(actual.nanValueCounts()).isEqualTo(expected.nanValueCounts());
    Assertions.assertThat(actual.lowerBounds()).isEqualTo(expected.lowerBounds());
    Assertions.assertThat(actual.upperBounds()).isEqualTo(expected.upperBounds());
    Assertions.assertThat(actual.sortOrderId()).isEqualTo(expected.sortOrderId());
  }
}
