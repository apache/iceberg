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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestManifestListVersions {
  private static final String PATH = "s3://bucket/table/m1.avro";
  private static final long LENGTH = 1024L;
  private static final int SPEC_ID = 1;
  private static final long SEQ_NUM = 34L;
  private static final long MIN_SEQ_NUM = 10L;
  private static final long SNAPSHOT_ID = 987134631982734L;
  private static final int ADDED_FILES = 2;
  private static final long ADDED_ROWS = 5292L;
  private static final int EXISTING_FILES = 343;
  private static final long EXISTING_ROWS = 857273L;
  private static final int DELETED_FILES = 1;
  private static final long DELETED_ROWS = 22910L;
  private static final List<ManifestFile.PartitionFieldSummary> PARTITION_SUMMARIES =
      ImmutableList.of();
  private static final ByteBuffer KEY_METADATA = null;

  private static final ManifestFile TEST_MANIFEST =
      new GenericManifestFile(
          PATH,
          LENGTH,
          SPEC_ID,
          ManifestContent.DATA,
          SEQ_NUM,
          MIN_SEQ_NUM,
          SNAPSHOT_ID,
          ADDED_FILES,
          ADDED_ROWS,
          EXISTING_FILES,
          EXISTING_ROWS,
          DELETED_FILES,
          DELETED_ROWS,
          PARTITION_SUMMARIES,
          KEY_METADATA);

  private static final ManifestFile TEST_DELETE_MANIFEST =
      new GenericManifestFile(
          PATH,
          LENGTH,
          SPEC_ID,
          ManifestContent.DELETES,
          SEQ_NUM,
          MIN_SEQ_NUM,
          SNAPSHOT_ID,
          ADDED_FILES,
          ADDED_ROWS,
          EXISTING_FILES,
          EXISTING_ROWS,
          DELETED_FILES,
          DELETED_ROWS,
          PARTITION_SUMMARIES,
          KEY_METADATA);

  @TempDir private Path temp;

  @Test
  public void testV1WriteDeleteManifest() {
    assertThatThrownBy(() -> writeManifestList(TEST_DELETE_MANIFEST, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot store delete manifests in a v1 table");
  }

  @Test
  public void testV1Write() throws IOException {
    ManifestFile manifest = writeAndReadManifestList(1);

    // v2 fields are not written and are defaulted
    assertThat(manifest.sequenceNumber()).isEqualTo(0);
    assertThat(manifest.minSequenceNumber()).isEqualTo(0);

    // v1 fields are read correctly, even though order changed
    assertThat(manifest.path()).isEqualTo(PATH);
    assertThat(manifest.length()).isEqualTo(LENGTH);
    assertThat(manifest.partitionSpecId()).isEqualTo(SPEC_ID);
    assertThat(manifest.content()).isEqualTo(ManifestContent.DATA);
    assertThat(manifest.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(manifest.addedFilesCount()).isEqualTo(ADDED_FILES);
    assertThat(manifest.existingFilesCount()).isEqualTo(EXISTING_FILES);
    assertThat(manifest.deletedFilesCount()).isEqualTo(DELETED_FILES);
    assertThat(manifest.addedRowsCount()).isEqualTo(ADDED_ROWS);
    assertThat(manifest.existingRowsCount()).isEqualTo(EXISTING_ROWS);
    assertThat(manifest.deletedRowsCount()).isEqualTo(DELETED_ROWS);
  }

  @Test
  public void testV2Write() throws IOException {
    ManifestFile manifest = writeAndReadManifestList(2);

    // all v2 fields should be read correctly
    assertThat(manifest.path()).isEqualTo(PATH);
    assertThat(manifest.length()).isEqualTo(LENGTH);
    assertThat(manifest.partitionSpecId()).isEqualTo(SPEC_ID);
    assertThat(manifest.content()).isEqualTo(ManifestContent.DATA);
    assertThat(manifest.sequenceNumber()).isEqualTo(SEQ_NUM);
    assertThat(manifest.minSequenceNumber()).isEqualTo(MIN_SEQ_NUM);
    assertThat(manifest.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(manifest.addedFilesCount()).isEqualTo(ADDED_FILES);
    assertThat(manifest.addedRowsCount()).isEqualTo(ADDED_ROWS);
    assertThat(manifest.existingFilesCount()).isEqualTo(EXISTING_FILES);
    assertThat(manifest.existingRowsCount()).isEqualTo(EXISTING_ROWS);
    assertThat(manifest.deletedFilesCount()).isEqualTo(DELETED_FILES);
    assertThat(manifest.deletedRowsCount()).isEqualTo(DELETED_ROWS);
  }

  @Test
  public void testV1ForwardCompatibility() throws IOException {
    InputFile manifestList = writeManifestList(TEST_MANIFEST, 1);
    GenericData.Record generic = readGeneric(manifestList, V1Metadata.MANIFEST_LIST_SCHEMA);

    // v1 metadata should match even though order changed
    assertThat(generic.get("manifest_path")).asString().isEqualTo(PATH);
    assertThat(generic.get("manifest_length")).isEqualTo(LENGTH);
    assertThat(generic.get("partition_spec_id")).isEqualTo(SPEC_ID);
    assertThat(generic.get("added_snapshot_id")).isEqualTo(SNAPSHOT_ID);
    assertThat(generic.get("added_files_count")).isEqualTo(ADDED_FILES);
    assertThat(generic.get("existing_files_count")).isEqualTo(EXISTING_FILES);
    assertThat(generic.get("deleted_files_count")).isEqualTo(DELETED_FILES);
    assertThat(generic.get("added_rows_count")).isEqualTo(ADDED_ROWS);
    assertThat(generic.get("existing_rows_count")).isEqualTo(EXISTING_ROWS);
    assertThat(generic.get("deleted_rows_count")).isEqualTo(DELETED_ROWS);
    assertEmptyAvroField(generic, ManifestFile.MANIFEST_CONTENT.name());
    assertEmptyAvroField(generic, ManifestFile.SEQUENCE_NUMBER.name());
    assertEmptyAvroField(generic, ManifestFile.MIN_SEQUENCE_NUMBER.name());
  }

  @Test
  public void testV2ForwardCompatibility() throws IOException {
    // v2 manifest list files can be read by v1 readers, but the sequence numbers and content will
    // be ignored.
    InputFile manifestList = writeManifestList(TEST_MANIFEST, 2);
    GenericData.Record generic = readGeneric(manifestList, V1Metadata.MANIFEST_LIST_SCHEMA);

    // v1 metadata should match even though order changed
    assertThat(generic.get("manifest_path")).asString().isEqualTo(PATH);
    assertThat(generic.get("manifest_length")).isEqualTo(LENGTH);
    assertThat(generic.get("partition_spec_id")).isEqualTo(SPEC_ID);
    assertThat(generic.get("added_snapshot_id")).isEqualTo(SNAPSHOT_ID);
    assertThat(generic.get("added_files_count")).isEqualTo(ADDED_FILES);
    assertThat(generic.get("existing_files_count")).isEqualTo(EXISTING_FILES);
    assertThat(generic.get("deleted_files_count")).isEqualTo(DELETED_FILES);
    assertThat(generic.get("added_rows_count")).isEqualTo(ADDED_ROWS);
    assertThat(generic.get("existing_rows_count")).isEqualTo(EXISTING_ROWS);
    assertThat(generic.get("deleted_rows_count")).isEqualTo(DELETED_ROWS);
    assertEmptyAvroField(generic, ManifestFile.MANIFEST_CONTENT.name());
    assertEmptyAvroField(generic, ManifestFile.SEQUENCE_NUMBER.name());
    assertEmptyAvroField(generic, ManifestFile.MIN_SEQUENCE_NUMBER.name());
  }

  @Test
  public void testManifestsWithoutRowStats() throws IOException {
    File manifestListFile = File.createTempFile("manifest-list.avro", null, temp.toFile());
    assertThat(manifestListFile.delete()).isTrue();

    Collection<String> columnNamesWithoutRowStats =
        ImmutableList.of(
            "manifest_path",
            "manifest_length",
            "partition_spec_id",
            "added_snapshot_id",
            "added_files_count",
            "existing_files_count",
            "deleted_files_count",
            "partitions");
    Schema schemaWithoutRowStats =
        V1Metadata.MANIFEST_LIST_SCHEMA.select(columnNamesWithoutRowStats);

    OutputFile outputFile = Files.localOutput(manifestListFile);
    try (FileAppender<GenericData.Record> appender =
        Avro.write(outputFile)
            .schema(schemaWithoutRowStats)
            .named("manifest_file")
            .overwrite()
            .build()) {

      org.apache.avro.Schema avroSchema =
          AvroSchemaUtil.convert(schemaWithoutRowStats, "manifest_file");
      GenericData.Record withoutRowStats =
          new GenericRecordBuilder(avroSchema)
              .set("manifest_path", "path/to/manifest.avro")
              .set("manifest_length", 1024L)
              .set("partition_spec_id", 1)
              .set("added_snapshot_id", 100L)
              .set("added_files_count", 2)
              .set("existing_files_count", 3)
              .set("deleted_files_count", 4)
              .set("partitions", null)
              .build();
      appender.add(withoutRowStats);
    }

    List<ManifestFile> files = ManifestLists.read(outputFile.toInputFile());
    ManifestFile manifest = Iterables.getOnlyElement(files);

    assertThat(manifest.hasAddedFiles()).isTrue();
    assertThat(manifest.addedFilesCount()).isEqualTo(2);
    assertThat(manifest.addedRowsCount()).isNull();
    assertThat(manifest.hasExistingFiles()).isTrue();
    assertThat(manifest.existingFilesCount()).isEqualTo(3);
    assertThat(manifest.existingRowsCount()).isNull();
    assertThat(manifest.hasDeletedFiles()).isTrue();
    assertThat(manifest.deletedFilesCount()).isEqualTo(4);
    assertThat(manifest.deletedRowsCount()).isNull();
  }

  @Test
  public void testManifestsPartitionSummary() throws IOException {
    ByteBuffer firstSummaryLowerBound = Conversions.toByteBuffer(Types.IntegerType.get(), 10);
    ByteBuffer firstSummaryUpperBound = Conversions.toByteBuffer(Types.IntegerType.get(), 100);
    ByteBuffer secondSummaryLowerBound = Conversions.toByteBuffer(Types.IntegerType.get(), 20);
    ByteBuffer secondSummaryUpperBound = Conversions.toByteBuffer(Types.IntegerType.get(), 200);

    List<ManifestFile.PartitionFieldSummary> partitionFieldSummaries =
        Lists.newArrayList(
            new GenericPartitionFieldSummary(false, firstSummaryLowerBound, firstSummaryUpperBound),
            new GenericPartitionFieldSummary(
                true, false, secondSummaryLowerBound, secondSummaryUpperBound));
    ManifestFile manifest =
        new GenericManifestFile(
            PATH,
            LENGTH,
            SPEC_ID,
            ManifestContent.DATA,
            SEQ_NUM,
            MIN_SEQ_NUM,
            SNAPSHOT_ID,
            ADDED_FILES,
            ADDED_ROWS,
            EXISTING_FILES,
            EXISTING_ROWS,
            DELETED_FILES,
            DELETED_ROWS,
            partitionFieldSummaries,
            KEY_METADATA);

    InputFile manifestList = writeManifestList(manifest, 2);

    List<ManifestFile> files = ManifestLists.read(manifestList);
    ManifestFile returnedManifest = Iterables.getOnlyElement(files);
    assertThat(returnedManifest.partitions()).hasSize(2);

    ManifestFile.PartitionFieldSummary first = returnedManifest.partitions().get(0);
    assertThat(first.containsNull()).isFalse();
    assertThat(first.containsNaN()).isNull();
    assertThat(first.lowerBound()).isEqualTo(firstSummaryLowerBound);
    assertThat(first.upperBound()).isEqualTo(firstSummaryUpperBound);

    ManifestFile.PartitionFieldSummary second = returnedManifest.partitions().get(1);
    assertThat(second.containsNull()).isTrue();
    assertThat(second.containsNaN()).isFalse();
    assertThat(second.lowerBound()).isEqualTo(secondSummaryLowerBound);
    assertThat(second.upperBound()).isEqualTo(secondSummaryUpperBound);
  }

  private InputFile writeManifestList(ManifestFile manifest, int formatVersion) throws IOException {
    OutputFile manifestList = new InMemoryOutputFile();
    try (FileAppender<ManifestFile> writer =
        ManifestLists.write(
            formatVersion,
            manifestList,
            SNAPSHOT_ID,
            SNAPSHOT_ID - 1,
            formatVersion > 1 ? SEQ_NUM : 0)) {
      writer.add(manifest);
    }
    return manifestList.toInputFile();
  }

  private GenericData.Record readGeneric(InputFile manifestList, Schema schema) throws IOException {
    try (CloseableIterable<GenericData.Record> files =
        Avro.read(manifestList).project(schema).reuseContainers(false).build()) {
      List<GenericData.Record> records = Lists.newLinkedList(files);
      assertThat(records).hasSize(1);
      return records.get(0);
    }
  }

  private ManifestFile writeAndReadManifestList(int formatVersion) throws IOException {
    List<ManifestFile> manifests =
        ManifestLists.read(writeManifestList(TEST_MANIFEST, formatVersion));
    assertThat(manifests).hasSize(1);
    return manifests.get(0);
  }

  private void assertEmptyAvroField(GenericRecord record, String field) {
    assertThatThrownBy(() -> record.get(field))
        .isInstanceOf(AvroRuntimeException.class)
        .hasMessage("Not a valid schema field: " + field);
  }
}
