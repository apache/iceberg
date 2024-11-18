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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestManifestFileSerialization {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          required(3, "date", Types.StringType.get()),
          required(4, "double", Types.DoubleType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("double").build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(0)
          .withPartition(TestHelpers.Row.of(1D))
          .withPartitionPath("double=1")
          .withMetrics(
              new Metrics(
                  5L,
                  null, // no column sizes
                  ImmutableMap.of(1, 5L, 2, 3L), // value count
                  ImmutableMap.of(1, 0L, 2, 2L), // null count
                  ImmutableMap.of(), // nan count
                  ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(4L)) // upper bounds
                  ))
          .build();

  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-2.parquet")
          .withFileSizeInBytes(0)
          .withPartition(TestHelpers.Row.of(Double.NaN))
          .withPartitionPath("double=NaN")
          .withMetrics(
              new Metrics(
                  1L,
                  null, // no column sizes
                  ImmutableMap.of(1, 1L, 4, 1L), // value count
                  ImmutableMap.of(1, 0L, 2, 0L), // null count
                  ImmutableMap.of(4, 1L), // nan count
                  ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(1L)) // upper bounds
                  ))
          .build();

  private static final FileIO FILE_IO = new HadoopFileIO(new Configuration());

  @TempDir private Path temp;

  @Test
  public void testManifestFileKryoSerialization() throws IOException {
    File data = File.createTempFile("junit", null, temp.toFile());
    assertThat(data.delete()).isTrue();

    Kryo kryo = new KryoSerializer(new SparkConf()).newKryo();

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);

    try (Output out = new Output(new FileOutputStream(data))) {
      kryo.writeClassAndObject(out, manifest);
      kryo.writeClassAndObject(out, manifest.copy());
      kryo.writeClassAndObject(out, GenericManifestFile.copyOf(manifest).build());
    }

    try (Input in = new Input(new FileInputStream(data))) {
      for (int i = 0; i < 3; i += 1) {
        Object obj = kryo.readClassAndObject(in);
        assertThat(obj).as("Should be a ManifestFile").isInstanceOf(ManifestFile.class);
        checkManifestFile(manifest, (ManifestFile) obj);
      }
    }
  }

  @Test
  public void testManifestFileJavaSerialization() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);

    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(manifest);
      out.writeObject(manifest.copy());
      out.writeObject(GenericManifestFile.copyOf(manifest).build());
    }

    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      for (int i = 0; i < 3; i += 1) {
        Object obj = in.readObject();
        assertThat(obj).as("Should be a ManifestFile").isInstanceOf(ManifestFile.class);
        checkManifestFile(manifest, (ManifestFile) obj);
      }
    }
  }

  private void checkManifestFile(ManifestFile expected, ManifestFile actual) {
    assertThat(actual.path()).as("Path must match").isEqualTo(expected.path());
    assertThat(actual.length()).as("Length must match").isEqualTo(expected.length());
    assertThat(actual.partitionSpecId())
        .as("Spec id must match")
        .isEqualTo(expected.partitionSpecId());
    assertThat(actual.snapshotId()).as("Snapshot id must match").isEqualTo(expected.snapshotId());
    assertThat(actual.hasAddedFiles())
        .as("Added files flag must match")
        .isEqualTo(expected.hasAddedFiles());
    assertThat(actual.addedFilesCount())
        .as("Added files count must match")
        .isEqualTo(expected.addedFilesCount());
    assertThat(actual.addedRowsCount())
        .as("Added rows count must match")
        .isEqualTo(expected.addedRowsCount());
    assertThat(actual.hasExistingFiles())
        .as("Existing files flag must match")
        .isEqualTo(expected.hasExistingFiles());
    assertThat(actual.existingFilesCount())
        .as("Existing files count must match")
        .isEqualTo(expected.existingFilesCount());
    assertThat(actual.existingRowsCount())
        .as("Existing rows count must match")
        .isEqualTo(expected.existingRowsCount());
    assertThat(actual.hasDeletedFiles())
        .as("Deleted files flag must match")
        .isEqualTo(expected.hasDeletedFiles());
    assertThat(actual.deletedFilesCount())
        .as("Deleted files count must match")
        .isEqualTo(expected.deletedFilesCount());
    assertThat(actual.deletedRowsCount())
        .as("Deleted rows count must match")
        .isEqualTo(expected.deletedRowsCount());

    PartitionFieldSummary expectedPartition = expected.partitions().get(0);
    PartitionFieldSummary actualPartition = actual.partitions().get(0);

    assertThat(actualPartition.containsNull())
        .as("Null flag in partition must match")
        .isEqualTo(expectedPartition.containsNull());
    assertThat(actualPartition.containsNaN())
        .as("NaN flag in partition must match")
        .isEqualTo(expectedPartition.containsNaN());
    assertThat(actualPartition.lowerBound())
        .as("Lower bounds in partition must match")
        .isEqualTo(expectedPartition.lowerBound());
    assertThat(actualPartition.upperBound())
        .as("Upper bounds in partition must match")
        .isEqualTo(expectedPartition.upperBound());
  }

  private ManifestFile writeManifest(DataFile... files) throws IOException {
    File manifestFile = File.createTempFile("input.m0", ".avro", temp.toFile());
    assertThat(manifestFile.delete()).isTrue();
    OutputFile outputFile = FILE_IO.newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter<DataFile> writer = ManifestFiles.write(SPEC, outputFile);
    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}
