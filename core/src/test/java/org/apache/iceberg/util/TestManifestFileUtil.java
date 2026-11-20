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
package org.apache.iceberg.util;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

class TestManifestFileUtil {
  private static final int MIN_FORMAT_VERSION_PARQUET_MANIFESTS = 4;

  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()),
          optional(2, "unknown", Types.UnknownType.get()),
          optional(3, "floats", Types.FloatType.get()));

  private static final List<Type.PrimitiveType> BINARY_AND_FIXED =
      List.of(Types.BinaryType.get(), Types.FixedType.ofLength(3));
  private static final ByteBuffer BELOW = ByteBuffer.wrap(new byte[] {0x01, 0x00, 0x00});
  private static final ByteBuffer BOUND =
      ByteBuffer.wrap(new byte[] {(byte) 0xbc, (byte) 0xd1, 0x00});
  private static final ByteBuffer ABOVE =
      ByteBuffer.wrap(new byte[] {(byte) 0xe3, (byte) 0xbc, (byte) 0xd1});

  private final AtomicInteger manifestCounter = new AtomicInteger(0);

  @TempDir private Path temp;

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#V3_AND_BELOW")
  void canContainWithUnknownTypeOnly(int formatVersion) throws IOException {
    // Parquet cannot represent the empty struct produced by an UnknownType-only partition
    assumeThat(formatVersion).isLessThan(MIN_FORMAT_VERSION_PARQUET_MANIFESTS);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("unknown").build();
    PartitionData partition = new PartitionData(spec.partitionType());
    partition.set(0, "someValue");
    ManifestFile manifestFile = writeManifestWithDataFile(formatVersion, spec, partition);

    assertThat(
            ManifestFileUtil.canContainAny(
                manifestFile,
                ImmutableList.of(Pair.of(spec.specId(), partition)),
                ImmutableMap.of(spec.specId(), spec)))
        .isTrue();
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#V3_AND_BELOW")
  void canContainWithNaNValueOnly(int formatVersion) throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("floats").build();
    PartitionData partition = new PartitionData(spec.partitionType());
    partition.set(0, Float.NaN);
    ManifestFile manifestFile = writeManifestWithDataFile(formatVersion, spec, partition);

    assertThat(
            ManifestFileUtil.canContainAny(
                manifestFile,
                ImmutableList.of(Pair.of(spec.specId(), partition)),
                ImmutableMap.of(spec.specId(), spec)))
        .isTrue();
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#V3_AND_BELOW")
  void canContainWithNullValueOnly(int formatVersion) throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("floats").build();
    PartitionData partition = new PartitionData(spec.partitionType());
    partition.set(0, null);
    ManifestFile manifestFile = writeManifestWithDataFile(formatVersion, spec, partition);

    assertThat(
            ManifestFileUtil.canContainAny(
                manifestFile,
                ImmutableList.of(Pair.of(spec.specId(), partition)),
                ImmutableMap.of(spec.specId(), spec)))
        .isTrue();
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#V3_AND_BELOW")
  void canContainWithUnknownType(int formatVersion) throws IOException {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA).identity("floats").identity("unknown").build();
    PartitionData partition = new PartitionData(spec.partitionType());
    partition.set(0, 1.0f);
    partition.set(1, "someValue");
    ManifestFile manifestFile = writeManifestWithDataFile(formatVersion, spec, partition);

    assertThat(
            ManifestFileUtil.canContainAny(
                manifestFile,
                ImmutableList.of(Pair.of(spec.specId(), partition)),
                ImmutableMap.of(spec.specId(), spec)))
        .isTrue();
  }

  @ParameterizedTest
  @MethodSource("binaryAndFixedValuesOutsideBounds")
  void canContainIgnoresBinaryAndFixedBounds(Type.PrimitiveType type, ByteBuffer value) {
    assertThat(canContain(type, new TestHelpers.TestFieldSummary(false, BOUND, BOUND), value))
        .isTrue();
  }

  @ParameterizedTest
  @FieldSource("BINARY_AND_FIXED")
  void canContainPrunesBinaryAndFixedNullOnlySummaries(Type.PrimitiveType type) {
    assertThat(canContain(type, new TestHelpers.TestFieldSummary(true, null, null), ABOVE))
        .isFalse();
  }

  private static Stream<Arguments> binaryAndFixedValuesOutsideBounds() {
    return BINARY_AND_FIXED.stream()
        .flatMap(type -> Stream.of(BELOW, ABOVE).map(value -> Arguments.of(type, value)));
  }

  private static boolean canContain(
      Type.PrimitiveType type, ManifestFile.PartitionFieldSummary summary, ByteBuffer value) {
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("value").build();
    ManifestFile manifest =
        new TestHelpers.TestManifestFile(
            "manifest.avro", 1024, spec.specId(), null, 1, 0, 0, ImmutableList.of(summary), null);
    PartitionData partition = new PartitionData(spec.partitionType());
    partition.set(0, value);
    return ManifestFileUtil.canContainAny(
        manifest,
        ImmutableList.of(Pair.of(spec.specId(), partition)),
        ImmutableMap.of(spec.specId(), spec));
  }

  private ManifestFile writeManifestWithDataFile(
      int formatVersion, PartitionSpec spec, PartitionData partition) throws IOException {
    FileFormat format =
        formatVersion >= MIN_FORMAT_VERSION_PARQUET_MANIFESTS
            ? FileFormat.PARQUET
            : FileFormat.AVRO;
    String filename = format.addExtension("manifest-" + manifestCounter.getAndIncrement());
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(
            formatVersion, spec, Files.localOutput(temp.resolve(filename).toFile()), null);
    try (writer) {
      writer.add(
          DataFiles.builder(spec)
              .withPath("/path/to/data-a.parquet")
              .withFileSizeInBytes(10)
              .withPartition(partition)
              .withRecordCount(10)
              .build());
    }

    return writer.toManifestFile();
  }
}
