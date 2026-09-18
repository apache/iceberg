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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.mockito.Mockito;

class TestV4ManifestReader {
  private static final ManifestFile UNREAD_MANIFEST_FILE = Mockito.mock(ManifestFile.class);

  static {
    Mockito.when(UNREAD_MANIFEST_FILE.path())
        .thenReturn(FileFormat.PARQUET.addExtension("manifest"));
    Mockito.when(UNREAD_MANIFEST_FILE.formatVersion()).thenReturn(4);
    Mockito.when(UNREAD_MANIFEST_FILE.content()).thenReturn(ManifestContent.DATA);
  }

  private static final long SNAPSHOT_ID = 42L;
  private static final int FORMAT_VERSION_V4 = 4;
  private static final long RECORD_COUNT = 100L;
  private static final long FILE_SIZE_IN_BYTES = 1024L;
  private static final DeletionVector DV = dv("s3://bucket/dv.puffin");

  private static final Tracking ADDED_TRACKING = TrackingBuilder.added(SNAPSHOT_ID).build();

  private static final ManifestInfo MANIFEST_INFO =
      new ManifestInfoStruct(49, 51, 0, 0, 4_900L, 5_100L, 0L, 0L, 1L, null);

  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final Schema LOCATION_ONLY_SCHEMA = new Schema(TrackedFile.LOCATION);

  private static final PartitionSpec ID_PARTITIONED =
      PartitionSpec.builderFor(TABLE_SCHEMA).withSpecId(1).identity("id").build();
  private static final Types.StructType ID_PARTITIONED_TYPE = ID_PARTITIONED.partitionType();
  private static final Map<Integer, PartitionSpec> ID_PARTITIONING_SPECS =
      ImmutableMap.of(ID_PARTITIONED.specId(), ID_PARTITIONED);

  private static final Types.StructType UNPARTITIONED_TYPE = Types.StructType.of();
  private static final Map<Integer, PartitionSpec> UNPARTITIONED_SPECS =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());

  private static final MetricsConfig METRICS_CONFIG =
      MetricsConfig.from(ImmutableMap.of(), TABLE_SCHEMA, null);
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsWriteSchema(TABLE_SCHEMA, METRICS_CONFIG);
  private static final Types.StructType ID_ONLY_STATS_TYPE =
      Types.StructType.of(STATS_TYPE.field("id"));
  private static final Types.StructType DATA_ONLY_STATS_TYPE =
      Types.StructType.of(STATS_TYPE.field("data"));

  private static final FieldStatsStruct<Integer> ID_STATS =
      new FieldStatsStruct<>(
          STATS_TYPE.fieldType("id").asStructType(), 0, 99, true, RECORD_COUNT, 0, 0, null);
  private static final FieldStatsStruct<String> DATA_STATS =
      new FieldStatsStruct<>(
          STATS_TYPE.fieldType("data").asStructType(), "a", "z", false, RECORD_COUNT, 20, 0, null);
  private static final ContentStatsStruct CONTENT_STATS = new ContentStatsStruct(STATS_TYPE);

  static {
    CONTENT_STATS.setStats(1, ID_STATS);
    CONTENT_STATS.setStats(2, DATA_STATS);
  }

  private static final Comparator<TrackedFile> FILE_COMPARATOR =
      V4TestComparators.trackedFileStatusOnly(ID_PARTITIONED_TYPE);
  private static final Comparator<TrackedFile> UNPARTITIONED_FILE_COMPARATOR =
      V4TestComparators.trackedFileStatusOnly(UNPARTITIONED_TYPE);

  // shared data files: FILE_A is in partition id=1, FILE_B in partition id=2
  private static final TrackedFile UNPARTITIONED_FILE =
      unpartitionedFileWithoutStats("s3://bucket/table/unpartitioned.parquet");
  private static final TrackedFile FILE_A =
      idPartitionedDataFileWithoutStats("s3://bucket/table/id=1/file-a.parquet", idPartition(1));
  private static final TrackedFile FILE_B =
      idPartitionedDataFileWithoutStats("s3://bucket/table/id=2/file-b.parquet", idPartition(2));
  private static final TrackedFile FILE_C =
      unpartitionedDataFileWithStats("s3://bucket/table/file-c.parquet", CONTENT_STATS);
  private static final TrackedFile FILE_D =
      unpartitionedFileWithoutStats("s3://bucket/table/file-d.parquet");
  private static final TrackedFile EQ_DELETES_A =
      idPartitionedDeleteFileWithoutStats(
          "s3://bucket/table/id=1/eq-deletes-a.parquet", idPartition(1));
  private static final TrackedFile EQ_DELETES_B =
      idPartitionedDeleteFileWithoutStats(
          "s3://bucket/table/id=2/eq-deletes-b.parquet", idPartition(2));
  private static final TrackedFile DATA_MANIFEST_REF =
      manifestRefWithoutStats(FileContent.DATA_MANIFEST, "s3://bucket/table/data-leaf.parquet");
  private static final TrackedFile DELETE_MANIFEST_REF =
      manifestRefWithoutStats(FileContent.DELETE_MANIFEST, "s3://bucket/table/delete-leaf.parquet");
  private static final TrackedFile DATA_MANIFEST_WITH_STATS_REF =
      manifestRefWithStats(
          FileContent.DATA_MANIFEST, "s3://bucket/table/metadata/data-leaf-stats.parquet");

  private static final List<FileFormat> MANIFEST_FORMATS =
      ImmutableList.of(FileFormat.AVRO, FileFormat.PARQUET);

  private static final InMemoryFileIO IO = new InMemoryFileIO();

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void readDataFile(FileFormat format) throws IOException {
    TrackedFile file =
        new TrackedFileStruct(
            ADDED_TRACKING,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            ID_PARTITIONED.specId(),
            idPartition(7),
            CONTENT_STATS,
            SortOrder.unsorted().orderId(),
            DV,
            null, // manifest info
            ByteBuffer.wrap(new byte[] {1, 2, 3}), // key metadata
            ImmutableList.of(50L, 100L),
            null); // equality field IDs

    ManifestFile manifest = writeManifest(format, ID_PARTITIONED_TYPE, file);

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .metricsConfig(METRICS_CONFIG);
    TrackedFile actual = readOne(builder);

    assertThat(actual).usingComparator(FILE_COMPARATOR).isEqualTo(file);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void readForScanPlanningDoesNotCopyStats(FileFormat format) throws IOException {
    TrackedFile file =
        new TrackedFileStruct(
            ADDED_TRACKING,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            ID_PARTITIONED.specId(),
            idPartition(7),
            CONTENT_STATS,
            SortOrder.unsorted().orderId(),
            DV,
            null, // manifest info
            ByteBuffer.wrap(new byte[] {1, 2, 3}), // key metadata
            ImmutableList.of(50L, 100L),
            null); // equality field IDs

    ManifestFile manifest = writeManifest(format, ID_PARTITIONED_TYPE, file);

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .forScanPlanning()
            .metricsConfig(METRICS_CONFIG);
    TrackedFile actual = readOne(builder);

    assertThat(actual).usingComparator(FILE_COMPARATOR).isEqualTo(file.copyWithoutStats());
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void readForScanPlanningCopiesRequestedStats(FileFormat format) throws IOException {
    int idFieldId = TABLE_SCHEMA.findField("id").fieldId();
    TrackedFile file =
        new TrackedFileStruct(
            ADDED_TRACKING,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            ID_PARTITIONED.specId(),
            idPartition(7),
            CONTENT_STATS,
            SortOrder.unsorted().orderId(),
            DV,
            null, // manifest info
            ByteBuffer.wrap(new byte[] {1, 2, 3}), // key metadata
            ImmutableList.of(50L, 100L),
            null); // equality field IDs

    ManifestFile manifest = writeManifest(format, ID_PARTITIONED_TYPE, file);

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .forScanPlanning()
            .metricsConfig(METRICS_CONFIG)
            .projectStats(idFieldId);
    TrackedFile actual = readOne(builder);

    assertThat(actual)
        .usingComparator(FILE_COMPARATOR)
        .isEqualTo(file.copyWithStats(Set.of(idFieldId)));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void readEqualityDelete(FileFormat format) throws IOException {
    TrackedFile delete =
        new TrackedFileStruct(
            ADDED_TRACKING,
            FileContent.EQUALITY_DELETES,
            FORMAT_VERSION_V4,
            "s3://bucket/eq-delete.parquet",
            FileFormat.PARQUET,
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            ID_PARTITIONED.specId(),
            idPartition(7),
            CONTENT_STATS,
            SortOrder.unsorted().orderId(),
            null, // dv
            null, // manifest info
            ByteBuffer.wrap(new byte[] {1, 2, 3}), // key metadata
            null, // split offsets
            ImmutableList.of(1, 2));

    ManifestFile manifest = writeManifest(format, ID_PARTITIONED_TYPE, delete);

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .metricsConfig(METRICS_CONFIG);
    TrackedFile actual = readOne(builder);

    assertThat(actual).usingComparator(FILE_COMPARATOR).isEqualTo(delete);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void readManifestFile(FileFormat format) throws IOException {
    TrackedFile manifestRef =
        new TrackedFileStruct(
            ADDED_TRACKING,
            FileContent.DATA_MANIFEST,
            FORMAT_VERSION_V4,
            "s3://bucket/leaf-manifest.parquet",
            FileFormat.PARQUET,
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            null, // spec id
            null, // partition
            CONTENT_STATS,
            null, // sort order id
            null, // dv
            MANIFEST_INFO,
            ByteBuffer.wrap(new byte[] {1, 2, 3}), // key metadata
            null, // split offsets
            ImmutableList.of(1, 2));

    ManifestFile manifest = writeManifest(format, ID_PARTITIONED_TYPE, manifestRef);

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .metricsConfig(METRICS_CONFIG);
    TrackedFile actual = readOne(builder);

    assertThat(actual).usingComparator(FILE_COMPARATOR).isEqualTo(manifestRef);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void statusFilter(FileFormat format) throws IOException {
    List<TrackedFile> files =
        ImmutableList.of(
            unpartitionedFileWithStatus(EntryStatus.ADDED, "s3://bucket/added.parquet"),
            unpartitionedFileWithStatus(EntryStatus.MODIFIED, "s3://bucket/modified.parquet"),
            unpartitionedFileWithStatus(EntryStatus.DELETED, "s3://bucket/deleted.parquet"),
            unpartitionedFileWithStatus(EntryStatus.EXISTING, "s3://bucket/existing.parquet"),
            unpartitionedFileWithStatus(EntryStatus.REPLACED, "s3://bucket/replaced.parquet"));

    ManifestFile manifest = writeManifest(format, UNPARTITIONED_TYPE, files);

    List<TrackedFile> liveFiles =
        read(
            V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                .metricsConfig(METRICS_CONFIG));
    assertThat(liveFiles)
        .usingComparatorForType(FILE_COMPARATOR, TrackedFile.class)
        .containsExactly(files.get(0), files.get(1), files.get(3));

    List<TrackedFile> allFiles =
        read(
            V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                .metricsConfig(METRICS_CONFIG)
                .includeAll());
    assertThat(allFiles)
        .usingComparatorForType(FILE_COMPARATOR, TrackedFile.class)
        .containsExactlyElementsOf(files);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void inheritanceManifestLocationAndPosition(FileFormat format) throws IOException {
    List<TrackedFile> files =
        ImmutableList.of(FILE_A, FILE_B, DATA_MANIFEST_REF, DELETE_MANIFEST_REF);

    ManifestFile manifest = writeManifest(format, UNPARTITIONED_TYPE, files);

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .metricsConfig(METRICS_CONFIG);
    List<TrackedFile> read = read(builder);

    assertThat(read)
        .allSatisfy(
            file -> assertThat(file.tracking().manifestLocation()).isEqualTo(manifest.path()));
    assertThat(read)
        .extracting(file -> file.tracking().manifestPos())
        .containsExactly(0L, 1L, 2L, 3L);
  }

  @Test
  public void projectionFullByDefault() {
    Types.StructType readSchema =
        V4ManifestReader.builder(UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .metricsConfig(METRICS_CONFIG)
            .filter(Expressions.equal("id", 5)) // does not cause stats to be filtered
            .build()
            .readSchema()
            .asStruct();

    Types.StructType expected =
        TypeUtil.replaceFieldTypes(
                TrackedFile.schema(ID_PARTITIONED_TYPE, STATS_TYPE),
                ImmutableMap.of(TrackedFile.TRACKING.fieldId(), TrackingStruct.BASE_TYPE))
            .asStruct();

    assertThat(readSchema)
        .as("No projection configuration should project the full table manifest schema")
        .isEqualTo(expected);
  }

  @Test
  public void projectionDependsOnMetricsConfig() {
    MetricsConfig metricsWithoutID =
        MetricsConfig.from(
            ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id", "none"),
            TABLE_SCHEMA,
            null);

    Types.StructType readSchema =
        V4ManifestReader.builder(UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .metricsConfig(metricsWithoutID)
            .build()
            .readSchema()
            .asStruct();

    Types.StructType expected =
        TypeUtil.replaceFieldTypes(
                TrackedFile.schema(ID_PARTITIONED_TYPE, DATA_ONLY_STATS_TYPE),
                ImmutableMap.of(TrackedFile.TRACKING.fieldId(), TrackingStruct.BASE_TYPE))
            .asStruct();

    assertThat(readSchema)
        .as("Scan planning configuration should automatically prune tracking and stats")
        .isEqualTo(expected);
  }

  @Test
  public void projectionForScanPlanning() {
    Types.StructType readSchema =
        V4ManifestReader.builder(UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .forScanPlanning()
            .filter(Expressions.equal("id", 5))
            .metricsConfig(METRICS_CONFIG)
            .build()
            .readSchema()
            .asStruct();

    Types.StructType expected =
        TypeUtil.replaceFieldTypes(
                TrackedFile.schema(ID_PARTITIONED_TYPE, ID_ONLY_STATS_TYPE),
                ImmutableMap.of(TrackedFile.TRACKING.fieldId(), TrackingStruct.SCAN_TYPE))
            .asStruct();

    assertThat(readSchema)
        .as("Scan planning configuration should automatically prune tracking and stats")
        .isEqualTo(expected);
  }

  @Test
  public void projectionForScanPlanningOverridesMetricsConfig() {
    MetricsConfig metricsWithoutID =
        MetricsConfig.from(
            ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id", "none"),
            TABLE_SCHEMA,
            null);

    Types.StructType readSchema =
        V4ManifestReader.builder(UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .forScanPlanning()
            .filter(Expressions.equal("id", 5))
            .metricsConfig(metricsWithoutID)
            .build()
            .readSchema()
            .asStruct();

    Types.StructType expected =
        TypeUtil.replaceFieldTypes(
                TrackedFile.schema(ID_PARTITIONED_TYPE, ID_ONLY_STATS_TYPE),
                ImmutableMap.of(TrackedFile.TRACKING.fieldId(), TrackingStruct.SCAN_TYPE))
            .asStruct();

    assertThat(readSchema)
        .as("Scan planning configuration should automatically prune tracking and stats")
        .isEqualTo(expected);
  }

  @Test
  public void projectionForScanPlanningIncludesRequestedStatsMetricsConfig() {
    MetricsConfig metricsWithoutID =
        MetricsConfig.from(
            ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id", "none"),
            TABLE_SCHEMA,
            null);

    Types.StructType readSchema =
        V4ManifestReader.builder(UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .forScanPlanning()
            .filter(Expressions.equal("id", 5))
            .projectStats(TABLE_SCHEMA.findField("data").fieldId())
            .metricsConfig(metricsWithoutID)
            .build()
            .readSchema()
            .asStruct();

    Types.StructType expected =
        TypeUtil.replaceFieldTypes(
                TrackedFile.schema(ID_PARTITIONED_TYPE, STATS_TYPE),
                ImmutableMap.of(TrackedFile.TRACKING.fieldId(), TrackingStruct.SCAN_TYPE))
            .asStruct();

    assertThat(readSchema)
        .as("Scan planning configuration should automatically prune tracking and stats")
        .isEqualTo(expected);
  }

  private static final List<Named<Consumer<V4ManifestReader.Builder>>> PROJECTION_CASES =
      ImmutableList.of(
          Named.of("select", builder -> builder.select("location")),
          Named.of(
              "case-insensitive select",
              builder -> builder.select("LOCATION").caseSensitive(false)),
          Named.of("project", builder -> builder.project(LOCATION_ONLY_SCHEMA)));

  @ParameterizedTest
  @FieldSource("PROJECTION_CASES")
  public void projectionCustomization(Consumer<V4ManifestReader.Builder> config) {
    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .metricsConfig(METRICS_CONFIG);

    config.accept(builder);

    Types.StructType readSchema = builder.build().readSchema().asStruct();

    assertThat(readSchema.field(TrackedFile.LOCATION.fieldId()))
        .as("Projected field 'location' should be present")
        .isEqualTo(TrackedFile.LOCATION);

    assertThat(readSchema.field(TrackedFile.KEY_METADATA.fieldId()))
        .as("Unselected, non-required field should be omitted")
        .isNull();

    assertThat(readSchema.field(TrackedFile.RECORD_COUNT.fieldId()))
        .as("Required field 'record_count' should be present")
        .isEqualTo(TrackedFile.RECORD_COUNT);

    assertThat(readSchema.field(TrackedFile.PARTITION_ID))
        .as("Partition is not automatically projected")
        .isNull();

    assertThat(readSchema.field(TrackedFile.CONTENT_STATS_ID))
        .as("Content stats are not automatically projected")
        .isNull();
  }

  @ParameterizedTest
  @FieldSource("PROJECTION_CASES")
  public void projectionCustomizationWithFilter(Consumer<V4ManifestReader.Builder> config) {
    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .filter(Expressions.equal("id", 5))
            .metricsConfig(METRICS_CONFIG);

    config.accept(builder);

    Types.StructType readSchema = builder.build().readSchema().asStruct();

    assertThat(readSchema.field(TrackedFile.LOCATION.fieldId()))
        .as("Projected field 'location' should be present")
        .isEqualTo(TrackedFile.LOCATION);

    assertThat(readSchema.field(TrackedFile.KEY_METADATA.fieldId()))
        .as("Unselected, non-required field should be omitted")
        .isNull();

    assertThat(readSchema.field(TrackedFile.RECORD_COUNT.fieldId()))
        .as("Required field 'record_count' should be present")
        .isEqualTo(TrackedFile.RECORD_COUNT);

    assertThat(readSchema.field(TrackedFile.PARTITION_ID).type())
        .as("Partition is projected for filtering")
        .isEqualTo(ID_PARTITIONED_TYPE);

    assertThat(readSchema.field(TrackedFile.CONTENT_STATS_ID).type())
        .as("Content stats are projected for filtering")
        .isEqualTo(ID_ONLY_STATS_TYPE);
  }

  @Test
  public void projectionModesAreMutuallyExclusive() {
    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .select("location")
                    .project(LOCATION_ONLY_SCHEMA))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot use project(Schema) with select(Iterable<String>)");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .project(LOCATION_ONLY_SCHEMA)
                    .select("location"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot use select(Iterable<String>) with project(Schema)");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .forScanPlanning()
                    .select("location"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot use select(Iterable<String>) with forScanPlanning()");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .select("location")
                    .forScanPlanning())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot use forScanPlanning() with select(Iterable<String>) or project(Schema)");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .forScanPlanning()
                    .project(LOCATION_ONLY_SCHEMA))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot use project(Schema) with forScanPlanning()");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .project(LOCATION_ONLY_SCHEMA)
                    .forScanPlanning())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot use forScanPlanning() with select(Iterable<String>) or project(Schema)");
  }

  @Test
  public void projectionProjectNullFailure() {
    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .project(null))
        .hasMessage("Invalid projection: null")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void projectionSelectNullFailure() {
    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .select((String[]) null))
        .hasMessage("Invalid columns: null")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .select((Collection<String>) null))
        .hasMessage("Invalid columns: null")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void projectionStatsProjectionNullFailure() {
    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .projectStats((int[]) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: null");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .projectStats((Iterable<Integer>) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: null");
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void projectionUnpartitionedFile(FileFormat format) throws IOException {
    ManifestFile manifest = writeManifest(format, UNPARTITIONED_TYPE, UNPARTITIONED_FILE);

    TrackedFile actual =
        readOne(
            V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                .metricsConfig(METRICS_CONFIG));

    assertThat(actual).usingComparator(UNPARTITIONED_FILE_COMPARATOR).isEqualTo(UNPARTITIONED_FILE);

    assertThat(actual.specId()).isNull();
    assertThat(actual.partition()).isNull();
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void projectionUnpartitionedWithPartitionedRead(FileFormat format) throws IOException {
    ManifestFile manifest = writeManifest(format, UNPARTITIONED_TYPE, UNPARTITIONED_FILE);

    TrackedFile actual =
        readOne(
            V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
                .metricsConfig(METRICS_CONFIG));

    assertThat(actual).usingComparator(FILE_COMPARATOR).isEqualTo(UNPARTITIONED_FILE);

    assertThat(actual.specId()).isNull();
    assertThat(actual.partition()).isNull();
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void projectionUnpartitionedInPartitionedManifest(FileFormat format) throws IOException {
    ManifestFile manifest =
        writeManifest(format, ID_PARTITIONED_TYPE, ImmutableList.of(UNPARTITIONED_FILE, FILE_A));

    List<TrackedFile> actualFiles =
        read(
            V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
                .metricsConfig(METRICS_CONFIG));

    TrackedFile actualUnpartitioned = actualFiles.get(0);
    TrackedFile actualPartitioned = actualFiles.get(1);

    assertThat(actualUnpartitioned).usingComparator(FILE_COMPARATOR).isEqualTo(UNPARTITIONED_FILE);
    assertThat(actualPartitioned).usingComparator(FILE_COMPARATOR).isEqualTo(FILE_A);

    assertThat(actualUnpartitioned.specId()).isNull();
    assertThat(actualUnpartitioned.partition()).isNull();
  }

  @Test
  public void statsFilterMissingColumnFailure() {
    // stats for the filter's columns are resolved against the table schema
    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .forScanPlanning()
            .filter(Expressions.equal("missing", 34))
            .metricsConfig(METRICS_CONFIG);

    assertThatThrownBy(() -> read(builder))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'missing' in struct: %s", TABLE_SCHEMA.asStruct());
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void statsFilterRecordCountFiltering(FileFormat format) throws IOException {
    TrackedFile emptyTrackedFile =
        new TrackedFileStruct(
            ADDED_TRACKING,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/table/empty-file.parquet",
            FileFormat.PARQUET,
            0, // file contains no records
            100L,
            null,
            null,
            null,
            SortOrder.unsorted().orderId(),
            null,
            null,
            null,
            List.of(4L),
            null);

    ManifestFile manifest =
        writeManifest(format, UNPARTITIONED_TYPE, ImmutableList.of(emptyTrackedFile, FILE_D));

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .metricsConfig(METRICS_CONFIG);

    List<TrackedFile> actualFiles = read(builder);

    assertThat(actualFiles)
        .usingComparatorForType(FILE_COMPARATOR, TrackedFile.class)
        .containsExactly(FILE_D);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void statsFilterInvalidRecordCountNotFiltered(FileFormat format) throws IOException {
    // A bug in old writers produced Avro files with record_count=-1 (unknown)
    TrackedFile invalidRecordCountFile =
        new TrackedFileStruct(
            ADDED_TRACKING,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/table/very-old.avro",
            FileFormat.AVRO,
            -1, // mimic invalid record count in old Avro metadata
            100L,
            null,
            null,
            null,
            SortOrder.unsorted().orderId(),
            null,
            null,
            null,
            List.of(4L),
            null);

    ManifestFile manifest =
        writeManifest(format, UNPARTITIONED_TYPE, ImmutableList.of(invalidRecordCountFile, FILE_D));

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .metricsConfig(METRICS_CONFIG);

    List<TrackedFile> actualFiles = read(builder);

    assertThat(actualFiles)
        .usingComparatorForType(FILE_COMPARATOR, TrackedFile.class)
        .containsExactly(invalidRecordCountFile, FILE_D);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void statsFilterDataFileBoundsFiltering(FileFormat format) throws IOException {
    // FILE_C has stats {id in [0, 99], data in [a, z]}, FILE_D has no stats
    ManifestFile manifest =
        writeManifest(format, UNPARTITIONED_TYPE, ImmutableList.of(FILE_C, FILE_D));

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .filter(Expressions.equal("id", 105)) // eliminates FILE_C
            .metricsConfig(METRICS_CONFIG);

    List<TrackedFile> actualFiles = read(builder);

    assertThat(actualFiles)
        .usingComparatorForType(FILE_COMPARATOR, TrackedFile.class)
        .containsExactly(FILE_D);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void statsFilterManifestBoundsFiltering(FileFormat format) throws IOException {
    // DATA_MANIFEST_WITH_STATS_REF has stats {id in [0, 99], data in [a, z]}
    ManifestFile manifest =
        writeManifest(
            format,
            UNPARTITIONED_TYPE,
            ImmutableList.of(DATA_MANIFEST_REF, DATA_MANIFEST_WITH_STATS_REF));

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .filter(Expressions.equal("id", 105)) // eliminates the manifest with stats
            .metricsConfig(METRICS_CONFIG);

    List<TrackedFile> actualFiles = read(builder);

    assertThat(actualFiles)
        .usingComparatorForType(FILE_COMPARATOR, TrackedFile.class)
        .containsExactly(DATA_MANIFEST_REF);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void statsFilterBoundsFilteringWithForScanPlanning(FileFormat format) throws IOException {
    // FILE_C has stats {id in [0, 99], data in [a, z]}, FILE_D has no stats
    ManifestFile manifest =
        writeManifest(format, UNPARTITIONED_TYPE, ImmutableList.of(FILE_C, FILE_D));

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .forScanPlanning() // does not project unused stats
            .filter(Expressions.equal("id", 105)) // eliminates FILE_C
            .metricsConfig(METRICS_CONFIG);

    List<TrackedFile> actualFiles = read(builder);

    assertThat(actualFiles)
        .usingComparatorForType(FILE_COMPARATOR, TrackedFile.class)
        .containsExactly(FILE_D);
  }

  @ParameterizedTest
  @FieldSource("PROJECTION_CASES")
  public void statsFilterBoundsFilteringWithProjection(Consumer<V4ManifestReader.Builder> config)
      throws IOException {
    // FILE_C has stats {id in [0, 99], data in [a, z]}, FILE_D has no stats
    // config projects just the file location, but stats are automatically projected for the filter
    ManifestFile manifest =
        writeManifest(FileFormat.PARQUET, UNPARTITIONED_TYPE, ImmutableList.of(FILE_C, FILE_D));

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .filter(Expressions.equal("id", 105)) // eliminates FILE_C
            .metricsConfig(METRICS_CONFIG);

    config.accept(builder);

    List<TrackedFile> actualFiles = read(builder);

    assertThat(actualFiles).extracting(TrackedFile::location).containsExactly(FILE_D.location());
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void statsFilterCaseSensitivity(FileFormat format) throws IOException {
    // FILE_C has stats {id in [0, 99], data in [a, z]}, FILE_D has no stats
    ManifestFile manifest =
        writeManifest(format, UNPARTITIONED_TYPE, ImmutableList.of(FILE_C, FILE_D));

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .filter(Expressions.equal("ID", 105)) // eliminates FILE_C
            .metricsConfig(METRICS_CONFIG);

    assertThatThrownBy(() -> read(builder))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'ID' in struct");

    List<TrackedFile> actualFiles = read(builder.caseSensitive(false));

    assertThat(actualFiles)
        .usingComparatorForType(FILE_COMPARATOR, TrackedFile.class)
        .containsExactly(FILE_D);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void statsFilterBoundsFilteringUpdatesMetricsSkipCounts(FileFormat format)
      throws IOException {
    ManifestFile manifest =
        writeManifest(
            format,
            UNPARTITIONED_TYPE,
            ImmutableList.of(DATA_MANIFEST_REF, DATA_MANIFEST_WITH_STATS_REF, FILE_C, FILE_D));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .scanMetrics(metrics)
            .filter(Expressions.equal("id", 105)) // eliminates manifest with stats and FILE_C
            .metricsConfig(METRICS_CONFIG);

    List<TrackedFile> actualFiles = read(builder);

    assertThat(actualFiles)
        .usingComparatorForType(FILE_COMPARATOR, TrackedFile.class)
        .containsExactly(DATA_MANIFEST_REF, FILE_D);

    assertThat(metrics.skippedDataFiles().value()).as("FILE_C should be pruned").isEqualTo(1L);
    assertThat(metrics.skippedDeleteFiles().value()).as("No deletes are present").isEqualTo(0L);
    assertThat(metrics.skippedDataManifests().value())
        .as("Manifest with stats should be pruned")
        .isEqualTo(1L);
    assertThat(metrics.skippedDeleteManifests().value())
        .as("No delete manifests are present")
        .isEqualTo(0L);
    assertThat(metrics.scannedDataManifests().value())
        .as("The root manifest was scanned")
        .isEqualTo(1L);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void partitionFilterBucketPartitionID(FileFormat format) throws IOException {
    PartitionSpec bucketSpec =
        PartitionSpec.builderFor(TABLE_SCHEMA).withSpecId(2).bucket("id", 100).build();

    Map<Integer, PartitionSpec> bucketSpecs = ImmutableMap.of(bucketSpec.specId(), bucketSpec);

    int fileBucketNum = 7;

    TrackedFile bucketPartitionedFile =
        new TrackedFileStruct(
            ADDED_TRACKING,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/table/bucket=7/data-file.parquet",
            FileFormat.AVRO,
            10,
            100L,
            bucketSpec.specId(),
            partition(bucketSpec, fileBucketNum),
            CONTENT_STATS,
            SortOrder.unsorted().orderId(),
            null, // dv
            null, // manifest info
            null, // key metadata
            List.of(4L),
            null); // eq delete ids

    ManifestFile manifest =
        writeManifest(
            format, bucketSpec.partitionType(), ImmutableList.of(bucketPartitionedFile, FILE_D));

    int queryId = 34; // in the content stats range for id: [0, 99]

    Transform<Integer, Integer> bucket100 = Transforms.bucket(100);
    int queryBucketNum = bucket100.bind(Types.IntegerType.get()).apply(queryId);
    assertThat(queryBucketNum)
        .as("Query filter ID should NOT be in the data file bucket")
        .isNotEqualTo(fileBucketNum);

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, bucketSpecs)
            .filter(Expressions.equal("id", queryId))
            .metricsConfig(METRICS_CONFIG);

    List<TrackedFile> actualFiles = read(builder);

    assertThat(actualFiles)
        .usingComparatorForType(FILE_COMPARATOR, TrackedFile.class)
        .containsExactly(FILE_D);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void partitionFilterUpdatesScanMetricsSkipCounts(FileFormat format) throws IOException {
    ManifestFile manifest =
        writeManifest(
            format,
            ID_PARTITIONED_TYPE,
            ImmutableList.of(
                FILE_A,
                FILE_B,
                EQ_DELETES_A,
                EQ_DELETES_B,
                DATA_MANIFEST_REF,
                DELETE_MANIFEST_REF));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .filter(Expressions.equal("id", 1))
            .metricsConfig(METRICS_CONFIG)
            .scanMetrics(metrics);

    assertThat(read(builder))
        .usingComparatorForType(FILE_COMPARATOR, TrackedFile.class)
        .containsExactly(FILE_A, EQ_DELETES_A, DATA_MANIFEST_REF, DELETE_MANIFEST_REF);

    assertThat(metrics.skippedDataFiles().value())
        .as("one data file is pruned by the partition filter")
        .isEqualTo(1L);
    assertThat(metrics.skippedDeleteFiles().value())
        .as("one delete file is pruned by the partition filter")
        .isEqualTo(1L);
    assertThat(metrics.skippedDataManifests().value())
        .as("manifests have no partition and are not pruned")
        .isEqualTo(0L);
    assertThat(metrics.skippedDeleteManifests().value())
        .as("manifests have no partition and are not pruned")
        .isEqualTo(0L);
    assertThat(metrics.scannedDataManifests().value()).as("one manifest is scanned").isEqualTo(1L);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void partitionFilterCaseSensitivity(FileFormat format) throws IOException {
    ManifestFile manifest =
        writeManifest(format, ID_PARTITIONED_TYPE, ImmutableList.of(FILE_A, FILE_B));

    Expression filter = Expressions.equal("ID", 1);

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .filter(filter)
            .metricsConfig(METRICS_CONFIG);

    assertThatThrownBy(() -> read(builder))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'ID'");

    assertThat(readOne(builder.caseSensitive(false)))
        .usingComparator(FILE_COMPARATOR)
        .isEqualTo(FILE_A);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void partitionFilterWithMultipleSpecs(FileFormat format) throws IOException {
    PartitionSpec idSpec =
        PartitionSpec.builderFor(TABLE_SCHEMA)
            .withSpecId(1)
            .add(1, 1000, "id", Transforms.identity())
            .build();
    PartitionSpec dataSpec =
        PartitionSpec.builderFor(TABLE_SCHEMA)
            .withSpecId(2)
            .add(2, 1001, "data", Transforms.identity())
            .build();
    Map<Integer, PartitionSpec> specsById =
        ImmutableMap.of(idSpec.specId(), idSpec, dataSpec.specId(), dataSpec);

    PartitionData dataPartitionX = partition(dataSpec, "x");
    TrackedFile dataPartitionedFile =
        dataFileWithoutStats(
            "s3://bucket/table/data=x/file-c.parquet", dataSpec.specId(), dataPartitionX);

    ManifestFile idPartitionedManifest =
        writeManifest(format, idSpec.partitionType(), ImmutableList.of(FILE_A, FILE_B));
    ManifestFile dataPartitionedManifest =
        writeManifest(format, dataSpec.partitionType(), dataPartitionedFile);

    List<TrackedFile> files = Lists.newArrayList();
    files.addAll(
        read(
            V4ManifestReader.builder(idPartitionedManifest, IO, TABLE_SCHEMA, specsById)
                .metricsConfig(METRICS_CONFIG)));
    files.addAll(
        read(
            V4ManifestReader.builder(dataPartitionedManifest, IO, TABLE_SCHEMA, specsById)
                .metricsConfig(METRICS_CONFIG)));

    Types.StructType unionType = Partitioning.unionPartitionTypes(specsById.values());

    ManifestFile manifest = writeManifest(format, unionType, files);

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, specsById)
            .filter(Expressions.equal("id", 1))
            .metricsConfig(METRICS_CONFIG);

    // the comparator is built for ID partitioning, so only check the location
    assertThat(read(builder))
        .extracting(TrackedFile::location)
        .containsExactlyInAnyOrder(FILE_A.location(), dataPartitionedFile.location());
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void partitionFilterKeepsFileWithUnknownSpec(FileFormat format) throws IOException {
    int missingSpecId = 5;
    TrackedFile file =
        dataFileWithoutStats("s3://bucket/table/orphan.parquet", missingSpecId, idPartition(1));

    assertThat(ID_PARTITIONING_SPECS).doesNotContainKey(missingSpecId);
    ManifestFile manifest = writeManifest(format, ID_PARTITIONED_TYPE, file);

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .filter(Expressions.equal("id", 2))
            .metricsConfig(METRICS_CONFIG);

    TrackedFile actual = readOne(builder);

    assertThat(actual).usingComparator(FILE_COMPARATOR).isEqualTo(file);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void partitionFilterKeepsUnpartitionedFile(FileFormat format) throws IOException {
    ManifestFile manifest = writeManifest(format, ID_PARTITIONED_TYPE, UNPARTITIONED_FILE);

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, ID_PARTITIONING_SPECS)
            .filter(Expressions.equal("id", 2))
            .metricsConfig(METRICS_CONFIG);
    TrackedFile actual = readOne(builder);

    assertThat(actual).usingComparator(FILE_COMPARATOR).isEqualTo(UNPARTITIONED_FILE);
  }

  @Test
  public void validationUnknownManifestFormatFailure() {
    ManifestFile fileWithoutFormat = Mockito.mock(ManifestFile.class);
    Mockito.when(fileWithoutFormat.formatVersion()).thenReturn(4);
    Mockito.when(fileWithoutFormat.content()).thenReturn(ManifestContent.DATA);

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(fileWithoutFormat, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot determine format of manifest");
  }

  @Test
  public void validationUnknownFormatVersionFailure() {
    ManifestFile v3File = Mockito.mock(ManifestFile.class);
    Mockito.when(v3File.formatVersion()).thenReturn(3);
    Mockito.when(v3File.content()).thenReturn(ManifestContent.DATA);

    assertThatThrownBy(
            () -> V4ManifestReader.builder(v3File, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot read manifest with format version 3: only 4 is supported");
  }

  @Test
  public void validationDeleteManifestFailure() {
    ManifestFile v3File = Mockito.mock(ManifestFile.class);
    Mockito.when(v3File.formatVersion()).thenReturn(4);
    Mockito.when(v3File.content()).thenReturn(ManifestContent.DELETES);

    assertThatThrownBy(
            () -> V4ManifestReader.builder(v3File, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot read manifest with content DELETES");
  }

  @Test
  public void validationManifestDeletionVectorFailure() {
    ManifestFile fileWithMDV = Mockito.mock(ManifestFile.class);
    Mockito.when(fileWithMDV.path()).thenReturn(FileFormat.PARQUET.addExtension("manifest"));
    Mockito.when(fileWithMDV.formatVersion()).thenReturn(4);
    Mockito.when(fileWithMDV.content()).thenReturn(ManifestContent.DATA);
    Mockito.when(fileWithMDV.manifestDeletionVector())
        .thenReturn(Mockito.mock(ManifestBitmap.class));

    assertThatThrownBy(
            () -> V4ManifestReader.builder(fileWithMDV, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Cannot read manifest with a deletion vector");
  }

  @Test
  public void validationChecksBuilderArguments() {
    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .filter(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid filter: null");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .scanMetrics(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid scan metrics: null");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        UNREAD_MANIFEST_FILE, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
                    .tableLocation(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table location: null");

    assertThatThrownBy(
            () -> V4ManifestReader.builder(UNREAD_MANIFEST_FILE, IO, null, UNPARTITIONED_SPECS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table schema: null");
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void resolutionMissingTableLocationFailure(FileFormat format) throws IOException {
    ManifestFile manifest =
        writeManifest(
            format,
            UNPARTITIONED_TYPE,
            unpartitionedFileWithoutStats("data/unpartitioned.parquet"));

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .metricsConfig(METRICS_CONFIG);

    assertThatThrownBy(() -> read(builder))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("\"tableLocation\" is null");
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void resolutionRelativeDataFileLocation(FileFormat format) throws IOException {
    ManifestFile manifest =
        writeManifest(
            format,
            UNPARTITIONED_TYPE,
            unpartitionedFileWithoutStats("data/unpartitioned.parquet"));

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .tableLocation("s3://bucket/db/table")
            .metricsConfig(METRICS_CONFIG);

    assertThat(readOne(builder))
        .usingComparator(FILE_COMPARATOR)
        .isEqualTo(
            unpartitionedFileWithoutStats("s3://bucket/db/table/data/unpartitioned.parquet"));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void resolutionRelativeLeafManifestLocation(FileFormat format) throws IOException {
    ManifestFile manifest =
        writeManifest(
            format,
            UNPARTITIONED_TYPE,
            manifestRefWithoutStats(FileContent.DATA_MANIFEST, "metadata/leaf.avro"));

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .tableLocation("s3://bucket/db/table")
            .metricsConfig(METRICS_CONFIG);

    assertThat(readOne(builder))
        .usingComparator(FILE_COMPARATOR)
        .isEqualTo(
            manifestRefWithoutStats(
                FileContent.DATA_MANIFEST, "s3://bucket/db/table/metadata/leaf.avro"));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void resolutionRelativeDVLocation(FileFormat format) throws IOException {
    ManifestFile manifest =
        writeManifest(
            format,
            UNPARTITIONED_TYPE,
            unpartitionedDataWithDVFile("s3://other/abs.parquet", "data/dv.puffin"));

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .tableLocation("s3://bucket/db/table")
            .metricsConfig(METRICS_CONFIG);

    assertThat(readOne(builder))
        .usingComparator(FILE_COMPARATOR)
        .isEqualTo(
            unpartitionedDataWithDVFile(
                "s3://other/abs.parquet", "s3://bucket/db/table/data/dv.puffin"));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void resolutionResolvesOnlyRelativePaths(FileFormat format) throws IOException {
    ManifestFile manifest =
        writeManifest(
            format,
            UNPARTITIONED_TYPE,
            ImmutableList.of(
                unpartitionedDataWithDVFile("s3://other/abs.parquet", "data/dv.puffin"),
                unpartitionedDataWithDVFile("data/rel.parquet", "s3://other/abs-dv.puffin")));

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, IO, TABLE_SCHEMA, UNPARTITIONED_SPECS)
            .tableLocation("s3://bucket/db/table")
            .metricsConfig(METRICS_CONFIG);

    assertThat(read(builder))
        .usingComparatorForType(FILE_COMPARATOR, TrackedFile.class)
        .containsExactly(
            unpartitionedDataWithDVFile(
                "s3://other/abs.parquet", "s3://bucket/db/table/data/dv.puffin"),
            unpartitionedDataWithDVFile(
                "s3://bucket/db/table/data/rel.parquet", "s3://other/abs-dv.puffin"));
  }

  private static DeletionVector dv(String location) {
    return DeletionVectorStruct.builder()
        .location(location)
        .offset(100L)
        .sizeInBytes(50L)
        .cardinality(5L)
        .build();
  }

  private static TrackedFile unpartitionedDataFileWithStats(String location, ContentStats stats) {
    return unpartitionedDataFile(ADDED_TRACKING, location, stats, null /* no DV */);
  }

  private static TrackedFile unpartitionedFileWithoutStats(String location) {
    return unpartitionedFileWithStatus(EntryStatus.ADDED, location);
  }

  private static TrackedFile idPartitionedDataFileWithoutStats(
      String location, PartitionData partition) {
    return dataFileWithoutStats(location, ID_PARTITIONED.specId(), partition);
  }

  private static TrackedFile dataFileWithoutStats(
      String location, Integer specId, PartitionData partition) {
    return new TrackedFileStruct(
        ADDED_TRACKING,
        FileContent.DATA,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        specId,
        partition,
        null, // content_stats
        SortOrder.unsorted().orderId(),
        null, // deletion_vector
        null, // manifest_info
        null, // key_metadata
        ImmutableList.of(4L), // split offsets
        null); // equality_ids
  }

  private static TrackedFile idPartitionedDeleteFileWithoutStats(
      String location, PartitionData partition) {
    return new TrackedFileStruct(
        ADDED_TRACKING,
        FileContent.EQUALITY_DELETES,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        ID_PARTITIONED.specId(),
        partition,
        null, // content_stats
        SortOrder.unsorted().orderId(),
        null, // deletion_vector
        null, // manifest_info
        null, // key_metadata
        ImmutableList.of(4L), // split offsets
        ImmutableList.of(1)); // equality_ids
  }

  private static TrackedFile unpartitionedDataWithDVFile(String location, String dvLocation) {
    return unpartitionedDataFile(ADDED_TRACKING, location, null, dv(dvLocation));
  }

  private static TrackedFile manifestRefWithoutStats(FileContent content, String location) {
    return manifestRef(content, location, null /* no stats */);
  }

  private static TrackedFile manifestRefWithStats(FileContent content, String location) {
    return manifestRef(content, location, CONTENT_STATS);
  }

  private static TrackedFile manifestRef(FileContent content, String location, ContentStats stats) {
    return new TrackedFileStruct(
        ADDED_TRACKING,
        content,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        null, // spec_id: a manifest reference has no spec
        null, // partition: a manifest reference has no partition tuple
        stats, // content_stats
        null, // sort_order_id
        null, // deletion_vector
        MANIFEST_INFO,
        null, // key_metadata
        ImmutableList.of(4L), // split_offsets
        null); // equality_ids
  }

  private static TrackedFile unpartitionedFileWithStatus(EntryStatus status, String location) {
    Tracking tracking =
        new TrackingStruct(
            status,
            SNAPSHOT_ID,
            3L, // data sequence number
            3L, // file sequence number
            null, // dv snapshot id
            null, // first row id
            null, // deleted positions
            null); // replaced positions
    return unpartitionedDataFile(tracking, location, null /* no stats */, null /* no DV */);
  }

  private static TrackedFile unpartitionedDataFile(
      Tracking tracking, String location, ContentStats stats, DeletionVector dv) {
    return new TrackedFileStruct(
        tracking,
        FileContent.DATA,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        null, // unpartitioned
        null, // null partition data
        stats,
        SortOrder.unsorted().orderId(),
        dv,
        null, // manifest info
        null, // key metadata
        ImmutableList.of(4L), // split offsets
        null); // equality ids
  }

  private static PartitionData idPartition(int id) {
    return partition(ID_PARTITIONED, id);
  }

  private static PartitionData partition(PartitionSpec spec, Object... values) {
    PartitionData partition = DataFiles.newPartitionData(spec);
    for (int i = 0; i < values.length; i += 1) {
      partition.set(i, values[i]);
    }
    return partition;
  }

  private ManifestFile writeManifest(
      FileFormat format, Types.StructType partitionType, TrackedFile file) throws IOException {
    return writeManifest(format, partitionType, ImmutableList.of(file));
  }

  private ManifestFile writeManifest(
      FileFormat format, Types.StructType partitionType, List<TrackedFile> files)
      throws IOException {
    Schema writeSchema = TrackedFile.schema(partitionType, STATS_TYPE);
    OutputFile out = IO.newOutputFile(format.addExtension("manifest." + System.nanoTime()));
    try (FileAppender<StructLike> appender =
        InternalData.write(format, out).schema(writeSchema).named("tracked_file").build()) {
      for (TrackedFile file : files) {
        appender.add((StructLike) file);
      }
    }

    return v4Manifest(out.location());
  }

  private static ManifestFile v4Manifest(String path) {
    ManifestFile manifest = mock(ManifestFile.class);
    when(manifest.path()).thenReturn(path);
    when(manifest.formatVersion()).thenReturn(FORMAT_VERSION_V4);
    when(manifest.content()).thenReturn(ManifestContent.DATA);
    return manifest;
  }

  private TrackedFile readOne(V4ManifestReader.Builder builder) throws IOException {
    return Iterables.getOnlyElement(read(builder));
  }

  private List<TrackedFile> read(V4ManifestReader.Builder builder) throws IOException {
    try (V4ManifestReader reader = builder.build()) {
      return Lists.newArrayList(reader);
    }
  }
}
