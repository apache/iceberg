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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

class TestV4ManifestReaderStats {
  private static final Types.StructType EMPTY_PARTITION = Types.StructType.of();
  private static final PartitionData EMPTY_PARTITION_DATA = new PartitionData(EMPTY_PARTITION);
  private static final Map<Integer, PartitionSpec> UNPARTITIONED_SPECS =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());
  private static final List<FileFormat> MANIFEST_FORMATS =
      List.of(FileFormat.AVRO, FileFormat.PARQUET);
  private static final String TABLE_LOCATION = "s3://bucket/db/table";

  private static final int ID_FIELD_ID = 1;
  private static final int DATA_FIELD_ID = 2;
  private static final int MEASURE_FIELD_ID = 3;

  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(ID_FIELD_ID, "id", Types.IntegerType.get()),
          optional(DATA_FIELD_ID, "data", Types.StringType.get()),
          optional(MEASURE_FIELD_ID, "measure", Types.DoubleType.get()));
  private static final Types.StructType CONTENT_STATS_TYPE =
      StatsUtil.statsWriteSchema(
          TABLE_SCHEMA,
          MetricsConfig.from(
              ImmutableMap.of(
                  TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id",
                  "full",
                  TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "data",
                  "full",
                  TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "measure",
                  "full"),
              TABLE_SCHEMA,
              null));
  private static final FieldStats<Integer> ID_STATS =
      new FieldStatsStruct<>(
          CONTENT_STATS_TYPE.fieldType("id").asStructType(), 1, 100, true, 26L, 2L, 0L, null);
  private static final FieldStats<String> DATA_STATS =
      new FieldStatsStruct<>(
          CONTENT_STATS_TYPE.fieldType("data").asStructType(), "a", "z", true, 26L, 0L, 0L, 4);
  private static final FieldStats<Double> MEASURE_STATS =
      new FieldStatsStruct<>(
          CONTENT_STATS_TYPE.fieldType("measure").asStructType(),
          1.5,
          9.5,
          false,
          26L,
          1L,
          3L,
          null);

  @Test
  void invalidProjectStatsArguments() {
    InputFile manifest = new InMemoryInputFile(new byte[0]);

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .projectStats((Iterable<Integer>) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: null");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(
                        manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .projectStats((int[]) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: null");
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void readContentStatsForAllFieldIds(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertThat(stats).isNotNull();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);
      assertFieldStats(stats.statsFor(DATA_FIELD_ID), DATA_STATS);
      assertFieldStats(stats.statsFor(MEASURE_FIELD_ID), MEASURE_STATS);
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void statsAreReadWithMetricsConfig(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    MetricsConfig metricsConfig =
        MetricsConfig.from(
            ImmutableMap.of(
                TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "data",
                "counts",
                TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "measure",
                "none"),
            TABLE_SCHEMA,
            null);

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .metricsConfig(metricsConfig)
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);

      FieldStats<?> dataStats = stats.statsFor(DATA_FIELD_ID);
      assertThat(dataStats).isNotNull();
      assertThat(dataStats.lowerBound()).isNull();
      assertThat(dataStats.upperBound()).isNull();
      assertThat(dataStats.valueCount()).isEqualTo(DATA_STATS.valueCount());
      assertThat(dataStats.nullValueCount()).isEqualTo(DATA_STATS.nullValueCount());
      assertThat(dataStats.avgValueSizeInBytes()).isEqualTo(DATA_STATS.avgValueSizeInBytes());

      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void schemaProjectionStatsAreReadWhenOmittedByMetricsConfig(FileFormat format)
      throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    // the projection is based on the manifest schema, so it determines the stats that are read even
    // though the metrics config would not produce stats for measure
    Schema projection =
        new Schema(TrackedFile.LOCATION, StatsUtil.contentStatsField(CONTENT_STATS_TYPE));
    MetricsConfig metricsConfig =
        MetricsConfig.from(
            ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "measure", "none"),
            TABLE_SCHEMA,
            null);

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .project(projection)
            .metricsConfig(metricsConfig)
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);
      assertFieldStats(stats.statsFor(DATA_FIELD_ID), DATA_STATS);
      assertFieldStats(stats.statsFor(MEASURE_FIELD_ID), MEASURE_STATS);
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void rowFilterKeepsStatsForAllFieldIds(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    // a filter narrows the stats only for scan planning, so a default read still carries the
    // stats of every field even though the filter needs one of them
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .filter(Expressions.equal("id", 1))
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);
      assertFieldStats(stats.statsFor(DATA_FIELD_ID), DATA_STATS);
      assertFieldStats(stats.statsFor(MEASURE_FIELD_ID), MEASURE_STATS);
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void selectStatsByName(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .select("location", "content_stats.data")
            .build()) {
      TrackedFile actual = Iterables.getOnlyElement(reader);
      assertThat(actual.location()).isEqualTo("s3://bucket/file.parquet");

      ContentStats stats = actual.contentStats();
      assertFieldStats(stats.statsFor(DATA_FIELD_ID), DATA_STATS);
      assertThat(stats.statsFor(ID_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void projectStatsReadsOnlyRequestedColumns(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .projectStats(ID_FIELD_ID)
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);
      assertThat(stats.statsFor(DATA_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void projectStatsWithoutFieldIdsOmitsStats(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    // requesting no field IDs opts out of the default projection of every field
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .projectStats(List.of())
            .build()) {
      assertThat(Iterables.getOnlyElement(reader).contentStats()).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void projectStatsWithoutFieldIdsStillReadsFilterStats(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    // requesting no field IDs opts out of every field's stats, but not out of what the filter needs
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .projectStats(List.of())
            .filter(Expressions.equal("data", "m"))
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(DATA_FIELD_ID), DATA_STATS);
      assertThat(stats.statsFor(ID_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void projectStatsCopiesFieldIds(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    List<Integer> fieldIds = Lists.newArrayList(ID_FIELD_ID);
    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .projectStats(fieldIds);

    // the builder copies the field IDs, so a later change to fieldIds does not widen the projection
    fieldIds.add(DATA_FIELD_ID);

    try (V4ManifestReader reader = builder.build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);
      assertThat(stats.statsFor(DATA_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void requestedStatsAreProjectedWhenOmittedBySchemaProjection(FileFormat format)
      throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .project(new Schema(TrackedFile.LOCATION))
            .projectStats(MEASURE_FIELD_ID)
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(MEASURE_FIELD_ID), MEASURE_STATS);
      assertThat(stats.statsFor(ID_FIELD_ID)).isNull();
      assertThat(stats.statsFor(DATA_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void filterStatsAreProjectedWhenOmittedBySchemaProjection(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    // filter references data, so its stats are read even though the schema projection omits them
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .project(new Schema(TrackedFile.LOCATION))
            .filter(Expressions.equal("data", "m"))
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(DATA_FIELD_ID), DATA_STATS);
      assertThat(stats.statsFor(ID_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void singleStatsFieldIsReadForSchemaProjection(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    // a projection may narrow stats to a single field rather than to whole stats structs
    Types.NestedField dataStats = CONTENT_STATS_TYPE.field("data");
    Types.StructType lowerBoundOnly =
        Types.StructType.of(
            optional(
                dataStats.fieldId(),
                dataStats.name(),
                Types.StructType.of(
                    dataStats.type().asStructType().field(StatsUtil.LOWER_BOUND_NAME))));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .project(new Schema(TrackedFile.LOCATION, StatsUtil.contentStatsField(lowerBoundOnly)))
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      FieldStats<?> actual = stats.statsFor(DATA_FIELD_ID);
      assertThat(actual).isNotNull();
      assertThat(actual.type()).isEqualTo(lowerBoundOnly.fieldType("data").asStructType());
      assertThat(actual.lowerBound()).isEqualTo(DATA_STATS.lowerBound());
      assertThat(actual.upperBound()).isNull();
      assertThat(actual.hasValueCount()).isFalse();
      assertThat(stats.statsFor(ID_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void filterStatsAreProjectedWhenNarrowedBySchemaProjection(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    // the projection carries stats for data, but without the bounds that the filter is evaluated
    // against, so the stats that are read for data must be widened back to the full stats struct
    Types.StructType narrowStatsType =
        StatsUtil.statsWriteSchema(
            TABLE_SCHEMA,
            MetricsConfig.from(
                ImmutableMap.of(
                    TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id",
                    "none",
                    TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "data",
                    "counts",
                    TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "measure",
                    "none"),
                TABLE_SCHEMA,
                null));
    assertThat(narrowStatsType.fieldType("data").asStructType().field(StatsUtil.LOWER_BOUND_NAME))
        .isNull();

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .project(new Schema(TrackedFile.LOCATION, StatsUtil.contentStatsField(narrowStatsType)))
            .filter(Expressions.equal("data", "m"))
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(DATA_FIELD_ID), DATA_STATS);

      // stats omitted by the projection are not read because the filter does not need them
      assertThat(stats.statsFor(ID_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void filterStatsAreProjectedForCaseInsensitiveFilter(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    // the filter refers to data by a different case, which binds only when case is ignored
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .project(new Schema(TrackedFile.LOCATION))
            .caseSensitive(false)
            .filter(Expressions.equal("DATA", "m"))
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(DATA_FIELD_ID), DATA_STATS);
      assertThat(stats.statsFor(ID_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void projectStatsAndFilterStatsAreCombined(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    // scan planning narrows stats to the requested fields and the fields the filter needs
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .forScanPlanning()
            .projectStats(ID_FIELD_ID)
            .filter(Expressions.equal("data", "m"))
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);
      assertFieldStats(stats.statsFor(DATA_FIELD_ID), DATA_STATS);
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void projectStatsAndFilterStatsAreCombinedWithoutScanPlanning(FileFormat format)
      throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    // projectStats narrows stats to the requested fields and the fields the filter needs, which
    // does not depend on scan planning
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .projectStats(ID_FIELD_ID)
            .filter(Expressions.equal("data", "m"))
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);
      assertFieldStats(stats.statsFor(DATA_FIELD_ID), DATA_STATS);
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void forScanPlanningReadsOnlyFilterStats(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .forScanPlanning()
            .filter(Expressions.equal("id", 1))
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);
      assertThat(stats.statsFor(DATA_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void forScanPlanningOmitsStatsWithoutFilter(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    // scan planning without a filter has no stats to evaluate, so none are read
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .forScanPlanning()
            .build()) {
      assertThat(Iterables.getOnlyElement(reader).contentStats()).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void selectWithoutStatsOmitsStats(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, List.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .select("location")
            .build()) {
      assertThat(Iterables.getOnlyElement(reader).contentStats()).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void statsAreNullForColumnsWithoutStoredStats(FileFormat format) throws IOException {
    // the manifest stores stats for id alone, while the reader reads stats for every column
    Types.StructType storedStatsType =
        StatsUtil.statsWriteSchema(
            TABLE_SCHEMA,
            MetricsConfig.from(
                ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id", "full"),
                TABLE_SCHEMA,
                null));
    ContentStatsStruct stored = new ContentStatsStruct(storedStatsType);
    stored.setStats(ID_FIELD_ID, ID_STATS);

    TrackedFile file = fileWithStats("s3://bucket/file.parquet", stored);
    InputFile manifest = writeManifest(format, storedStatsType, List.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);
      assertThat(stats.statsFor(DATA_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
      assertThat(stats.fieldStats()).doesNotContainNull().hasSize(1);
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void statsAreCorrectWithContainerReuse(FileFormat format) throws IOException {
    // the reader decodes entries into a reused container and copies each one before returning it,
    // so every copy must hold the stats of its own entry rather than alias the container that the
    // next entry overwrites; giving every entry stats for a different column makes a carried over
    // value visible
    ContentStatsStruct idStats = new ContentStatsStruct(CONTENT_STATS_TYPE);
    idStats.setStats(ID_FIELD_ID, ID_STATS);

    ContentStatsStruct dataStats = new ContentStatsStruct(CONTENT_STATS_TYPE);
    dataStats.setStats(DATA_FIELD_ID, DATA_STATS);

    ContentStatsStruct measureStats = new ContentStatsStruct(CONTENT_STATS_TYPE);
    measureStats.setStats(MEASURE_FIELD_ID, MEASURE_STATS);

    List<TrackedFile> files =
        List.of(
            fileWithStats("s3://bucket/with-id-stats.parquet", idStats),
            fileWithStats(
                "s3://bucket/without-stats.parquet", new ContentStatsStruct(CONTENT_STATS_TYPE)),
            fileWithStats("s3://bucket/with-data-stats.parquet", dataStats),
            fileWithStats("s3://bucket/with-measure-stats.parquet", measureStats));
    InputFile manifest = writeManifest(format, CONTENT_STATS_TYPE, files);

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .build()) {
      List<TrackedFile> read = Lists.newArrayList(reader);

      ContentStats withIdStats = read.get(0).contentStats();
      assertFieldStats(withIdStats.statsFor(ID_FIELD_ID), ID_STATS);
      assertThat(withIdStats.statsFor(DATA_FIELD_ID)).isNull();
      assertThat(withIdStats.statsFor(MEASURE_FIELD_ID)).isNull();
      assertThat(withIdStats.fieldStats()).doesNotContainNull();

      ContentStats withoutStats = read.get(1).contentStats();
      assertThat(withoutStats.statsFor(ID_FIELD_ID)).isNull();
      assertThat(withoutStats.fieldStats()).isEmpty();

      ContentStats withDataStats = read.get(2).contentStats();
      assertFieldStats(withDataStats.statsFor(DATA_FIELD_ID), DATA_STATS);
      assertThat(withDataStats.statsFor(ID_FIELD_ID)).isNull();
      assertThat(withDataStats.statsFor(MEASURE_FIELD_ID)).isNull();
      assertThat(withDataStats.fieldStats()).doesNotContainNull();

      ContentStats withMeasureStats = read.get(3).contentStats();
      assertFieldStats(withMeasureStats.statsFor(MEASURE_FIELD_ID), MEASURE_STATS);
      assertThat(withMeasureStats.statsFor(ID_FIELD_ID)).isNull();
      assertThat(withMeasureStats.statsFor(DATA_FIELD_ID)).isNull();
      assertThat(withMeasureStats.fieldStats()).doesNotContainNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void readStatsForNestedFields(FileFormat format) throws IOException {
    int locationFieldId = 20;
    int latFieldId = 21;
    int lonFieldId = 22;
    int tagsFieldId = 23;
    int tagFieldId = 24;

    Schema nestedSchema =
        new Schema(
            optional(
                locationFieldId,
                "location",
                Types.StructType.of(
                    required(latFieldId, "lat", Types.DoubleType.get()),
                    optional(lonFieldId, "lon", Types.DoubleType.get()))),
            optional(
                tagsFieldId,
                "tags",
                Types.ListType.ofOptional(tagFieldId, Types.StringType.get())));
    Types.StructType statsType =
        StatsUtil.statsWriteSchema(
            nestedSchema,
            MetricsConfig.from(
                ImmutableMap.of(
                    TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "location.lat",
                    "full",
                    TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "location.lon",
                    "full"),
                nestedSchema,
                null));
    FieldStats<Double> latStats =
        new FieldStatsStruct<>(
            statsType.fieldType("location_lat").asStructType(), 1.5, 9.5, true, 26L, 0L, 0L, null);
    FieldStats<Double> lonStats =
        new FieldStatsStruct<>(
            statsType.fieldType("location_lon").asStructType(),
            -9.5,
            -1.5,
            true,
            26L,
            2L,
            0L,
            null);

    ContentStatsStruct stats = new ContentStatsStruct(statsType);
    stats.setStats(latFieldId, latStats);
    stats.setStats(lonFieldId, lonStats);

    TrackedFile file = fileWithStats("s3://bucket/file.parquet", stats);
    InputFile manifest = writeManifest(format, statsType, List.of(file));

    // the reader requests stats for every field of the table, including the struct and the list
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, nestedSchema, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .build()) {
      ContentStats actual = Iterables.getOnlyElement(reader).contentStats();
      assertThat(actual.type()).isEqualTo(statsType);
      assertFieldStats(actual.statsFor(latFieldId), latStats);
      assertFieldStats(actual.statsFor(lonFieldId), lonStats);
      assertThat(actual.statsFor(locationFieldId)).isNull();
      assertThat(actual.statsFor(tagsFieldId)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void readVariantStats(FileFormat format) throws IOException {
    int fieldId = 12;
    Schema schema = new Schema(optional(fieldId, "var", Types.VariantType.get()));
    Types.StructType statsType =
        StatsUtil.statsWriteSchema(
            schema,
            MetricsConfig.from(
                ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "var", "full"),
                schema,
                null));
    Types.StructType varStatsType = statsType.fieldType("var").asStructType();

    VariantMetadata metadata = Variants.metadata("$['x']");
    Variant lowerBound = variantBound(metadata, 1);
    Variant upperBound = variantBound(metadata, 10);

    ContentStatsStruct stats = new ContentStatsStruct(statsType);
    stats.setStats(
        fieldId,
        new FieldStatsStruct<>(varStatsType, lowerBound, upperBound, false, 26L, 2L, 0L, 32));

    TrackedFile file = fileWithStats("s3://bucket/file.parquet", stats);
    InputFile manifest = writeManifest(format, statsType, List.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, schema, UNPARTITIONED_SPECS, TABLE_LOCATION).build()) {
      FieldStats<?> actual = Iterables.getOnlyElement(reader).contentStats().statsFor(fieldId);

      assertThat(actual.type()).isEqualTo(varStatsType);
      assertThat(actual.valueCount()).isEqualTo(26L);
      assertThat(actual.nullValueCount()).isEqualTo(2L);
      assertThat(actual.avgValueSizeInBytes()).isEqualTo(32);
      assertThat(actual.tightBounds()).isFalse();
      assertThat(actual.hasNanValueCount()).isFalse();
      assertVariant(actual.lowerBound(), lowerBound);
      assertVariant(actual.upperBound(), upperBound);
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void variantStatsAreCorrectWithContainerReuse(FileFormat format) throws IOException {
    int fieldId = 12;
    Schema schema = new Schema(optional(fieldId, "var", Types.VariantType.get()));
    Types.StructType statsType =
        StatsUtil.statsWriteSchema(
            schema,
            MetricsConfig.from(
                ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "var", "full"),
                schema,
                null));
    Types.StructType varStatsType = statsType.fieldType("var").asStructType();

    VariantMetadata metadata = Variants.metadata("$['x']");
    Variant firstLower = variantBound(metadata, 1);
    Variant firstUpper = variantBound(metadata, 10);
    ContentStatsStruct first = new ContentStatsStruct(statsType);
    first.setStats(
        fieldId,
        new FieldStatsStruct<>(varStatsType, firstLower, firstUpper, false, 26L, 2L, 0L, 32));

    Variant secondLower = variantBound(metadata, 100);
    Variant secondUpper = variantBound(metadata, 1000);
    ContentStatsStruct second = new ContentStatsStruct(statsType);
    second.setStats(
        fieldId,
        new FieldStatsStruct<>(varStatsType, secondLower, secondUpper, false, 45L, 1L, 0L, 64));

    List<TrackedFile> files =
        List.of(
            fileWithStats("s3://bucket/first.parquet", first),
            fileWithStats("s3://bucket/second.parquet", second));
    InputFile manifest = writeManifest(format, statsType, files);

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, schema, UNPARTITIONED_SPECS, TABLE_LOCATION).build()) {
      List<TrackedFile> read = Lists.newArrayList(reader);

      FieldStats<?> firstStats = read.get(0).contentStats().statsFor(fieldId);
      assertThat(firstStats.type()).isEqualTo(varStatsType);
      assertThat(firstStats.valueCount()).isEqualTo(26L);
      assertThat(firstStats.nullValueCount()).isEqualTo(2L);
      assertThat(firstStats.avgValueSizeInBytes()).isEqualTo(32);
      assertThat(firstStats.tightBounds()).isFalse();
      assertThat(firstStats.hasNanValueCount()).isFalse();
      assertVariant(firstStats.lowerBound(), firstLower);
      assertVariant(firstStats.upperBound(), firstUpper);

      FieldStats<?> secondStats = read.get(1).contentStats().statsFor(fieldId);
      assertThat(secondStats.type()).isEqualTo(varStatsType);
      assertThat(secondStats.valueCount()).isEqualTo(45L);
      assertThat(secondStats.nullValueCount()).isEqualTo(1L);
      assertThat(secondStats.avgValueSizeInBytes()).isEqualTo(64);
      assertThat(secondStats.tightBounds()).isFalse();
      assertThat(secondStats.hasNanValueCount()).isFalse();
      assertVariant(secondStats.lowerBound(), secondLower);
      assertVariant(secondStats.upperBound(), secondUpper);
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void readGeoStats(FileFormat format) throws IOException {
    int geometryFieldId = 10;
    int geographyFieldId = 11;
    Schema geoSchema =
        new Schema(
            optional(geometryFieldId, "geom", Types.GeometryType.crs84()),
            optional(geographyFieldId, "geog", Types.GeographyType.crs84()));
    Types.StructType statsType =
        StatsUtil.statsWriteSchema(
            geoSchema,
            MetricsConfig.from(
                ImmutableMap.of(
                    TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "geom",
                    "full",
                    TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "geog",
                    "full"),
                geoSchema,
                null));
    Types.StructType geomStatsType = statsType.fieldType("geom").asStructType();
    Types.StructType geogStatsType = statsType.fieldType("geog").asStructType();

    ContentStatsStruct stats = new ContentStatsStruct(statsType);
    stats.setStats(
        geometryFieldId,
        new FieldStatsStruct<>(
            geomStatsType,
            boundingBox(geomStatsType, StatsUtil.LOWER_BOUND_NAME, 1.0d, 2.0d, 3.0d, 4.0d),
            boundingBox(geomStatsType, StatsUtil.UPPER_BOUND_NAME, 5.0d, 6.0d, 7.0d, 8.0d),
            false,
            26L,
            2L,
            0L,
            32));
    stats.setStats(
        geographyFieldId,
        new FieldStatsStruct<>(
            geogStatsType,
            boundingBox(geogStatsType, StatsUtil.LOWER_BOUND_NAME, -20.0d, -10.0d, 0.0d, 1.0d),
            boundingBox(geogStatsType, StatsUtil.UPPER_BOUND_NAME, 20.0d, 10.0d, 30.0d, 2.0d),
            false,
            30L,
            3L,
            0L,
            48));

    TrackedFile file = fileWithStats("s3://bucket/file.parquet", stats);
    InputFile manifest = writeManifest(format, statsType, List.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, geoSchema, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .build()) {
      ContentStats actual = Iterables.getOnlyElement(reader).contentStats();

      FieldStats<?> geom = actual.statsFor(geometryFieldId);
      assertThat(geom.type()).isEqualTo(geomStatsType);
      assertThat(geom.valueCount()).isEqualTo(26L);
      assertThat(geom.nullValueCount()).isEqualTo(2L);
      assertThat(geom.hasNanValueCount()).isFalse();
      assertThat(geom.tightBounds()).isFalse();
      assertThat(geom.avgValueSizeInBytes()).isEqualTo(32);
      assertBoundingBox(geom.lowerBound(), 1.0d, 2.0d, 3.0d, 4.0d);
      assertBoundingBox(geom.upperBound(), 5.0d, 6.0d, 7.0d, 8.0d);

      FieldStats<?> geog = actual.statsFor(geographyFieldId);
      assertThat(geog.type()).isEqualTo(geogStatsType);
      assertThat(geog.valueCount()).isEqualTo(30L);
      assertThat(geog.nullValueCount()).isEqualTo(3L);
      assertThat(geog.hasNanValueCount()).isFalse();
      assertThat(geog.tightBounds()).isFalse();
      assertThat(geog.avgValueSizeInBytes()).isEqualTo(48);
      assertBoundingBox(geog.lowerBound(), -20.0d, -10.0d, 0.0d, 1.0d);
      assertBoundingBox(geog.upperBound(), 20.0d, 10.0d, 30.0d, 2.0d);
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void geoStatsAreCorrectWithContainerReuse(FileFormat format) throws IOException {
    int geometryFieldId = 10;
    Schema geoSchema = new Schema(optional(geometryFieldId, "geom", Types.GeometryType.crs84()));
    Types.StructType statsType =
        StatsUtil.statsWriteSchema(
            geoSchema,
            MetricsConfig.from(
                ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "geom", "full"),
                geoSchema,
                null));
    Types.StructType geomStatsType = statsType.fieldType("geom").asStructType();

    // geo bounds are bounding box structs that the reader reuses across entries, so a copy that
    // keeps a reference to one reports the last entry's box for every entry
    ContentStatsStruct first = new ContentStatsStruct(statsType);
    first.setStats(
        geometryFieldId,
        new FieldStatsStruct<>(
            geomStatsType,
            boundingBox(geomStatsType, StatsUtil.LOWER_BOUND_NAME, 1.0d, 2.0d, 3.0d, 4.0d),
            boundingBox(geomStatsType, StatsUtil.UPPER_BOUND_NAME, 5.0d, 6.0d, 7.0d, 8.0d),
            false,
            26L,
            2L,
            0L,
            32));

    ContentStatsStruct second = new ContentStatsStruct(statsType);
    second.setStats(
        geometryFieldId,
        new FieldStatsStruct<>(
            geomStatsType,
            boundingBox(geomStatsType, StatsUtil.LOWER_BOUND_NAME, 11.0d, 12.0d, 13.0d, 14.0d),
            boundingBox(geomStatsType, StatsUtil.UPPER_BOUND_NAME, 15.0d, 16.0d, 17.0d, 18.0d),
            false,
            45L,
            1L,
            0L,
            64));

    List<TrackedFile> files =
        List.of(
            fileWithStats("s3://bucket/first.parquet", first),
            fileWithStats("s3://bucket/second.parquet", second));
    InputFile manifest = writeManifest(format, statsType, files);

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, geoSchema, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .build()) {
      List<TrackedFile> read = Lists.newArrayList(reader);

      FieldStats<?> firstStats = read.get(0).contentStats().statsFor(geometryFieldId);
      assertThat(firstStats.type()).isEqualTo(geomStatsType);
      assertThat(firstStats.valueCount()).isEqualTo(26L);
      assertThat(firstStats.nullValueCount()).isEqualTo(2L);
      assertThat(firstStats.avgValueSizeInBytes()).isEqualTo(32);
      assertThat(firstStats.tightBounds()).isFalse();
      assertThat(firstStats.hasNanValueCount()).isFalse();
      assertBoundingBox(firstStats.lowerBound(), 1.0d, 2.0d, 3.0d, 4.0d);
      assertBoundingBox(firstStats.upperBound(), 5.0d, 6.0d, 7.0d, 8.0d);

      FieldStats<?> secondStats = read.get(1).contentStats().statsFor(geometryFieldId);
      assertThat(secondStats.type()).isEqualTo(geomStatsType);
      assertThat(secondStats.valueCount()).isEqualTo(45L);
      assertThat(secondStats.nullValueCount()).isEqualTo(1L);
      assertThat(secondStats.avgValueSizeInBytes()).isEqualTo(64);
      assertThat(secondStats.tightBounds()).isFalse();
      assertThat(secondStats.hasNanValueCount()).isFalse();
      assertBoundingBox(secondStats.lowerBound(), 11.0d, 12.0d, 13.0d, 14.0d);
      assertBoundingBox(secondStats.upperBound(), 15.0d, 16.0d, 17.0d, 18.0d);
    }
  }

  private static void assertFieldStats(FieldStats<?> actual, FieldStats<?> expected) {
    assertThat(actual).isNotNull();
    assertThat(actual.fieldId()).isEqualTo(expected.fieldId());
    assertThat(actual.type()).isEqualTo(expected.type());
    assertThat(actual.valueCount()).isEqualTo(expected.valueCount());
    assertThat(actual.lowerBound()).isEqualTo(expected.lowerBound());
    assertThat(actual.upperBound()).isEqualTo(expected.upperBound());
    assertThat(actual.tightBounds()).isEqualTo(expected.tightBounds());
    assertThat(actual.avgValueSizeInBytes()).isEqualTo(expected.avgValueSizeInBytes());

    Types.StructType statsType = expected.type();
    if (statsType.field("null_value_count") != null) {
      assertThat(actual.hasNullValueCount()).isTrue();
      assertThat(actual.nullValueCount()).isEqualTo(expected.nullValueCount());
    } else {
      assertThat(actual.hasNullValueCount()).isFalse();
    }

    if (statsType.field("nan_value_count") != null) {
      assertThat(actual.hasNanValueCount()).isTrue();
      assertThat(actual.nanValueCount()).isEqualTo(expected.nanValueCount());
    } else {
      assertThat(actual.hasNanValueCount()).isFalse();
    }
  }

  private static void assertBoundingBox(Object bound, double... ordinates) {
    assertThat(bound).isInstanceOf(StructLike.class);
    StructLike box = (StructLike) bound;
    assertThat(box.size()).isEqualTo(ordinates.length);
    for (int pos = 0; pos < ordinates.length; pos += 1) {
      assertThat(box.get(pos, Double.class))
          .as("Bounding box ordinate at position %s", pos)
          .isEqualTo(ordinates[pos]);
    }
  }

  /** Returns a variant bound holding the given value for the "$['x']" path. */
  private static Variant variantBound(VariantMetadata metadata, int value) {
    ShreddedObject object = Variants.object(metadata);
    object.put("$['x']", Variants.of(value));
    return Variant.of(metadata, object);
  }

  private static void assertVariant(Object actual, Variant expected) {
    assertThat(actual).isInstanceOf(Variant.class);
    Variant variant = (Variant) actual;
    VariantTestUtil.assertEqual(expected.metadata(), variant.metadata());
    VariantTestUtil.assertEqual(expected.value(), variant.value());
  }

  private static StructLike boundingBox(
      Types.StructType statsType, String boundName, double... ordinates) {
    PartitionData box = new PartitionData(statsType.fieldType(boundName).asStructType());
    for (int pos = 0; pos < ordinates.length; pos += 1) {
      box.set(pos, ordinates[pos]);
    }

    return box;
  }

  /** Returns stats for every table column, backed by the full content stats type. */
  private static ContentStats contentStats() {
    ContentStatsStruct stats = new ContentStatsStruct(CONTENT_STATS_TYPE);
    stats.setStats(ID_FIELD_ID, ID_STATS);
    stats.setStats(DATA_FIELD_ID, DATA_STATS);
    stats.setStats(MEASURE_FIELD_ID, MEASURE_STATS);
    return stats;
  }

  private static TrackedFile fileWithStats(String location, ContentStats stats) {
    return new TrackedFileStruct(
        new TrackingStruct(EntryStatus.ADDED, 42L, null, null, null, null, null, null),
        FileContent.DATA,
        4,
        location,
        FileFormat.PARQUET,
        100L,
        1024L,
        0,
        EMPTY_PARTITION_DATA,
        stats,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static InputFile writeManifest(
      FileFormat format, Types.StructType contentStatsType, Iterable<TrackedFile> files)
      throws IOException {
    Schema writeSchema = TrackedFile.schema(EMPTY_PARTITION, contentStatsType);
    OutputFile out = new InMemoryOutputFile("manifest." + format.name().toLowerCase(Locale.ROOT));
    try (FileAppender<StructLike> appender =
        InternalData.write(format, out).schema(writeSchema).named("tracked_file").build()) {
      for (TrackedFile file : files) {
        appender.add((StructLike) file);
      }
    }

    return out.toInputFile();
  }
}
