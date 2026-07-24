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
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.TestHelpers.RoundTripSerializer;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.mockito.Mockito;

public class TestContentStatsStruct {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "score", Types.DoubleType.get()));

  private static final Types.StructType CONTENT_STATS_STRUCT =
      StatsUtil.statsReadSchema(SCHEMA, ImmutableList.of(1, 2, 3));
  private static final Types.StructType UNKNOWN_FIELD_STATS_STRUCT =
      StatsUtil.fieldStatsStruct(false, Types.IntegerType.get(), 10_800, MetricsModes.Full.get());

  private static final FieldStats<Long> ID_STATS =
      StatsTestUtil.fieldStats(
          CONTENT_STATS_STRUCT.field("id").type().asStructType(),
          0L,
          25L,
          true,
          26L,
          null,
          null,
          null);
  private static final FieldStats<String> DATA_STATS =
      StatsTestUtil.fieldStats(
          CONTENT_STATS_STRUCT.field("data").type().asStructType(),
          "a",
          "z",
          true,
          26L,
          0L,
          null,
          null);

  @Test
  public void testEmptyContentStats() {
    ContentStats stats = new ContentStatsStruct(CONTENT_STATS_STRUCT);

    assertThat(stats.statsFor(1)).isNull();
    assertThat(stats.statsFor(2)).isNull();
    assertThat(stats.statsFor(3)).isNull();
    assertThat(stats.statsFor(4)).as("Should ignore unknown field IDs").isNull();
  }

  @Test
  public void testSetStats() {
    ContentStatsStruct stats = new ContentStatsStruct(CONTENT_STATS_STRUCT);

    stats.setStats(1, ID_STATS);
    stats.setStats(2, DATA_STATS);

    assertThat(stats.statsFor(1)).isEqualTo(ID_STATS);
    assertThat(stats.statsFor(2)).isEqualTo(DATA_STATS);
    assertThat(stats.statsFor(3)).isNull();
  }

  @Test
  public void testSetStatsWrongId() {
    ContentStatsStruct stats = new ContentStatsStruct(CONTENT_STATS_STRUCT);

    assertThatThrownBy(() -> stats.setStats(2, ID_STATS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Mismatched field stats for ID 2: actual ID 1");
  }

  @Test
  public void testSetStatsUnknownField() {
    ContentStatsStruct stats = new ContentStatsStruct(CONTENT_STATS_STRUCT);

    FieldStats<Integer> unknownStats =
        StatsTestUtil.fieldStats(UNKNOWN_FIELD_STATS_STRUCT, 0, 10, false, 8L, null, null, null);

    assertThatThrownBy(() -> stats.setStats(4, unknownStats))
        .hasMessage("Cannot set stats for unknown field ID: 4")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testGetByPosition() {
    // the content stats struct with a known field order
    Types.StructType contentStatsStruct =
        Types.StructType.of(
            CONTENT_STATS_STRUCT.field("data"),
            CONTENT_STATS_STRUCT.field("score"),
            CONTENT_STATS_STRUCT.field("id"));

    ContentStatsStruct stats = new ContentStatsStruct(contentStatsStruct);

    stats.setStats(1, ID_STATS);
    stats.setStats(2, DATA_STATS);

    assertThat(stats.get(0, FieldStats.class)).isEqualTo(DATA_STATS);
    assertThat(stats.get(1, FieldStats.class)).isNull();
    assertThat(stats.get(2, FieldStats.class)).isEqualTo(ID_STATS);
  }

  @Test
  public void testSetByPosition() {
    // the content stats struct with a known field order
    Types.StructType statsStruct =
        Types.StructType.of(
            CONTENT_STATS_STRUCT.field("data"),
            CONTENT_STATS_STRUCT.field("score"),
            CONTENT_STATS_STRUCT.field("id"));

    ContentStatsStruct stats = new ContentStatsStruct(statsStruct);

    stats.set(1, null);
    stats.set(2, ID_STATS);
    stats.set(0, DATA_STATS);

    assertThat(stats.statsFor(SCHEMA.findField("id").fieldId())).isEqualTo(ID_STATS);
    assertThat(stats.statsFor(SCHEMA.findField("data").fieldId())).isEqualTo(DATA_STATS);
    assertThat(stats.statsFor(SCHEMA.findField("score").fieldId())).isNull();
  }

  @Test
  public void testSize() {
    ContentStatsStruct stats = new ContentStatsStruct(CONTENT_STATS_STRUCT);

    assertThat(stats.size()).isEqualTo(3);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCopy() {
    FieldStats<Long> idStats = Mockito.mock(FieldStats.class);
    FieldStats<Long> idStatsCopy = Mockito.mock(FieldStats.class);
    Mockito.when(idStats.fieldId()).thenReturn(1);
    Mockito.when(idStats.copy()).thenReturn(idStatsCopy);

    FieldStats<String> dataStats = Mockito.mock(FieldStats.class);
    FieldStats<String> dataStatsCopy = Mockito.mock(FieldStats.class);
    Mockito.when(dataStats.fieldId()).thenReturn(2);
    Mockito.when(dataStats.copy()).thenReturn(dataStatsCopy);

    ContentStatsStruct stats = new ContentStatsStruct(CONTENT_STATS_STRUCT);
    stats.setStats(1, idStats);
    stats.setStats(2, dataStats);

    ContentStats copy = stats.copy();

    assertThat(copy).isInstanceOf(ContentStatsStruct.class).isNotSameAs(stats);
    assertThat(copy.type()).isEqualTo(stats.type());

    // each field stats is deep-copied using its own copy() method
    assertThat(copy.statsFor(1)).isSameAs(idStatsCopy);
    assertThat(copy.statsFor(2)).isSameAs(dataStatsCopy);
    assertThat(copy.statsFor(3)).isNull();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFilteredCopy() {
    FieldStats<Long> idStats = Mockito.mock(FieldStats.class);
    FieldStats<Long> idStatsCopy = Mockito.mock(FieldStats.class);
    Mockito.when(idStats.fieldId()).thenReturn(1);
    Mockito.when(idStats.copy()).thenReturn(idStatsCopy);

    FieldStats<String> dataStats = Mockito.mock(FieldStats.class);
    FieldStats<String> dataStatsCopy = Mockito.mock(FieldStats.class);
    Mockito.when(dataStats.fieldId()).thenReturn(2);
    Mockito.when(dataStats.copy()).thenReturn(dataStatsCopy);

    ContentStatsStruct stats = new ContentStatsStruct(CONTENT_STATS_STRUCT);
    stats.setStats(1, idStats);
    stats.setStats(2, dataStats);

    // copy just the stats for field ID 2 / data
    ContentStats copy = stats.copy(ImmutableSet.of(2));

    assertThat(copy).isInstanceOf(ContentStatsStruct.class).isNotSameAs(stats);
    assertThat(copy.type()).isEqualTo(Types.StructType.of(CONTENT_STATS_STRUCT.field("data")));

    // each field stats is deep-copied using its own copy() method
    assertThat(copy.statsFor(1)).isNull();
    assertThat(copy.statsFor(2)).isSameAs(dataStatsCopy);
    assertThat(copy.statsFor(3)).isNull();
  }

  private static final List<Named<RoundTripSerializer<ContentStatsStruct>>> SERIALIZERS =
      List.of(
          Named.of("Java", TestHelpers::roundTripSerialize),
          Named.of("Kryo", TestHelpers.KryoHelpers::roundTripSerialize),
          Named.of("InternalData", TestContentStatsStruct::roundTripInternalData));

  @ParameterizedTest
  @FieldSource("SERIALIZERS")
  public void testSerialization(RoundTripSerializer<ContentStatsStruct> serializer)
      throws Exception {
    ContentStatsStruct stats = new ContentStatsStruct(CONTENT_STATS_STRUCT);
    stats.setStats(1, ID_STATS);
    stats.setStats(2, DATA_STATS);

    Comparator<StructLike> comparator = Comparators.forType(CONTENT_STATS_STRUCT);

    ContentStatsStruct copy = serializer.apply(stats);
    assertThat(copy.type()).isEqualTo(stats.type());
    assertThat(comparator.compare(copy, stats)).isEqualTo(0);
  }

  private static ContentStatsStruct roundTripInternalData(ContentStatsStruct stats)
      throws IOException {
    Schema schema = stats.type().asSchema();
    InMemoryOutputFile file = new InMemoryOutputFile("internal.avro");

    try (FileAppender<ContentStatsStruct> writer =
        InternalData.write(FileFormat.AVRO, file).schema(schema).build()) {
      writer.add(stats);
    }

    InternalData.ReadBuilder read =
        InternalData.read(FileFormat.AVRO, file.toInputFile())
            .project(schema)
            .setRootType(ContentStatsStruct.class);
    // the nested field stats structs are read as FieldStatsStruct
    for (Types.NestedField field : stats.type().fields()) {
      read = read.setCustomType(field.fieldId(), FieldStatsStruct.class);
    }

    try (CloseableIterable<ContentStatsStruct> reader = read.build()) {
      return Iterables.getOnlyElement(reader);
    }
  }
}
