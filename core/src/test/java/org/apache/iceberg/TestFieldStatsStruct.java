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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.TestHelpers.RoundTripSerializer;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TestFieldStatsStruct {
  private static final int BASE_ID = 30_000;

  private static final Types.StructType STRING_STATS =
      StatsUtil.fieldStatsStruct(true, Types.StringType.get(), BASE_ID, MetricsModes.Full.get());

  private static final Types.StructType DOUBLE_STATS =
      StatsUtil.fieldStatsStruct(true, Types.DoubleType.get(), BASE_ID, MetricsModes.Full.get());

  @Test
  public void testFieldAccess() {
    FieldStats<String> stats = new FieldStatsStruct<>(STRING_STATS, "a", "z", true, 28, 2, 0, 1);

    assertThat(stats.fieldId()).isEqualTo(100);
    assertThat(stats.lowerBound()).isEqualTo("a");
    assertThat(stats.upperBound()).isEqualTo("z");
    assertThat(stats.tightBounds()).isTrue();
    assertThat(stats.valueCount()).isEqualTo(28L);
    assertThat(stats.nullValueCount()).isEqualTo(2L);
    assertThat(stats.nanValueCount()).isEqualTo(0L);
    assertThat(stats.avgValueSizeInBytes()).isEqualTo(1);
  }

  @Test
  public void testStringGetByPosition() {
    FieldStatsStruct<String> stats =
        new FieldStatsStruct<>(STRING_STATS, "a", "z", true, 28, 2, 0, 1);

    assertThat(stats.get(pos(STRING_STATS, "lower_bound"), String.class)).isEqualTo("a");
    assertThat(stats.get(pos(STRING_STATS, "upper_bound"), String.class)).isEqualTo("z");
    assertThat(stats.get(pos(STRING_STATS, "tight_bounds"), Boolean.class)).isTrue();
    assertThat(stats.get(pos(STRING_STATS, "value_count"), Long.class)).isEqualTo(28L);
    assertThat(stats.get(pos(STRING_STATS, "null_value_count"), Long.class)).isEqualTo(2L);
    assertThat(stats.get(pos(STRING_STATS, "avg_value_size_in_bytes"), Integer.class)).isEqualTo(1);
  }

  @Test
  public void testStringSetByPosition() {
    FieldStatsStruct<String> stats = new FieldStatsStruct<>(STRING_STATS);

    stats.set(pos(STRING_STATS, "lower_bound"), "a");
    stats.set(pos(STRING_STATS, "upper_bound"), "z");
    stats.set(pos(STRING_STATS, "tight_bounds"), false);
    stats.set(pos(STRING_STATS, "value_count"), 28L);
    stats.set(pos(STRING_STATS, "null_value_count"), 2L);
    stats.set(pos(STRING_STATS, "avg_value_size_in_bytes"), 1);

    assertThat(stats.lowerBound()).isEqualTo("a");
    assertThat(stats.upperBound()).isEqualTo("z");
    assertThat(stats.tightBounds()).isFalse();
    assertThat(stats.valueCount()).isEqualTo(28L);
    assertThat(stats.nullValueCount()).isEqualTo(2L);
    assertThat(stats.avgValueSizeInBytes()).isEqualTo(1);
  }

  @Test
  public void testDoubleGetByPosition() {
    FieldStatsStruct<Double> stats =
        new FieldStatsStruct<>(DOUBLE_STATS, 0.0d, 25.0d, true, 34, 2, 6, 0);

    assertThat(stats.get(pos(DOUBLE_STATS, "lower_bound"), Double.class)).isEqualTo(0.0d);
    assertThat(stats.get(pos(DOUBLE_STATS, "upper_bound"), Double.class)).isEqualTo(25.0d);
    assertThat(stats.get(pos(DOUBLE_STATS, "tight_bounds"), Boolean.class)).isTrue();
    assertThat(stats.get(pos(DOUBLE_STATS, "value_count"), Long.class)).isEqualTo(34L);
    assertThat(stats.get(pos(DOUBLE_STATS, "null_value_count"), Long.class)).isEqualTo(2L);
    assertThat(stats.get(pos(DOUBLE_STATS, "nan_value_count"), Long.class)).isEqualTo(6L);
  }

  @Test
  public void testDoubleSetByPosition() {
    FieldStatsStruct<Double> stats = new FieldStatsStruct<>(DOUBLE_STATS);

    stats.set(pos(DOUBLE_STATS, "lower_bound"), 0.0d);
    stats.set(pos(DOUBLE_STATS, "upper_bound"), 25.0d);
    stats.set(pos(DOUBLE_STATS, "tight_bounds"), false);
    stats.set(pos(DOUBLE_STATS, "value_count"), 34L);
    stats.set(pos(DOUBLE_STATS, "null_value_count"), 2L);
    stats.set(pos(DOUBLE_STATS, "nan_value_count"), 6L);

    assertThat(stats.lowerBound()).isEqualTo(0.0d);
    assertThat(stats.upperBound()).isEqualTo(25.0d);
    assertThat(stats.tightBounds()).isFalse();
    assertThat(stats.valueCount()).isEqualTo(34L);
    assertThat(stats.nullValueCount()).isEqualTo(2L);
    assertThat(stats.nanValueCount()).isEqualTo(6L);
    assertThat(stats.avgValueSizeInBytes()).isNull();
  }

  @Test
  public void testFromFieldMetricsWrongField() {
    FieldStatsStruct<String> stats = new FieldStatsStruct<>(STRING_STATS);
    assertThatThrownBy(() -> stats.fromFieldMetrics(new FieldMetrics<>(21, 50, 0)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot store stats for field ID: 21 (expected 100)");
  }

  @Test
  public void testFromFieldMetricsString() {
    FieldStatsStruct<String> stats = new FieldStatsStruct<>(STRING_STATS);

    stats.fromFieldMetrics(new FieldMetrics<>(100, 28, 2, -1, "a", "z", null, 4));

    assertThat(stats.lowerBound()).isEqualTo("a");
    assertThat(stats.upperBound()).isEqualTo("z");
    assertThat(stats.tightBounds()).isFalse();
    assertThat(stats.valueCount()).isEqualTo(28L);
    assertThat(stats.nullValueCount()).isEqualTo(2L);
    assertThat(stats.avgValueSizeInBytes()).isEqualTo(4);
  }

  @Test
  public void testFromFieldMetricsDouble() {
    FieldStatsStruct<Double> stats = new FieldStatsStruct<>(DOUBLE_STATS);

    stats.fromFieldMetrics(new FieldMetrics<>(100, 34, 2, 6, 0.0d, 25.0d));

    assertThat(stats.lowerBound()).isEqualTo(0.0d);
    assertThat(stats.upperBound()).isEqualTo(25.0d);
    assertThat(stats.tightBounds()).isFalse();
    assertThat(stats.valueCount()).isEqualTo(34L);
    assertThat(stats.nullValueCount()).isEqualTo(2L);
    assertThat(stats.nanValueCount()).isEqualTo(6L);
    assertThat(stats.avgValueSizeInBytes()).isNull(); // unknown
  }

  @Test
  public void testFieldStatsProjection() {
    Types.StructType projection =
        Types.StructType.of(
            optional(BASE_ID + StatsUtil.VALUE_COUNT_OFFSET, "count", Types.StringType.get()),
            optional(BASE_ID + StatsUtil.UPPER_BOUND_OFFSET, "upper", Types.StringType.get()));

    FieldStatsStruct<String> stats = new FieldStatsStruct<>(projection);

    stats.set(0, 34L);
    stats.set(1, "z");
    assertThatThrownBy(() -> stats.set(stats.size(), null))
        .hasMessage("Index 2 out of bounds for length 2")
        .isInstanceOf(IndexOutOfBoundsException.class);

    assertThat(stats.upperBound()).isEqualTo("z");
    assertThat(stats.valueCount()).isEqualTo(34L);

    assertThat(stats.get(0, Long.class)).isEqualTo(34L);
    assertThat(stats.get(1, String.class)).isEqualTo("z");
    assertThatThrownBy(() -> stats.get(stats.size(), Object.class))
        .hasMessage("Index 2 out of bounds for length 2")
        .isInstanceOf(IndexOutOfBoundsException.class);
  }

  private static final List<Arguments> TYPES_AND_BOUNDS =
      List.of(
          Arguments.of(Types.BooleanType.get(), false, true),
          Arguments.of(Types.IntegerType.get(), -5, 100),
          Arguments.of(Types.LongType.get(), 0L, 1_000L),
          Arguments.of(Types.FloatType.get(), 1.5f, 9.5f),
          Arguments.of(Types.DoubleType.get(), 0.0d, 25.0d),
          Arguments.of(Types.DateType.get(), 100, 200),
          Arguments.of(Types.TimeType.get(), 1_000L, 2_000L),
          Arguments.of(Types.TimestampType.withZone(), 111L, 222L),
          Arguments.of(Types.TimestampNanoType.withZone(), 111L, 222L),
          Arguments.of(Types.StringType.get(), "a", "z"),
          Arguments.of(
              Types.UUIDType.get(),
              UUID.fromString("07ceab48-62b2-4219-9172-856e687c92ad"),
              UUID.fromString("a9a7c24d-2869-4c66-8803-b1f6a36256ed")),
          Arguments.of(
              Types.FixedType.ofLength(4),
              ByteBuffer.wrap(new byte[] {0, 1, 2, 3}),
              ByteBuffer.wrap(new byte[] {4, 5, 6, 7})),
          Arguments.of(
              Types.BinaryType.get(),
              ByteBuffer.wrap(new byte[] {1, 2}),
              ByteBuffer.wrap(new byte[] {3, 4, 5})),
          Arguments.of(Types.DecimalType.of(9, 2), new BigDecimal("1.23"), new BigDecimal("9.99")),
          Arguments.of(Types.UnknownType.get(), null, null));

  private static final List<Named<RoundTripSerializer<FieldStatsStruct<?>>>> SERIALIZERS =
      List.of(
          Named.of("Java", TestHelpers::roundTripSerialize),
          Named.of("Kryo", TestHelpers.KryoHelpers::roundTripSerialize),
          Named.of("InternalData", TestFieldStatsStruct::roundTripInternalData),
          Named.of("FieldStats#copy", FieldStatsStruct::copy));

  private static Stream<Arguments> serializationCases() {
    return TYPES_AND_BOUNDS.stream()
        .flatMap(
            typeCase -> {
              Object[] typeAndBounds = typeCase.get();
              return SERIALIZERS.stream()
                  .map(
                      serializer ->
                          Arguments.of(
                              typeAndBounds[0], typeAndBounds[1], typeAndBounds[2], serializer));
            });
  }

  @ParameterizedTest
  @MethodSource("serializationCases")
  public void testSerializationAndCopy(
      Type type,
      Object lowerBound,
      Object upperBound,
      RoundTripSerializer<FieldStatsStruct<?>> serializer)
      throws Exception {
    Types.StructType statsStruct =
        StatsUtil.fieldStatsStruct(true, type, BASE_ID, MetricsModes.Full.get());

    boolean isFloatingPoint =
        type.typeId() == Type.TypeID.FLOAT || type.typeId() == Type.TypeID.DOUBLE;
    boolean isVariableLength =
        type.typeId() == Type.TypeID.STRING || type.typeId() == Type.TypeID.BINARY;

    FieldStatsStruct<Object> stats =
        new FieldStatsStruct<>(
            statsStruct,
            lowerBound,
            upperBound,
            true,
            28L,
            2L,
            isFloatingPoint ? 6L : 0L,
            isVariableLength ? 1 : null);

    Comparator<StructLike> comparator = Comparators.forType(statsStruct);

    FieldStatsStruct<?> copy = serializer.apply(stats);
    assertThat(copy.fieldId()).isEqualTo(stats.fieldId());
    assertThat(copy.type()).isEqualTo(stats.type());
    assertThat(comparator.compare(copy, stats)).isEqualTo(0);
  }

  private static Stream<Arguments> geoCases() {
    return ImmutableList.of(Types.GeometryType.crs84(), Types.GeographyType.crs84()).stream()
        .flatMap(type -> SERIALIZERS.stream().map(serializer -> Arguments.of(type, serializer)));
  }

  @ParameterizedTest
  @MethodSource("geoCases")
  public void testGeoSerialization(
      Type geoType, RoundTripSerializer<FieldStatsStruct<?>> serializer) throws Exception {
    Types.StructType statsStruct =
        StatsUtil.fieldStatsStruct(true, geoType, BASE_ID, MetricsModes.Full.get());

    // geometry and geography use bounding-box structs (x, y, z, m) for their bounds
    PartitionData lowerBound =
        new PartitionData(statsStruct.field("lower_bound").type().asStructType());
    lowerBound.set(0, 1.0d);
    lowerBound.set(1, 2.0d);
    lowerBound.set(2, 3.0d);
    lowerBound.set(3, 4.0d);
    PartitionData upperBound =
        new PartitionData(statsStruct.field("upper_bound").type().asStructType());
    upperBound.set(0, 5.0d);
    upperBound.set(1, 6.0d);
    upperBound.set(2, 7.0d);
    upperBound.set(3, 8.0d);

    FieldStatsStruct<Object> stats =
        new FieldStatsStruct<>(statsStruct, lowerBound, upperBound, false, 28L, 2L, 0L, null);

    Comparator<StructLike> comparator = Comparators.forType(statsStruct);

    FieldStatsStruct<?> copy = serializer.apply(stats);
    assertThat(copy.fieldId()).isEqualTo(stats.fieldId());
    assertThat(copy.type()).isEqualTo(stats.type());
    assertThat(comparator.compare(copy, stats)).isEqualTo(0);
  }

  // Variant is not Serializable so this does not test Java serialization
  private static final List<Named<RoundTripSerializer<FieldStatsStruct<?>>>> VARIANT_SERIALIZERS =
      List.of(
          Named.of("Kryo", TestHelpers.KryoHelpers::roundTripSerialize),
          Named.of("InternalData", TestFieldStatsStruct::roundTripInternalData),
          Named.of("FieldStats#copy", FieldStatsStruct::copy));

  @ParameterizedTest
  @FieldSource("VARIANT_SERIALIZERS")
  public void testVariantSerialization(RoundTripSerializer<FieldStatsStruct<?>> serializer)
      throws Exception {
    Types.StructType statsStruct =
        StatsUtil.fieldStatsStruct(true, Types.VariantType.get(), BASE_ID, MetricsModes.Full.get());

    // variant bounds are variants keyed by JSON path expression that hold the bounds of fields
    // within the variant; here the field "$['x']" ranges from 1 to 10
    VariantMetadata metadata = Variants.metadata("$['x']");

    ShreddedObject lowerObject = Variants.object(metadata);
    lowerObject.put("$['x']", Variants.of(1));
    Variant lowerBound = Variant.of(metadata, lowerObject);

    ShreddedObject upperObject = Variants.object(metadata);
    upperObject.put("$['x']", Variants.of(10));
    Variant upperBound = Variant.of(metadata, upperObject);

    int size = metadata.dictionarySize() + lowerObject.sizeInBytes();

    FieldStatsStruct<Object> stats =
        new FieldStatsStruct<>(statsStruct, lowerBound, upperBound, false, 28L, 2L, 0L, size);

    FieldStatsStruct<?> copy = serializer.apply(stats);
    assertThat(copy.fieldId()).isEqualTo(stats.fieldId());
    assertThat(copy.type()).isEqualTo(stats.type());
    assertThat(copy.valueCount()).isEqualTo(28L);
    assertThat(copy.nullValueCount()).isEqualTo(2L);
    assertThat(copy.avgValueSizeInBytes()).isEqualTo(size);

    Variant copyLower = (Variant) copy.lowerBound();
    VariantTestUtil.assertEqual(lowerBound.metadata(), copyLower.metadata());
    VariantTestUtil.assertEqual(lowerBound.value(), copyLower.value());
    Variant copyUpper = (Variant) copy.upperBound();
    VariantTestUtil.assertEqual(upperBound.metadata(), copyUpper.metadata());
    VariantTestUtil.assertEqual(upperBound.value(), copyUpper.value());
  }

  private static <T> FieldStatsStruct<T> roundTripInternalData(FieldStats<T> stats)
      throws IOException {
    Schema schema = stats.type().asSchema();
    InMemoryOutputFile file = new InMemoryOutputFile("internal.avro");

    try (FileAppender<FieldStats<T>> writer =
        InternalData.write(FileFormat.AVRO, file).schema(schema).build()) {
      writer.add(stats);
    }

    try (CloseableIterable<FieldStatsStruct<T>> reader =
        InternalData.read(FileFormat.AVRO, file.toInputFile())
            .project(schema)
            .setRootType(FieldStatsStruct.class)
            .build()) {
      return Iterables.getOnlyElement(reader);
    }
  }

  private static int pos(Types.StructType type, String fieldName) {
    List<Types.NestedField> fields = type.fields();
    for (int pos = 0; pos < fields.size(); pos += 1) {
      if (fields.get(pos).name().equals(fieldName)) {
        return pos;
      }
    }

    throw new IllegalArgumentException("Missing field: " + fieldName);
  }
}
