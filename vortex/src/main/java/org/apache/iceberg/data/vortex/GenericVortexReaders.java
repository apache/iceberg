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
package org.apache.iceberg.data.vortex;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.data.GenericDataUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.iceberg.vortex.VortexSchemas;
import org.apache.iceberg.vortex.VortexValueReader;

public class GenericVortexReaders {
  private GenericVortexReaders() {}

  public static VortexValueReader<Boolean> bools() {
    return BooleanReader.INSTANCE;
  }

  public static VortexValueReader<Integer> ints() {
    return IntegerReader.INSTANCE;
  }

  public static VortexValueReader<BigDecimal> decimals() {
    return DecimalReader.INSTANCE;
  }

  public static VortexValueReader<Long> longs() {
    return LongReader.INSTANCE;
  }

  public static VortexValueReader<Float> floats() {
    return FloatReader.INSTANCE;
  }

  public static VortexValueReader<Double> doubles() {
    return DoubleReader.INSTANCE;
  }

  public static VortexValueReader<String> strings() {
    return StringReader.INSTANCE;
  }

  public static VortexValueReader<ByteBuffer> bytes() {
    return BytesReader.INSTANCE;
  }

  public static VortexValueReader<UUID> uuids() {
    return UuidReader.INSTANCE;
  }

  public static VortexValueReader<Variant> variants() {
    return VariantReader.INSTANCE;
  }

  public static VortexValueReader<LocalDate> date(boolean isMillis) {
    return new DateReader(isMillis);
  }

  public static VortexValueReader<LocalTime> time(boolean nanosecond) {
    return new TimeReader(nanosecond);
  }

  public static VortexValueReader<LocalDateTime> timestamp(boolean nanosecond) {
    return new TimestampReader(nanosecond);
  }

  public static VortexValueReader<OffsetDateTime> timestampTz(String timeZone, boolean nanosecond) {
    return new TimestampTzReader(timeZone, nanosecond);
  }

  public static VortexValueReader<Record> struct(
      Types.StructType schema, List<Field> fields, List<VortexValueReader<?>> readers) {
    return new StructReader(schema, fields, readers);
  }

  public static <T> VortexValueReader<List<T>> list(VortexValueReader<T> elementReader) {
    return new ListReader<>(elementReader);
  }

  /**
   * Returns a reader that always produces {@code constant}, ignoring the Arrow vector and row.
   *
   * <p>Used to inject identity-partition values and metadata columns (for example {@code _file} or
   * {@code _spec_id}) that are supplied through {@code idToConstant} rather than being read from
   * the data file.
   */
  public static <C> VortexValueReader<C> constants(C constant) {
    return new ConstantReader<>(constant);
  }

  private static class StructReader implements VortexValueReader<Record> {
    private final Types.StructType schema;
    // File column name backing each expected field, or null when the field is absent from the file.
    private final String[] childNames;
    private final List<VortexValueReader<?>> readers;

    private StructReader(
        Types.StructType schema, List<Field> fields, List<VortexValueReader<?>> readers) {
      this.schema = schema;
      this.readers = Lists.newArrayListWithCapacity(readers.size());
      this.childNames = new String[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        this.childNames[i] = field == null ? null : field.getName();
        VortexValueReader<?> reader = readers.get(i);
        if (reader != null) {
          this.readers.add(reader);
        } else {
          Types.NestedField expectedField = schema.fields().get(i);
          if (expectedField.initialDefault() != null) {
            this.readers.add(
                constants(
                    GenericDataUtil.internalToGeneric(
                        expectedField.type(), expectedField.initialDefault())));
          } else if (expectedField.isOptional()) {
            this.readers.add(constants(null));
          } else {
            throw new IllegalArgumentException(
                String.format("Missing required field: %s", expectedField.name()));
          }
        }
      }
    }

    @Override
    public Record readNonNull(FieldVector vector, int row) {
      StructVector struct = (StructVector) vector;
      GenericRecord record = GenericRecord.create(schema);
      for (int i = 0; i < readers.size(); i++) {
        VortexValueReader<?> reader = readers.get(i);
        if (childNames[i] == null) {
          record.set(i, reader.read(null, row));
        } else {
          FieldVector child = (FieldVector) struct.getChild(childNames[i]);
          record.set(i, reader.read(child, row));
        }
      }
      return record;
    }
  }

  private static class ListReader<T> implements VortexValueReader<List<T>> {
    private final VortexValueReader<T> elementReader;

    private ListReader(VortexValueReader<T> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public List<T> readNonNull(FieldVector vector, int row) {
      ListVector listVector = (ListVector) vector;
      int start = listVector.getElementStartIndex(row);
      int end = listVector.getElementEndIndex(row);
      FieldVector elementVector = listVector.getDataVector();
      return IntStream.range(start, end)
          .mapToObj(i -> elementReader.read(elementVector, i))
          .toList();
    }
  }

  private static class ConstantReader<C> implements VortexValueReader<C> {
    private final C constant;

    private ConstantReader(C constant) {
      this.constant = constant;
    }

    @Override
    public C read(FieldVector vector, int row) {
      return constant;
    }

    @Override
    public C readNonNull(FieldVector vector, int row) {
      return constant;
    }
  }

  private static class BooleanReader implements VortexValueReader<Boolean> {
    static final BooleanReader INSTANCE = new BooleanReader();

    private BooleanReader() {}

    @Override
    public Boolean readNonNull(FieldVector vector, int row) {
      return ((BitVector) vector).get(row) != 0;
    }
  }

  private static class IntegerReader implements VortexValueReader<Integer> {
    static final IntegerReader INSTANCE = new IntegerReader();

    private IntegerReader() {}

    @Override
    public Integer readNonNull(FieldVector vector, int row) {
      return (int) ((BaseIntVector) vector).getValueAsLong(row);
    }
  }

  private static class LongReader implements VortexValueReader<Long> {
    static final LongReader INSTANCE = new LongReader();

    private LongReader() {}

    @Override
    public Long readNonNull(FieldVector vector, int row) {
      return ((BaseIntVector) vector).getValueAsLong(row);
    }
  }

  private static class DecimalReader implements VortexValueReader<BigDecimal> {
    static final DecimalReader INSTANCE = new DecimalReader();

    private DecimalReader() {}

    @Override
    public BigDecimal readNonNull(FieldVector vector, int row) {
      return ((DecimalVector) vector).getObjectNotNull(row);
    }
  }

  private static class FloatReader implements VortexValueReader<Float> {
    static final FloatReader INSTANCE = new FloatReader();

    private FloatReader() {}

    @Override
    public Float readNonNull(FieldVector vector, int row) {
      return ((Float4Vector) vector).get(row);
    }
  }

  private static class DoubleReader implements VortexValueReader<Double> {
    static final DoubleReader INSTANCE = new DoubleReader();

    private DoubleReader() {}

    @Override
    public Double readNonNull(FieldVector vector, int row) {
      return ((Float8Vector) vector).get(row);
    }
  }

  private static class StringReader implements VortexValueReader<String> {
    static final StringReader INSTANCE = new StringReader();

    private StringReader() {}

    @Override
    public String readNonNull(FieldVector vector, int row) {
      return new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8);
    }
  }

  private static class BytesReader implements VortexValueReader<ByteBuffer> {
    static final BytesReader INSTANCE = new BytesReader();

    private BytesReader() {}

    @Override
    public ByteBuffer readNonNull(FieldVector vector, int row) {
      return ByteBuffer.wrap(((VarBinaryVector) vector).get(row));
    }
  }

  private static class UuidReader implements VortexValueReader<UUID> {
    static final UuidReader INSTANCE = new UuidReader();

    private UuidReader() {}

    @Override
    public UUID readNonNull(FieldVector vector, int row) {
      return UUIDUtil.convert(uuidStorage(vector).get(row));
    }
  }

  /**
   * Returns the underlying {@link FixedSizeBinaryVector} for a UUID column. Vortex may emit either
   * a registered extension vector (wrapping FixedSizeBinary) or the raw fixed-binary storage,
   * depending on whether {@code arrow.uuid} is registered in the consumer's {@link
   * org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry}.
   */
  static FixedSizeBinaryVector uuidStorage(FieldVector vector) {
    if (vector instanceof ExtensionTypeVector<?> ext) {
      return (FixedSizeBinaryVector) ext.getUnderlyingVector();
    }
    return (FixedSizeBinaryVector) vector;
  }

  private static class VariantReader implements VortexValueReader<Variant> {
    static final VariantReader INSTANCE = new VariantReader();

    private VariantReader() {}

    @Override
    public Variant read(FieldVector vector, int row) {
      StructVector storage = variantStorage(vector);
      if (vector.isNull(row)) {
        return null;
      }

      return readVariant(storage, row);
    }

    @Override
    public Variant readNonNull(FieldVector vector, int row) {
      return readVariant(variantStorage(vector), row);
    }

    private Variant readVariant(StructVector storage, int row) {
      VarBinaryVector metadataVector = storage.getChild("metadata", VarBinaryVector.class);
      if (metadataVector == null || metadataVector.isNull(row)) {
        throw new IllegalStateException("Invalid Vortex variant: metadata is null");
      }

      byte[] metadataBytes = metadataVector.get(row);
      if (metadataBytes.length == 0) {
        throw new IllegalStateException("Invalid Vortex variant: serialized metadata is empty");
      }

      VariantMetadata metadata =
          VariantMetadata.from(ByteBuffer.wrap(metadataBytes).order(ByteOrder.LITTLE_ENDIAN));
      VariantValue value = readValue(storage, metadata, row);
      return Variant.of(metadata, value != null ? value : Variants.ofNull());
    }
  }

  private static VariantValue readValue(StructVector storage, VariantMetadata metadata, int row) {
    VariantValue serialized = readSerialized(storage, metadata, row);
    FieldVector typedVector = (FieldVector) storage.getChild("typed_value");
    if (typedVector == null || typedVector.isNull(row)) {
      return serialized;
    }

    VariantValue typed = readTypedValue(typedVector, metadata, row, serialized);
    if (!(typedVector instanceof StructVector)) {
      Preconditions.checkArgument(
          serialized == null,
          "Invalid variant, conflicting value and typed_value: value=%s typed_value=%s",
          serialized,
          typed);
    }

    return typed;
  }

  private static VariantValue readSerialized(
      StructVector storage, VariantMetadata metadata, int row) {
    VarBinaryVector valueVector = storage.getChild("value", VarBinaryVector.class);
    if (valueVector == null || valueVector.isNull(row)) {
      return null;
    }

    byte[] valueBytes = valueVector.get(row);
    Preconditions.checkArgument(
        valueBytes.length > 0, "Invalid Vortex variant: serialized value is empty");
    return VariantValue.from(metadata, ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN));
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static VariantValue readTypedValue(
      FieldVector vector, VariantMetadata metadata, int row, VariantValue serialized) {
    ArrowType type = vector.getField().getType();
    if (type instanceof ArrowType.Struct) {
      return readObject((StructVector) vector, metadata, row, serialized);
    } else if (type instanceof ArrowType.List) {
      return readArray((ListVector) vector, metadata, row);
    } else if (type instanceof ArrowType.Bool) {
      return Variants.of(((BitVector) vector).get(row) != 0);
    } else if (type instanceof ArrowType.Int intType) {
      Preconditions.checkArgument(
          intType.getIsSigned(), "Unsupported unsigned shredded variant type: %s", type);
      long value = ((BaseIntVector) vector).getValueAsLong(row);
      return switch (intType.getBitWidth()) {
        case Byte.SIZE -> Variants.of((byte) value);
        case Short.SIZE -> Variants.of((short) value);
        case Integer.SIZE -> Variants.of((int) value);
        case Long.SIZE -> Variants.of(value);
        default -> throw new IllegalArgumentException("Invalid shredded integer type: " + type);
      };
    } else if (type instanceof ArrowType.FloatingPoint floatingPoint) {
      return switch (floatingPoint.getPrecision()) {
        case SINGLE -> Variants.of(((Float4Vector) vector).get(row));
        case DOUBLE -> Variants.of(((Float8Vector) vector).get(row));
        default ->
            throw new UnsupportedOperationException(
                "Unsupported shredded floating-point type: " + type);
      };
    } else if (type instanceof ArrowType.Decimal decimalType) {
      PhysicalType physicalType;
      if (decimalType.getPrecision() <= 9) {
        physicalType = PhysicalType.DECIMAL4;
      } else if (decimalType.getPrecision() <= 18) {
        physicalType = PhysicalType.DECIMAL8;
      } else {
        physicalType = PhysicalType.DECIMAL16;
      }

      return Variants.of(physicalType, ((DecimalVector) vector).getObjectNotNull(row));
    } else if (type instanceof ArrowType.Date dateType) {
      int days =
          switch (dateType.getUnit()) {
            case DAY -> ((DateDayVector) vector).get(row);
            case MILLISECOND ->
                (int) Math.floorDiv(((DateMilliVector) vector).get(row), 86_400_000L);
          };
      return Variants.ofDate(days);
    } else if (type instanceof ArrowType.Time timeType) {
      long value = ((Number) vector.getObject(row)).longValue();
      long micros =
          switch (timeType.getUnit()) {
            case SECOND -> value * 1_000_000L;
            case MILLISECOND -> value * 1_000L;
            case MICROSECOND -> value;
            case NANOSECOND -> value / 1_000L;
          };
      return Variants.ofTime(micros);
    } else if (type instanceof ArrowType.Timestamp timestampType) {
      long value = ((TimeStampVector) vector).get(row);
      boolean nanos = timestampType.getUnit() == org.apache.arrow.vector.types.TimeUnit.NANOSECOND;
      long timestamp =
          switch (timestampType.getUnit()) {
            case SECOND -> value * 1_000_000L;
            case MILLISECOND -> value * 1_000L;
            case MICROSECOND, NANOSECOND -> value;
          };
      if (timestampType.getTimezone() != null) {
        return nanos ? Variants.ofTimestamptzNanos(timestamp) : Variants.ofTimestamptz(timestamp);
      }

      return nanos ? Variants.ofTimestampntzNanos(timestamp) : Variants.ofTimestampntz(timestamp);
    } else if (type instanceof ArrowType.Utf8) {
      return Variants.of(new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8));
    } else if (VortexSchemas.isUuidField(vector.getField())) {
      return Variants.ofUUID(UUIDUtil.convert(uuidStorage(vector).get(row)));
    } else if (type instanceof ArrowType.Binary) {
      return Variants.of(ByteBuffer.wrap(((VarBinaryVector) vector).get(row)));
    } else if (type instanceof ArrowType.FixedSizeBinary) {
      return Variants.of(ByteBuffer.wrap(((FixedSizeBinaryVector) vector).get(row)));
    }

    throw new UnsupportedOperationException("Unsupported shredded variant type: " + type);
  }

  private static VariantValue readObject(
      StructVector vector, VariantMetadata metadata, int row, VariantValue serialized) {
    ShreddedObject object;
    if (serialized == null) {
      object = Variants.object(metadata);
    } else {
      Preconditions.checkArgument(
          serialized.type() == PhysicalType.OBJECT,
          "Invalid variant, non-object value with shredded fields: %s",
          serialized);
      object = Variants.object(metadata, serialized.asObject());
    }

    for (FieldVector child : vector.getChildrenFromFields()) {
      Preconditions.checkArgument(
          child instanceof StructVector,
          "Invalid shredded variant field %s: expected struct, found %s",
          child.getName(),
          child.getField().getType());
      VariantValue fieldValue =
          child.isNull(row) ? null : readValue((StructVector) child, metadata, row);
      if (fieldValue == null) {
        object.remove(child.getName());
      } else {
        object.put(child.getName(), fieldValue);
      }
    }

    return object;
  }

  private static ValueArray readArray(ListVector vector, VariantMetadata metadata, int row) {
    int start = vector.getElementStartIndex(row);
    int end = vector.getElementEndIndex(row);
    FieldVector elements = vector.getDataVector();
    Preconditions.checkArgument(
        elements instanceof StructVector,
        "Invalid shredded variant array element: expected struct, found %s",
        elements.getField().getType());

    ValueArray array = Variants.array();
    for (int index = start; index < end; index++) {
      VariantValue element =
          elements.isNull(index) ? null : readValue((StructVector) elements, metadata, index);
      array.add(element != null ? element : Variants.ofNull());
    }

    return array;
  }

  private static StructVector variantStorage(FieldVector vector) {
    if (vector instanceof ExtensionTypeVector<?> ext) {
      return (StructVector) ext.getUnderlyingVector();
    }
    return (StructVector) vector;
  }

  private static class DateReader implements VortexValueReader<LocalDate> {
    private final boolean isMillis;

    DateReader(boolean isMillis) {
      this.isMillis = isMillis;
    }

    @Override
    public LocalDate readNonNull(FieldVector vector, int row) {
      int days;
      if (isMillis) {
        days = (int) Math.floorDiv(((DateMilliVector) vector).get(row), 86_400_000L);
      } else {
        days = ((DateDayVector) vector).get(row);
      }

      return DateTimeUtil.dateFromDays(days);
    }
  }

  private static class TimestampReader implements VortexValueReader<LocalDateTime> {
    private final boolean nanosecond;

    private TimestampReader(boolean nanosecond) {
      this.nanosecond = nanosecond;
    }

    @Override
    public LocalDateTime readNonNull(FieldVector vector, int row) {
      long measure = ((TimeStampVector) vector).get(row);
      if (nanosecond) {
        return DateTimeUtil.timestampFromNanos(measure);
      } else {
        return DateTimeUtil.timestampFromMicros(measure);
      }
    }
  }

  private static class TimeReader implements VortexValueReader<LocalTime> {
    private final boolean nanosecond;

    private TimeReader(boolean nanosecond) {
      this.nanosecond = nanosecond;
    }

    @Override
    public LocalTime readNonNull(FieldVector vector, int row) {
      if (nanosecond) {
        return LocalTime.ofNanoOfDay(((TimeNanoVector) vector).get(row));
      } else {
        return DateTimeUtil.timeFromMicros(((TimeMicroVector) vector).get(row));
      }
    }
  }

  private static class TimestampTzReader implements VortexValueReader<OffsetDateTime> {
    private final ZoneId timeZone;
    private final boolean nanosecond;

    private TimestampTzReader(String timeZone, boolean nanosecond) {
      this.timeZone = ZoneId.of(timeZone);
      this.nanosecond = nanosecond;
    }

    @Override
    public OffsetDateTime readNonNull(FieldVector vector, int row) {
      long measure = ((TimeStampVector) vector).get(row);
      long nanoAdjustment;
      if (nanosecond) {
        nanoAdjustment = measure;
      } else {
        nanoAdjustment = Math.multiplyExact(1_000L, measure);
      }
      return OffsetDateTime.ofInstant(Instant.EPOCH.plusNanos(nanoAdjustment), timeZone);
    }
  }
}
