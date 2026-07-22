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
import org.apache.arrow.memory.ArrowBuf;
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
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
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
import org.apache.iceberg.vortex.BoundVortexReader;
import org.apache.iceberg.vortex.VortexArrowProperties;
import org.apache.iceberg.vortex.VortexSchemas;
import org.apache.iceberg.vortex.VortexValueReader;

/**
 * Value readers for Arrow batches produced by Vortex scans.
 *
 * <p>Readers are stateful: {@link VortexValueReader#bind(FieldVector)} must be called with each new
 * batch's vector before reading rows. Binding hoists per-batch work out of the per-row path:
 * validity is checked against the bound null count and validity buffer instead of per-row {@code
 * isNull} calls, struct children are resolved by name once instead of per row, and decimal
 * precision/scale and data buffers are captured once per batch.
 */
public class GenericVortexReaders {
  static {
    // Configure Arrow's unsafe-memory-access and null-check-for-get properties like
    // VectorizedSparkParquetReaders does; readers always check nullability before value getters,
    // so Arrow's per-get null checks and bounds checks are redundant on this path.
    VortexArrowProperties.ensureConfigured();
  }

  // Largest decimal precision whose unscaled value always fits in a long.
  private static final int MAX_LONG_PRECISION = 18;

  private GenericVortexReaders() {}

  /**
   * Mirrors the vectorized read path: a decimal with precision of at most 18 digits fits in a long,
   * so its little-endian decimal128 low 8 bytes are the full value with the upper half being sign
   * extension. Reading them directly avoids materializing a 16-byte array and a {@link
   * java.math.BigInteger} per value. Only valid on little-endian hardware.
   */
  private static BigDecimal readBigDecimal(DecimalVector vector, int row) {
    if (vector.getPrecision() <= MAX_LONG_PRECISION) {
      long unscaled = vector.getDataBuffer().getLong((long) row * DecimalVector.TYPE_WIDTH);
      return BigDecimal.valueOf(unscaled, vector.getScale());
    }

    return vector.getObjectNotNull(row);
  }

  public static VortexValueReader<Boolean> bools() {
    return new BooleanReader();
  }

  public static VortexValueReader<Integer> ints() {
    return new IntegerReader();
  }

  public static VortexValueReader<BigDecimal> decimals() {
    return new DecimalReader();
  }

  public static VortexValueReader<Long> longs() {
    return new LongReader();
  }

  /** Reader for int columns projected as longs (schema evolution int-to-long promotion). */
  public static VortexValueReader<Long> intsAsLongs() {
    return new LongReader();
  }

  /**
   * Reader for the {@code _row_id} metadata column, bound to a struct packed by the scan as {@code
   * {value: <stored _row_id, when the file has one>, pos: row_idx}}. Stored values win; null (or
   * absent) values inherit {@code firstRowId + position}, per the row lineage spec.
   */
  public static VortexValueReader<Long> rowIds(long firstRowId) {
    return new RowIdReader(firstRowId);
  }

  /** Reader for long columns that substitutes a constant when the stored value is null. */
  public static VortexValueReader<Long> longsOrDefault(long defaultValue) {
    return new LongOrDefaultReader(defaultValue);
  }

  /** Reader for float columns projected as doubles (schema evolution float-to-double promotion). */
  public static VortexValueReader<Double> floatsAsDoubles() {
    return new FloatAsDoubleReader();
  }

  public static VortexValueReader<Float> floats() {
    return new FloatReader();
  }

  public static VortexValueReader<Double> doubles() {
    return new DoubleReader();
  }

  public static VortexValueReader<String> strings() {
    return new StringReader();
  }

  /** Reader for Utf8View columns, which Vortex scans return for strings. */
  public static VortexValueReader<String> stringsView() {
    return new StringViewReader();
  }

  public static VortexValueReader<ByteBuffer> bytes() {
    return new BytesReader();
  }

  /** Reader for BinaryView columns, which Vortex scans return for binary. */
  public static VortexValueReader<ByteBuffer> bytesView() {
    return new BytesViewReader();
  }

  public static VortexValueReader<UUID> uuids() {
    return new UuidReader();
  }

  public static VortexValueReader<Variant> variants() {
    return new VariantReader();
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
   * Returns a reader that always produces {@code constant}, ignoring the bound vector and row.
   *
   * <p>Used to inject identity-partition values and metadata columns (for example {@code _file} or
   * {@code _spec_id}) that are supplied through {@code idToConstant} rather than being read from
   * the data file.
   */
  public static <C> VortexValueReader<C> constants(C constant) {
    return new ConstantReader<>(constant);
  }

  private static class StructReader extends BoundVortexReader<Record> {
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
    protected void bindVector(FieldVector vector) {
      StructVector struct = (StructVector) vector;
      for (int i = 0; i < childNames.length; i++) {
        if (childNames[i] != null) {
          FieldVector child = (FieldVector) struct.getChild(childNames[i]);
          Preconditions.checkState(
              child != null, "Vortex batch is missing struct child: %s", childNames[i]);
          readers.get(i).bind(child);
        }
      }
    }

    @Override
    public Record readNonNull(int row) {
      GenericRecord record = GenericRecord.create(schema);
      for (int i = 0; i < readers.size(); i++) {
        record.set(i, readers.get(i).read(row));
      }
      return record;
    }
  }

  private static class ListReader<T> extends BoundVortexReader<List<T>> {
    private final VortexValueReader<T> elementReader;
    private ListVector listVector;

    private ListReader(VortexValueReader<T> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    protected void bindVector(FieldVector vector) {
      this.listVector = (ListVector) vector;
      elementReader.bind(listVector.getDataVector());
    }

    @Override
    public List<T> readNonNull(int row) {
      int start = listVector.getElementStartIndex(row);
      int end = listVector.getElementEndIndex(row);
      List<T> elements = Lists.newArrayListWithCapacity(end - start);
      for (int i = start; i < end; i++) {
        elements.add(elementReader.read(i));
      }
      return elements;
    }
  }

  private static class ConstantReader<C> implements VortexValueReader<C> {
    private final C constant;

    private ConstantReader(C constant) {
      this.constant = constant;
    }

    @Override
    public C read(int row) {
      return constant;
    }

    @Override
    public C readNonNull(int row) {
      return constant;
    }
  }

  private static class BooleanReader extends BoundVortexReader<Boolean> {
    private ArrowBuf data;

    @Override
    protected void bindVector(FieldVector vector) {
      this.data = ((BitVector) vector).getDataBuffer();
    }

    @Override
    public Boolean readNonNull(int row) {
      return (data.getByte(row >>> 3) & (1 << (row & 7))) != 0;
    }
  }

  private static class IntegerReader extends BoundVortexReader<Integer> {
    private BaseIntVector intVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.intVector = (BaseIntVector) vector;
    }

    @Override
    public Integer readNonNull(int row) {
      return (int) intVector.getValueAsLong(row);
    }
  }

  private static class LongReader extends BoundVortexReader<Long> {
    private BaseIntVector intVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.intVector = (BaseIntVector) vector;
    }

    @Override
    public Long readNonNull(int row) {
      return intVector.getValueAsLong(row);
    }
  }

  private static class DecimalReader extends BoundVortexReader<BigDecimal> {
    private DecimalVector decimalVector;
    private ArrowBuf data;
    private int scale;
    private boolean fitsInLong;

    @Override
    protected void bindVector(FieldVector vector) {
      this.decimalVector = (DecimalVector) vector;
      this.data = vector.getDataBuffer();
      this.scale = decimalVector.getScale();
      this.fitsInLong = decimalVector.getPrecision() <= MAX_LONG_PRECISION;
    }

    @Override
    public BigDecimal readNonNull(int row) {
      if (fitsInLong) {
        // See readBigDecimal: the low 8 little-endian bytes carry the full value.
        long unscaled = data.getLong((long) row * DecimalVector.TYPE_WIDTH);
        return BigDecimal.valueOf(unscaled, scale);
      }

      return decimalVector.getObjectNotNull(row);
    }
  }

  private static class FloatReader extends BoundVortexReader<Float> {
    private Float4Vector floatVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.floatVector = (Float4Vector) vector;
    }

    @Override
    public Float readNonNull(int row) {
      return floatVector.get(row);
    }
  }

  private static class DoubleReader extends BoundVortexReader<Double> {
    private Float8Vector doubleVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.doubleVector = (Float8Vector) vector;
    }

    @Override
    public Double readNonNull(int row) {
      return doubleVector.get(row);
    }
  }

  private static class FloatAsDoubleReader extends BoundVortexReader<Double> {
    private Float4Vector floatVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.floatVector = (Float4Vector) vector;
    }

    @Override
    public Double readNonNull(int row) {
      return (double) floatVector.get(row);
    }
  }

  private static class RowIdReader implements VortexValueReader<Long> {
    private final long firstRowId;
    private BaseIntVector valueVector;
    private ArrowBuf valueValidity;
    private BaseIntVector posVector;

    private RowIdReader(long firstRowId) {
      this.firstRowId = firstRowId;
    }

    @Override
    public void bind(FieldVector vector) {
      StructVector struct = (StructVector) vector;
      FieldVector value = (FieldVector) struct.getChild("value");
      this.valueVector = (BaseIntVector) value;
      this.valueValidity =
          value != null && value.getNullCount() > 0 ? value.getValidityBuffer() : null;
      this.posVector = (BaseIntVector) struct.getChild("pos");
    }

    @Override
    public Long read(int row) {
      // the packed struct itself is never null
      return readNonNull(row);
    }

    @Override
    public Long readNonNull(int row) {
      if (valueVector != null && !isValueNull(row)) {
        return valueVector.getValueAsLong(row);
      }

      return firstRowId + posVector.getValueAsLong(row);
    }

    private boolean isValueNull(int row) {
      return valueValidity != null && (valueValidity.getByte(row >>> 3) & (1 << (row & 7))) == 0;
    }
  }

  private static class LongOrDefaultReader implements VortexValueReader<Long> {
    private final long defaultValue;
    private BaseIntVector longVector;
    private ArrowBuf validity;

    private LongOrDefaultReader(long defaultValue) {
      this.defaultValue = defaultValue;
    }

    @Override
    public void bind(FieldVector vector) {
      this.longVector = (BaseIntVector) vector;
      this.validity = vector.getNullCount() > 0 ? vector.getValidityBuffer() : null;
    }

    @Override
    public Long read(int row) {
      if (longVector == null
          || (validity != null && (validity.getByte(row >>> 3) & (1 << (row & 7))) == 0)) {
        return defaultValue;
      }

      return readNonNull(row);
    }

    @Override
    public Long readNonNull(int row) {
      return longVector.getValueAsLong(row);
    }
  }

  private static class StringReader extends BoundVortexReader<String> {
    private VarCharVector stringVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.stringVector = (VarCharVector) vector;
    }

    @Override
    public String readNonNull(int row) {
      return new String(stringVector.get(row), StandardCharsets.UTF_8);
    }
  }

  private static class StringViewReader extends BoundVortexReader<String> {
    private ViewVarCharVector stringVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.stringVector = (ViewVarCharVector) vector;
    }

    @Override
    public String readNonNull(int row) {
      return new String(stringVector.get(row), StandardCharsets.UTF_8);
    }
  }

  private static class BytesReader extends BoundVortexReader<ByteBuffer> {
    private VarBinaryVector binaryVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.binaryVector = (VarBinaryVector) vector;
    }

    @Override
    public ByteBuffer readNonNull(int row) {
      return ByteBuffer.wrap(binaryVector.get(row));
    }
  }

  private static class BytesViewReader extends BoundVortexReader<ByteBuffer> {
    private ViewVarBinaryVector binaryVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.binaryVector = (ViewVarBinaryVector) vector;
    }

    @Override
    public ByteBuffer readNonNull(int row) {
      return ByteBuffer.wrap(binaryVector.get(row));
    }
  }

  /**
   * Reads a string vector's raw bytes when the vector type is only known at read time (shredded
   * variant values); column readers use the vector-specific readers selected at build time.
   */
  private static byte[] utf8Bytes(FieldVector vector, int row) {
    if (vector instanceof ViewVarCharVector view) {
      return view.get(row);
    }
    return ((VarCharVector) vector).get(row);
  }

  /** Binary counterpart of {@link #utf8Bytes} for dynamically-typed variant values. */
  private static byte[] binaryBytes(FieldVector vector, int row) {
    if (vector instanceof ViewVarBinaryVector view) {
      return view.get(row);
    }
    return ((VarBinaryVector) vector).get(row);
  }

  private static class UuidReader extends BoundVortexReader<UUID> {
    private FixedSizeBinaryVector storage;

    @Override
    protected void bindVector(FieldVector vector) {
      this.storage = uuidStorage(vector);
    }

    @Override
    public UUID readNonNull(int row) {
      return UUIDUtil.convert(storage.get(row));
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
    private FieldVector outerVector;
    private StructVector storage;

    private VariantReader() {}

    @Override
    public void bind(FieldVector vector) {
      this.outerVector = vector;
      this.storage = variantStorage(vector);
    }

    @Override
    public Variant read(int row) {
      if (outerVector.isNull(row)) {
        return null;
      }

      return readNonNull(row);
    }

    @Override
    public Variant readNonNull(int row) {
      FieldVector metadataVector = (FieldVector) storage.getChild("metadata");
      if (metadataVector == null || metadataVector.isNull(row)) {
        throw new IllegalStateException("Invalid Vortex variant: metadata is null");
      }

      byte[] metadataBytes = binaryBytes(metadataVector, row);
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
    FieldVector valueVector = (FieldVector) storage.getChild("value");
    if (valueVector == null || valueVector.isNull(row)) {
      return null;
    }

    byte[] valueBytes = binaryBytes(valueVector, row);
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

      return Variants.of(physicalType, readBigDecimal((DecimalVector) vector, row));
    } else if (type instanceof ArrowType.Date dateType) {
      int days =
          switch (dateType.getUnit()) {
            case DAY -> ((DateDayVector) vector).get(row);
            case MILLISECOND ->
                (int) Math.floorDiv(((DateMilliVector) vector).get(row), 86_400_000L);
          };
      return Variants.ofDate(days);
    } else if (type instanceof ArrowType.Time timeType) {
      long micros =
          switch (timeType.getUnit()) {
            case SECOND -> ((TimeSecVector) vector).get(row) * 1_000_000L;
            case MILLISECOND -> ((TimeMilliVector) vector).get(row) * 1_000L;
            case MICROSECOND -> ((TimeMicroVector) vector).get(row);
            case NANOSECOND -> ((TimeNanoVector) vector).get(row) / 1_000L;
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
    } else if (isUtf8Type(type)) {
      return Variants.of(new String(utf8Bytes(vector, row), StandardCharsets.UTF_8));
    } else if (VortexSchemas.isUuidField(vector.getField())) {
      return Variants.ofUUID(UUIDUtil.convert(uuidStorage(vector).get(row)));
    } else if (isBinaryType(type)) {
      return Variants.of(ByteBuffer.wrap(binaryBytes(vector, row)));
    } else if (type instanceof ArrowType.FixedSizeBinary) {
      return Variants.of(ByteBuffer.wrap(((FixedSizeBinaryVector) vector).get(row)));
    }

    throw new UnsupportedOperationException("Unsupported shredded variant type: " + type);
  }

  private static boolean isUtf8Type(ArrowType type) {
    return type instanceof ArrowType.Utf8
        || type instanceof ArrowType.LargeUtf8
        || type instanceof ArrowType.Utf8View;
  }

  private static boolean isBinaryType(ArrowType type) {
    return type instanceof ArrowType.Binary
        || type instanceof ArrowType.LargeBinary
        || type instanceof ArrowType.BinaryView;
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

  private static class DateReader extends BoundVortexReader<LocalDate> {
    private final boolean isMillis;
    private DateDayVector dayVector;
    private DateMilliVector milliVector;

    DateReader(boolean isMillis) {
      this.isMillis = isMillis;
    }

    @Override
    protected void bindVector(FieldVector vector) {
      if (isMillis) {
        this.milliVector = (DateMilliVector) vector;
      } else {
        this.dayVector = (DateDayVector) vector;
      }
    }

    @Override
    public LocalDate readNonNull(int row) {
      int days;
      if (isMillis) {
        days = (int) Math.floorDiv(milliVector.get(row), 86_400_000L);
      } else {
        days = dayVector.get(row);
      }

      return DateTimeUtil.dateFromDays(days);
    }
  }

  private static class TimestampReader extends BoundVortexReader<LocalDateTime> {
    private final boolean nanosecond;
    private TimeStampVector timestampVector;

    private TimestampReader(boolean nanosecond) {
      this.nanosecond = nanosecond;
    }

    @Override
    protected void bindVector(FieldVector vector) {
      this.timestampVector = (TimeStampVector) vector;
    }

    @Override
    public LocalDateTime readNonNull(int row) {
      long measure = timestampVector.get(row);
      if (nanosecond) {
        return DateTimeUtil.timestampFromNanos(measure);
      } else {
        return DateTimeUtil.timestampFromMicros(measure);
      }
    }
  }

  private static class TimeReader extends BoundVortexReader<LocalTime> {
    private final boolean nanosecond;
    private TimeNanoVector nanoVector;
    private TimeMicroVector microVector;

    private TimeReader(boolean nanosecond) {
      this.nanosecond = nanosecond;
    }

    @Override
    protected void bindVector(FieldVector vector) {
      if (nanosecond) {
        this.nanoVector = (TimeNanoVector) vector;
      } else {
        this.microVector = (TimeMicroVector) vector;
      }
    }

    @Override
    public LocalTime readNonNull(int row) {
      if (nanosecond) {
        return LocalTime.ofNanoOfDay(nanoVector.get(row));
      } else {
        return DateTimeUtil.timeFromMicros(microVector.get(row));
      }
    }
  }

  private static class TimestampTzReader extends BoundVortexReader<OffsetDateTime> {
    private final ZoneId timeZone;
    private final boolean nanosecond;
    private TimeStampVector timestampVector;

    private TimestampTzReader(String timeZone, boolean nanosecond) {
      this.timeZone = ZoneId.of(timeZone);
      this.nanosecond = nanosecond;
    }

    @Override
    protected void bindVector(FieldVector vector) {
      this.timestampVector = (TimeStampVector) vector;
    }

    @Override
    public OffsetDateTime readNonNull(int row) {
      long measure = timestampVector.get(row);
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
