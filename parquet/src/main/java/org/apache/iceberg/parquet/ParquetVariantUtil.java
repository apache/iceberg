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
package org.apache.iceberg.parquet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.iceberg.expressions.PathUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.iceberg.util.UnicodeUtil;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantPrimitive;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.VariantVisitor;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

class ParquetVariantUtil {
  private ParquetVariantUtil() {}

  /**
   * Convert a Parquet {@link PrimitiveType} to the equivalent Variant {@link PhysicalType}.
   *
   * @param primitive a Parquet {@link PrimitiveType}
   * @return a Variant {@link PhysicalType}
   * @throws UnsupportedOperationException if the Parquet type is not equivalent to a variant type
   */
  static PhysicalType convert(PrimitiveType primitive) {
    LogicalTypeAnnotation annotation = primitive.getLogicalTypeAnnotation();
    if (annotation != null) {
      return annotation.accept(PhysicalTypeConverter.INSTANCE).orElse(null);
    }

    switch (primitive.getPrimitiveTypeName()) {
      case BOOLEAN:
        return PhysicalType.BOOLEAN_TRUE;
      case INT32:
        return PhysicalType.INT32;
      case INT64:
        return PhysicalType.INT64;
      case FLOAT:
        return PhysicalType.FLOAT;
      case DOUBLE:
        return PhysicalType.DOUBLE;
      case BINARY:
        return PhysicalType.BINARY;
      default:
        return null;
    }
  }

  /**
   * Convert a Parquet {@link TimestampLogicalTypeAnnotation} to the equivalent Variant {@link
   * PhysicalType}.
   *
   * @param timestamp a Parquet {@link TimestampLogicalTypeAnnotation}
   * @return a Variant {@link PhysicalType}
   * @throws UnsupportedOperationException if the timestamp unit is not MICROS or NANOS
   */
  static PhysicalType convert(TimestampLogicalTypeAnnotation timestamp) {
    switch (timestamp.getUnit()) {
      case MICROS:
        return timestamp.isAdjustedToUTC() ? PhysicalType.TIMESTAMPTZ : PhysicalType.TIMESTAMPNTZ;
      case NANOS:
        return timestamp.isAdjustedToUTC()
            ? PhysicalType.TIMESTAMPTZ_NANOS
            : PhysicalType.TIMESTAMPNTZ_NANOS;
      default:
        throw new UnsupportedOperationException(
            "Invalid unit for shredded timestamp: " + timestamp.getUnit());
    }
  }

  /**
   * Serialize Variant metadata and value in a single concatenated buffer.
   *
   * @param metadata a {VariantMetadata}
   * @param value a {VariantValue}
   * @return a buffer containing the metadata and value serialized and concatenated
   */
  static ByteBuffer toByteBuffer(VariantMetadata metadata, VariantValue value) {
    ByteBuffer buffer =
        ByteBuffer.allocate(metadata.sizeInBytes() + value.sizeInBytes())
            .order(ByteOrder.LITTLE_ENDIAN);
    metadata.writeTo(buffer, 0);
    value.writeTo(buffer, metadata.sizeInBytes());
    return buffer;
  }

  /**
   * Converts a Parquet primitive value to a Variant in-memory value for the given {@link
   * PhysicalType type}.
   *
   * @param primitive a primitive variant {@link PhysicalType}
   * @param scale decimal scale used when the value is a decimal
   * @param value a value from Parquet value to convert
   * @param <T> Java type used for values of the given Variant physical type
   * @return the primitive value
   */
  @SuppressWarnings("unchecked")
  static <T> T convertValue(PhysicalType primitive, int scale, Object value) {
    switch (primitive) {
      case BOOLEAN_FALSE:
      case BOOLEAN_TRUE:
      case INT32:
      case INT64:
      case DATE:
      case TIMESTAMPTZ:
      case TIMESTAMPNTZ:
      case TIMESTAMPTZ_NANOS:
      case TIMESTAMPNTZ_NANOS:
      case TIME:
      case FLOAT:
      case DOUBLE:
        return (T) value;
      case INT8:
        return (T) (Byte) ((Number) value).byteValue();
      case INT16:
        return (T) (Short) ((Number) value).shortValue();
      case DECIMAL4:
      case DECIMAL8:
        return (T) BigDecimal.valueOf(((Number) value).longValue(), scale);
      case DECIMAL16:
        return (T) new BigDecimal(new BigInteger(((Binary) value).getBytes()), scale);
      case BINARY:
        return (T) ((Binary) value).toByteBuffer();
      case STRING:
        return (T) ((Binary) value).toStringUsingUTF8();
      case UUID:
        return (T) UUIDUtil.convert(((Binary) value).getBytes());
      default:
        throw new IllegalStateException("Invalid bound type: " + primitive);
    }
  }

  /**
   * Returns a comparator for values of the given primitive {@link PhysicalType type}.
   *
   * @param primitive a primitive variant {@link PhysicalType}
   * @param <T> Java type used for values of the given Variant physical type
   * @return a comparator for in-memory values of the given type
   */
  @SuppressWarnings("unchecked")
  static <T> Comparator<T> comparator(PhysicalType primitive) {
    if (primitive == PhysicalType.BINARY) {
      return (Comparator<T>) Comparators.unsignedBytes();
    } else {
      return (Comparator<T>) Comparator.naturalOrder();
    }
  }

  /**
   * Returns a decimal scale if the primitive is a decimal, or 0 otherwise.
   *
   * @param primitive a primitive type
   * @return decimal scale if the primitive is a decimal, 0 otherwise
   */
  static int scale(PrimitiveType primitive) {
    LogicalTypeAnnotation annotation = primitive.getLogicalTypeAnnotation();
    if (annotation instanceof DecimalLogicalTypeAnnotation) {
      return ((DecimalLogicalTypeAnnotation) annotation).getScale();
    }

    return 0;
  }

  /**
   * Creates a Parquet schema to fully shred the {@link VariantValue}.
   *
   * @param value a variant value
   * @return a Parquet schema that can fully shred the value
   */
  static Type toParquetSchema(VariantValue value) {
    return VariantVisitor.visit(value, new ParquetSchemaProducer());
  }

  private static class PhysicalTypeConverter implements LogicalTypeAnnotationVisitor<PhysicalType> {
    private static final PhysicalTypeConverter INSTANCE = new PhysicalTypeConverter();

    @Override
    public Optional<PhysicalType> visit(StringLogicalTypeAnnotation ignored) {
      return Optional.of(PhysicalType.STRING);
    }

    @Override
    public Optional<PhysicalType> visit(DecimalLogicalTypeAnnotation decimal) {
      if (decimal.getPrecision() <= 9) {
        return Optional.of(PhysicalType.DECIMAL4);
      } else if (decimal.getPrecision() <= 18) {
        return Optional.of(PhysicalType.DECIMAL8);
      } else {
        return Optional.of(PhysicalType.DECIMAL16);
      }
    }

    @Override
    public Optional<PhysicalType> visit(DateLogicalTypeAnnotation ignored) {
      return Optional.of(PhysicalType.DATE);
    }

    @Override
    public Optional<PhysicalType> visit(TimeLogicalTypeAnnotation ignored) {
      return Optional.of(PhysicalType.TIME);
    }

    @Override
    public Optional<PhysicalType> visit(TimestampLogicalTypeAnnotation timestamps) {
      switch (timestamps.getUnit()) {
        case MICROS:
          if (timestamps.isAdjustedToUTC()) {
            return Optional.of(PhysicalType.TIMESTAMPTZ);
          } else {
            return Optional.of(PhysicalType.TIMESTAMPNTZ);
          }
        case NANOS:
          if (timestamps.isAdjustedToUTC()) {
            return Optional.of(PhysicalType.TIMESTAMPTZ_NANOS);
          } else {
            return Optional.of(PhysicalType.TIMESTAMPNTZ_NANOS);
          }
        default:
          return Optional.empty();
      }
    }

    @Override
    public Optional<PhysicalType> visit(IntLogicalTypeAnnotation ints) {
      if (!ints.isSigned()) {
        return Optional.empty();
      }

      switch (ints.getBitWidth()) {
        case 8:
          return Optional.of(PhysicalType.INT8);
        case 16:
          return Optional.of(PhysicalType.INT16);
        case 32:
          return Optional.of(PhysicalType.INT32);
        case 64:
          return Optional.of(PhysicalType.INT64);
        default:
          return Optional.empty();
      }
    }

    @Override
    public Optional<PhysicalType> visit(UUIDLogicalTypeAnnotation uuidLogicalType) {
      return Optional.of(PhysicalType.UUID);
    }
  }

  static class VariantMetrics {
    private final long valueCount;
    private final long nullCount;
    private final VariantValue lowerBound;
    private final VariantValue upperBound;
    private final Deque<String> fieldNames = Lists.newLinkedList();
    private String lazyFieldName = null;

    VariantMetrics(long valueCount, long nullCount) {
      this.valueCount = valueCount;
      this.nullCount = nullCount;
      this.lowerBound = null;
      this.upperBound = null;
    }

    VariantMetrics(
        long valueCount, long nullCount, VariantValue lowerBound, VariantValue upperBound) {
      this.valueCount = valueCount;
      this.nullCount = nullCount;
      this.lowerBound = truncateLowerBound(lowerBound);
      this.upperBound = truncateUpperBound(upperBound);
    }

    VariantMetrics prependFieldName(String name) {
      this.lazyFieldName = null;
      fieldNames.addFirst(name);
      return this;
    }

    public String fieldName() {
      if (null == lazyFieldName) {
        this.lazyFieldName = PathUtil.toNormalizedPath(fieldNames);
      }

      return lazyFieldName;
    }

    public long valueCount() {
      return valueCount;
    }

    public long nullCount() {
      return nullCount;
    }

    public VariantValue lowerBound() {
      return lowerBound;
    }

    public VariantValue upperBound() {
      return upperBound;
    }

    private static VariantValue truncateLowerBound(VariantValue value) {
      switch (value.type()) {
        case STRING:
          return Variants.of(
              PhysicalType.STRING,
              UnicodeUtil.truncateStringMin((String) value.asPrimitive().get(), 16));
        case BINARY:
          return Variants.of(
              PhysicalType.BINARY,
              BinaryUtil.truncateBinaryMin((ByteBuffer) value.asPrimitive().get(), 16));
        default:
          return value;
      }
    }

    private static VariantValue truncateUpperBound(VariantValue value) {
      switch (value.type()) {
        case STRING:
          String truncatedString =
              UnicodeUtil.truncateStringMax((String) value.asPrimitive().get(), 16);
          return truncatedString != null ? Variants.of(PhysicalType.STRING, truncatedString) : null;
        case BINARY:
          ByteBuffer truncatedBuffer =
              BinaryUtil.truncateBinaryMin((ByteBuffer) value.asPrimitive().get(), 16);
          return truncatedBuffer != null ? Variants.of(PhysicalType.BINARY, truncatedBuffer) : null;
        default:
          return value;
      }
    }
  }

  private static class ParquetSchemaProducer extends VariantVisitor<Type> {
    @Override
    public Type object(VariantObject object, List<String> names, List<Type> typedValues) {
      if (object.numFields() < 1) {
        // Parquet cannot write  typed_value group with no fields
        return null;
      }

      List<GroupType> fields = Lists.newArrayList();
      int index = 0;
      for (String name : names) {
        Type typedValue = typedValues.get(index);
        fields.add(field(name, typedValue));
        index += 1;
      }

      return objectFields(fields);
    }

    @Override
    public Type array(VariantArray array, List<Type> elementResults) {
      if (elementResults.isEmpty()) {
        return null;
      }

      // Shred if all the elements are of a uniform type and build 3-level list
      Type shredType = elementResults.get(0);
      if (shredType != null
          && elementResults.stream().allMatch(type -> Objects.equals(type, shredType))) {
        return list(shredType);
      }

      return null;
    }

    private static GroupType list(Type shreddedType) {
      GroupType elementType = field("element", shreddedType);
      checkField(elementType);

      return Types.optionalList().element(elementType).named("typed_value");
    }

    @Override
    public Type primitive(VariantPrimitive<?> primitive) {
      switch (primitive.type()) {
        case NULL:
          return null;
        case BOOLEAN_TRUE:
        case BOOLEAN_FALSE:
          return shreddedPrimitive(PrimitiveType.PrimitiveTypeName.BOOLEAN);
        case INT8:
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8));
        case INT16:
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16));
        case INT32:
          return shreddedPrimitive(PrimitiveType.PrimitiveTypeName.INT32);
        case INT64:
          return shreddedPrimitive(PrimitiveType.PrimitiveTypeName.INT64);
        case FLOAT:
          return shreddedPrimitive(PrimitiveType.PrimitiveTypeName.FLOAT);
        case DOUBLE:
          return shreddedPrimitive(PrimitiveType.PrimitiveTypeName.DOUBLE);
        case DECIMAL4:
          BigDecimal decimal4 = (BigDecimal) primitive.get();
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.INT32,
              LogicalTypeAnnotation.decimalType(decimal4.scale(), 9));
        case DECIMAL8:
          BigDecimal decimal8 = (BigDecimal) primitive.get();
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.INT64,
              LogicalTypeAnnotation.decimalType(decimal8.scale(), 18));
        case DECIMAL16:
          BigDecimal decimal16 = (BigDecimal) primitive.get();
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.BINARY,
              LogicalTypeAnnotation.decimalType(decimal16.scale(), 38));
        case DATE:
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType());
        case TIMESTAMPTZ:
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.INT64,
              LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS));
        case TIMESTAMPNTZ:
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.INT64,
              LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS));
        case BINARY:
          return shreddedPrimitive(PrimitiveType.PrimitiveTypeName.BINARY);
        case STRING:
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
        case TIME:
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.INT64,
              LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS));
        case TIMESTAMPTZ_NANOS:
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.INT64,
              LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS));
        case TIMESTAMPNTZ_NANOS:
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.INT64,
              LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS));
        case UUID:
          return shreddedPrimitive(
              PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
              LogicalTypeAnnotation.uuidType(),
              16);
      }

      throw new UnsupportedOperationException("Unsupported shredding type: " + primitive.type());
    }

    private static GroupType objectFields(List<GroupType> fields) {
      Types.GroupBuilder<GroupType> builder = Types.buildGroup(Type.Repetition.OPTIONAL);
      for (GroupType field : fields) {
        checkField(field);
        builder.addField(field);
      }

      return builder.named("typed_value");
    }

    private static void checkField(GroupType fieldType) {
      Preconditions.checkArgument(
          fieldType.isRepetition(Type.Repetition.REQUIRED),
          "Invalid field type repetition: %s should be REQUIRED",
          fieldType.getRepetition());
    }

    private static GroupType field(String name, Type shreddedType) {
      Types.GroupBuilder<GroupType> builder =
          Types.buildGroup(Type.Repetition.REQUIRED)
              .optional(PrimitiveType.PrimitiveTypeName.BINARY)
              .named("value");

      if (shreddedType != null) {
        checkShreddedType(shreddedType);
        builder.addField(shreddedType);
      }

      return builder.named(name);
    }

    private static void checkShreddedType(Type shreddedType) {
      Preconditions.checkArgument(
          shreddedType.getName().equals("typed_value"),
          "Invalid shredded type name: %s should be typed_value",
          shreddedType.getName());
      Preconditions.checkArgument(
          shreddedType.isRepetition(Type.Repetition.OPTIONAL),
          "Invalid shredded type repetition: %s should be OPTIONAL",
          shreddedType.getRepetition());
    }

    private static Type shreddedPrimitive(PrimitiveType.PrimitiveTypeName primitive) {
      return Types.optional(primitive).named("typed_value");
    }

    private static Type shreddedPrimitive(
        PrimitiveType.PrimitiveTypeName primitive, LogicalTypeAnnotation annotation) {
      return Types.optional(primitive).as(annotation).named("typed_value");
    }

    private static Type shreddedPrimitive(
        PrimitiveType.PrimitiveTypeName primitive, LogicalTypeAnnotation annotation, int length) {
      return Types.optional(primitive).as(annotation).length(length).named("typed_value");
    }
  }
}
