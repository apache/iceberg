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
package org.apache.iceberg.arrow.vectorized;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.LargeListViewVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.RunEndEncodedVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

/**
 * This class is creates typed {@link ArrowVectorAccessor} from {@link VectorHolder}. It provides a
 * generic implementation for following Arrow types:
 *
 * <ul>
 *   <li>Decimal type can be deserialized to a type that supports decimal, e.g. BigDecimal or
 *       Spark's Decimal.
 *   <li>UTF8 String type can deserialized to a Java String or Spark's UTF8String.
 *   <li>List type: the child elements of a list can be deserialized to Spark's ColumnarArray or
 *       similar type.
 *   <li>Struct type: the child elements of a struct can be deserialized to a Spark's
 *       ArrowColumnVector or similar type.
 * </ul>
 *
 * @param <DecimalT> A concrete type that can represent a decimal.
 * @param <Utf8StringT> A concrete type that can represent a UTF8 string.
 * @param <ArrayT> A concrete type that can represent an array value in a list vector, e.g. Spark's
 *     ColumnarArray.
 * @param <ChildVectorT> A concrete type that can represent a child vector in a struct, e.g. Spark's
 *     ArrowColumnVector.
 */
public class GenericArrowVectorAccessorFactory<
    DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable> {

  private final Supplier<DecimalFactory<DecimalT>> decimalFactorySupplier;
  private final Supplier<StringFactory<Utf8StringT>> stringFactorySupplier;
  private final Supplier<StructChildFactory<ChildVectorT>> structChildFactorySupplier;
  private final Supplier<ArrayFactory<ChildVectorT, ArrayT>> arrayFactorySupplier;

  /**
   * The constructor is parameterized using the decimal, string, struct and array factories. If a
   * specific type is not supported, the factory supplier can raise an {@link
   * UnsupportedOperationException}.
   */
  protected GenericArrowVectorAccessorFactory(
      Supplier<DecimalFactory<DecimalT>> decimalFactorySupplier,
      Supplier<StringFactory<Utf8StringT>> stringFactorySupplier,
      Supplier<StructChildFactory<ChildVectorT>> structChildFactorySupplier,
      Supplier<ArrayFactory<ChildVectorT, ArrayT>> arrayFactorySupplier) {
    this.decimalFactorySupplier = decimalFactorySupplier;
    this.stringFactorySupplier = stringFactorySupplier;
    this.structChildFactorySupplier = structChildFactorySupplier;
    this.arrayFactorySupplier = arrayFactorySupplier;
  }

  public ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> getVectorAccessor(
      VectorHolder holder) {
    Dictionary dictionary = holder.dictionary();
    boolean isVectorDictEncoded = holder.isDictionaryEncoded();
    FieldVector vector = holder.vector();
    ColumnDescriptor desc = holder.descriptor();
    // desc could be null when the holder is ConstantVectorHolder/PositionVectorHolder
    PrimitiveType primitive = desc == null ? null : desc.getPrimitiveType();
    if (isVectorDictEncoded) {
      return getDictionaryVectorAccessor(dictionary, desc, vector, primitive);
    } else {
      return getPlainVectorAccessor(vector, primitive);
    }
  }

  private ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT>
      getDictionaryVectorAccessor(
          Dictionary dictionary,
          ColumnDescriptor desc,
          FieldVector vector,
          PrimitiveType primitive) {
    Preconditions.checkState(
        vector instanceof IntVector, "Dictionary ids should be stored in IntVectors only");
    // TODO: consider moving this to logical type annotations,
    // as new Parquet types are added only there.
    if (primitive.getOriginalType() != null) {
      return switch (desc.getPrimitiveType().getOriginalType()) {
        case ENUM, JSON, UTF8, BSON ->
            new DictionaryStringAccessor<>(
                (IntVector) vector, dictionary, stringFactorySupplier.get());
        case INT_64, TIME_MICROS, TIMESTAMP_MILLIS, TIMESTAMP_MICROS ->
            new DictionaryLongAccessor<>((IntVector) vector, dictionary);
        case DECIMAL ->
            switch (primitive.getPrimitiveTypeName()) {
              case BINARY, FIXED_LEN_BYTE_ARRAY ->
                  new DictionaryDecimalBinaryAccessor<>(
                      (IntVector) vector, dictionary, decimalFactorySupplier.get());
              case INT64 ->
                  new DictionaryDecimalLongAccessor<>(
                      (IntVector) vector, dictionary, decimalFactorySupplier.get());
              case INT32 ->
                  new DictionaryDecimalIntAccessor<>(
                      (IntVector) vector, dictionary, decimalFactorySupplier.get());
              default ->
                  throw new UnsupportedOperationException(
                      "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
            };
        default ->
            throw new UnsupportedOperationException(
                "Unsupported logical type: " + primitive.getOriginalType());
      };
    } else {
      return switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY, BINARY ->
            new DictionaryBinaryAccessor<>(
                (IntVector) vector, dictionary, stringFactorySupplier.get());
        case FLOAT -> new DictionaryFloatAccessor<>((IntVector) vector, dictionary);
        case INT64 -> new DictionaryLongAccessor<>((IntVector) vector, dictionary);
        case INT96 ->
            // Impala & Spark used to write timestamps as INT96 by default. For backwards
            // compatibility we try to read INT96 as timestamps. But INT96 is not recommended
            // and deprecated (see https://issues.apache.org/jira/browse/PARQUET-323)
            new DictionaryTimestampInt96Accessor<>((IntVector) vector, dictionary);
        case DOUBLE -> new DictionaryDoubleAccessor<>((IntVector) vector, dictionary);
        default -> throw new UnsupportedOperationException("Unsupported type: " + primitive);
      };
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> getPlainVectorAccessor(
      FieldVector vector, PrimitiveType primitive) {
    if (vector instanceof ExtensionTypeVector) {
      // e.g. the canonical Arrow UUID type, whose storage is a FixedSizeBinaryVector. Build an
      // accessor over the underlying storage vector.
      return getPlainVectorAccessor(
          (FieldVector) ((ExtensionTypeVector<?>) vector).getUnderlyingVector(), primitive);
    } else if (vector instanceof RunEndEncodedVector runEndEncoded) {
      return new RunEndEncodedAccessor<>(
          runEndEncoded, getPlainVectorAccessor(runEndEncoded.getValuesVector(), primitive));
    }
    if (vector instanceof BitVector) {
      return new BooleanAccessor<>((BitVector) vector);
    } else if (vector instanceof IntVector) {
      if (isDecimal(primitive)) {
        return new IntBackedDecimalAccessor<>((IntVector) vector, decimalFactorySupplier.get());
      }
      return new IntAccessor<>((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      if (isDecimal(primitive)) {
        return new LongBackedDecimalAccessor<>((BigIntVector) vector, decimalFactorySupplier.get());
      }
      return new LongAccessor<>((BigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      return new FloatAccessor<>((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      return new DoubleAccessor<>((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      return new DecimalAccessor<>((DecimalVector) vector, decimalFactorySupplier.get());
    } else if (vector instanceof VarCharVector) {
      return new StringAccessor<>((VarCharVector) vector, stringFactorySupplier.get());
    } else if (vector instanceof ViewVarCharVector) {
      return new StringViewAccessor<>((ViewVarCharVector) vector, stringFactorySupplier.get());
    } else if (vector instanceof VarBinaryVector) {
      return new BinaryAccessor<>((VarBinaryVector) vector);
    } else if (vector instanceof ViewVarBinaryVector) {
      return new BinaryViewAccessor<>((ViewVarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      return new DateAccessor<>((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      return new TimestampAccessor<>((TimeStampMicroTZVector) vector);
    } else if (vector instanceof TimeStampMicroVector) {
      return new TimestampAccessor<>((TimeStampMicroVector) vector);
    } else if (vector instanceof TimeStampNanoVector) {
      return new TimestampAccessor<>((TimeStampNanoVector) vector);
    } else if (vector instanceof TimeStampNanoTZVector) {
      return new TimestampAccessor<>((TimeStampNanoTZVector) vector);
    } else if (vector instanceof ListVector listVector) {
      return new ArrayAccessor<>(
          listVector, listVector.getDataVector(), arrayFactorySupplier.get());
    } else if (vector instanceof LargeListVector largeListVector) {
      return new LargeListArrayAccessor<>(
          largeListVector, largeListVector.getDataVector(), arrayFactorySupplier.get());
    } else if (vector instanceof ListViewVector listViewVector) {
      return new ArrayAccessor<>(
          listViewVector, listViewVector.getDataVector(), arrayFactorySupplier.get());
    } else if (vector instanceof LargeListViewVector largeListViewVector) {
      return new LargeListViewArrayAccessor<>(
          largeListViewVector, largeListViewVector.getDataVector(), arrayFactorySupplier.get());
    } else if (vector instanceof StructVector structVector) {
      return new StructAccessor<>(structVector, structChildFactorySupplier.get());
    } else if (vector instanceof TimeMicroVector) {
      return new TimeMicroAccessor<>((TimeMicroVector) vector);
    } else if (vector instanceof FixedSizeBinaryVector) {
      if (isDecimal(primitive)) {
        return new FixedSizeBinaryBackedDecimalAccessor<>(
            (FixedSizeBinaryVector) vector, decimalFactorySupplier.get());
      }
      return new FixedSizeBinaryAccessor<>(
          (FixedSizeBinaryVector) vector, stringFactorySupplier.get());
    }
    throw new UnsupportedOperationException("Unsupported vector: " + vector.getClass());
  }

  private static boolean isDecimal(PrimitiveType primitive) {
    return primitive != null
        && primitive.getLogicalTypeAnnotation()
            instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
  }

  private static class BooleanAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final BitVector vector;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final boolean getBoolean(int rowId) {
      return vector.get(rowId) == 1;
    }
  }

  private static class IntAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final IntVector vector;

    IntAccessor(IntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final int getInt(int rowId) {
      return vector.get(rowId);
    }

    @Override
    public final long getLong(int rowId) {
      return getInt(rowId);
    }
  }

  private static class LongAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final BigIntVector vector;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final long getLong(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class DictionaryLongAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final IntVector offsetVector;
    private final Dictionary dictionary;

    DictionaryLongAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.dictionary = dictionary;
    }

    @Override
    public final long getLong(int rowId) {
      return dictionary.decodeToLong(offsetVector.get(rowId));
    }
  }

  private static class FloatAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final Float4Vector vector;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final float getFloat(int rowId) {
      return vector.get(rowId);
    }

    @Override
    public final double getDouble(int rowId) {
      return getFloat(rowId);
    }
  }

  private static class DictionaryFloatAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final IntVector offsetVector;
    private final Dictionary dictionary;

    DictionaryFloatAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.dictionary = dictionary;
    }

    @Override
    public final float getFloat(int rowId) {
      return dictionary.decodeToFloat(offsetVector.get(rowId));
    }

    @Override
    public final double getDouble(int rowId) {
      return getFloat(rowId);
    }
  }

  private static class DoubleAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final Float8Vector vector;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final double getDouble(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class DictionaryDoubleAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final IntVector offsetVector;
    private final Dictionary dictionary;

    DictionaryDoubleAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.dictionary = dictionary;
    }

    @Override
    public final double getDouble(int rowId) {
      return dictionary.decodeToDouble(offsetVector.get(rowId));
    }
  }

  private static class StringAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final VarCharVector vector;
    private final StringFactory<Utf8StringT> stringFactory;

    StringAccessor(VarCharVector vector, StringFactory<Utf8StringT> stringFactory) {
      super(vector);
      this.vector = vector;
      this.stringFactory = stringFactory;
    }

    @Override
    public final Utf8StringT getUTF8String(int rowId) {
      return stringFactory.ofRow(vector, rowId);
    }
  }

  private static class StringViewAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final ViewVarCharVector vector;
    private final StringFactory<Utf8StringT> stringFactory;

    StringViewAccessor(ViewVarCharVector vector, StringFactory<Utf8StringT> stringFactory) {
      super(vector);
      this.vector = vector;
      this.stringFactory = stringFactory;
    }

    @Override
    public final Utf8StringT getUTF8String(int rowId) {
      return stringFactory.ofRow(vector, rowId);
    }
  }

  private static class BinaryViewAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final ViewVarBinaryVector vector;

    BinaryViewAccessor(ViewVarBinaryVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final byte[] getBinary(int rowId) {
      return vector.get(rowId);
    }
  }

  /**
   * Accessor for run-end encoded vectors. Each logical row is resolved to its physical position in
   * the run-end encoded values vector, and the read is delegated to an accessor over those values.
   */
  private static class RunEndEncodedAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final RunEndEncodedVector vector;
    private final ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> values;

    RunEndEncodedAccessor(
        RunEndEncodedVector vector,
        ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> values) {
      super(vector);
      this.vector = vector;
      this.values = values;
    }

    @Override
    public boolean getBoolean(int rowId) {
      return values.getBoolean(vector.getPhysicalIndex(rowId));
    }

    @Override
    public int getInt(int rowId) {
      return values.getInt(vector.getPhysicalIndex(rowId));
    }

    @Override
    public long getLong(int rowId) {
      return values.getLong(vector.getPhysicalIndex(rowId));
    }

    @Override
    public float getFloat(int rowId) {
      return values.getFloat(vector.getPhysicalIndex(rowId));
    }

    @Override
    public double getDouble(int rowId) {
      return values.getDouble(vector.getPhysicalIndex(rowId));
    }

    @Override
    public byte[] getBinary(int rowId) {
      return values.getBinary(vector.getPhysicalIndex(rowId));
    }

    @Override
    public DecimalT getDecimal(int rowId, int precision, int scale) {
      return values.getDecimal(vector.getPhysicalIndex(rowId), precision, scale);
    }

    @Override
    public Utf8StringT getUTF8String(int rowId) {
      return values.getUTF8String(vector.getPhysicalIndex(rowId));
    }

    @Override
    public ArrayT getArray(int rowId) {
      return values.getArray(vector.getPhysicalIndex(rowId));
    }
  }

  private static class DictionaryStringAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final Dictionary dictionary;
    private final StringFactory<Utf8StringT> stringFactory;
    private final IntVector offsetVector;
    private final Utf8StringT[] cache;

    DictionaryStringAccessor(
        IntVector vector, Dictionary dictionary, StringFactory<Utf8StringT> stringFactory) {
      super(vector);
      this.offsetVector = vector;
      this.dictionary = dictionary;
      this.stringFactory = stringFactory;
      this.cache = genericArray(stringFactory.getGenericClass(), dictionary.getMaxId() + 1);
    }

    @Override
    public final Utf8StringT getUTF8String(int rowId) {
      int offset = offsetVector.get(rowId);
      if (cache[offset] == null) {
        cache[offset] =
            stringFactory.ofByteBuffer(dictionary.decodeToBinary(offset).toByteBuffer());
      }
      return cache[offset];
    }
  }

  private static class BinaryAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final VarBinaryVector vector;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final byte[] getBinary(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class DictionaryBinaryAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final IntVector offsetVector;
    private final Dictionary dictionary;
    private final StringFactory<Utf8StringT> stringFactory;

    DictionaryBinaryAccessor(
        IntVector vector, Dictionary dictionary, StringFactory<Utf8StringT> stringFactory) {
      super(vector);
      this.offsetVector = vector;
      this.dictionary = dictionary;
      this.stringFactory = stringFactory;
    }

    @Override
    public final byte[] getBinary(int rowId) {
      return dictionary.decodeToBinary(offsetVector.get(rowId)).getBytes();
    }

    @Override
    public Utf8StringT getUTF8String(int rowId) {
      return null == stringFactory
          ? super.getUTF8String(rowId)
          : stringFactory.ofRow(offsetVector, dictionary, rowId);
    }
  }

  private static class DictionaryTimestampInt96Accessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final IntVector offsetVector;
    private final Dictionary dictionary;

    DictionaryTimestampInt96Accessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.dictionary = dictionary;
    }

    @Override
    public final long getLong(int rowId) {
      ByteBuffer byteBuffer =
          dictionary
              .decodeToBinary(offsetVector.get(rowId))
              .toByteBuffer()
              .order(ByteOrder.LITTLE_ENDIAN);
      return ParquetUtil.extractTimestampInt96(byteBuffer);
    }
  }

  private static class DateAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final DateDayVector vector;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final int getInt(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class TimestampAccessor<
          DecimalT,
          Utf8StringT,
          ArrayT,
          TimestampVectorT extends TimeStampVector,
          ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final TimestampVectorT vector;

    TimestampAccessor(TimestampVectorT vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final long getLong(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class TimeMicroAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final TimeMicroVector vector;

    TimeMicroAccessor(TimeMicroVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    public final long getLong(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class FixedSizeBinaryAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final FixedSizeBinaryVector vector;
    private final StringFactory<Utf8StringT> stringFactory;

    FixedSizeBinaryAccessor(
        FixedSizeBinaryVector vector, StringFactory<Utf8StringT> stringFactory) {
      super(vector);
      this.vector = vector;
      this.stringFactory = stringFactory;
    }

    @Override
    public byte[] getBinary(int rowId) {
      return vector.get(rowId);
    }

    @Override
    public Utf8StringT getUTF8String(int rowId) {
      return null == stringFactory
          ? super.getUTF8String(rowId)
          : stringFactory.ofRow(vector, rowId);
    }
  }

  /**
   * Accessor for {@link ListVector} and {@link ListViewVector}, both of which expose element ranges
   * through the {@link BaseListVector} interface.
   */
  private static class ArrayAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final BaseListVector vector;
    private final ChildVectorT arrayData;
    private final ArrayFactory<ChildVectorT, ArrayT> arrayFactory;

    ArrayAccessor(
        BaseListVector vector,
        FieldVector dataVector,
        ArrayFactory<ChildVectorT, ArrayT> arrayFactory) {
      super(vector);
      this.vector = vector;
      this.arrayFactory = arrayFactory;
      this.arrayData = arrayFactory.ofChild(dataVector);
    }

    @Override
    public final ArrayT getArray(int rowId) {
      int start = vector.getElementStartIndex(rowId);
      int end = vector.getElementEndIndex(rowId);
      return arrayFactory.ofRow(arrayData, start, end - start);
    }
  }

  /**
   * Accessor for {@link LargeListViewVector}, which uses 64-bit offset and size buffers and does
   * not implement {@link BaseListVector}, so element ranges are read from its buffers directly.
   */
  private static class LargeListViewArrayAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final LargeListViewVector vector;
    private final ChildVectorT arrayData;
    private final ArrayFactory<ChildVectorT, ArrayT> arrayFactory;

    LargeListViewArrayAccessor(
        LargeListViewVector vector,
        FieldVector dataVector,
        ArrayFactory<ChildVectorT, ArrayT> arrayFactory) {
      super(vector);
      this.vector = vector;
      this.arrayFactory = arrayFactory;
      this.arrayData = arrayFactory.ofChild(dataVector);
    }

    @Override
    public final ArrayT getArray(int rowId) {
      long start =
          vector.getOffsetBuffer().getLong((long) rowId * LargeListViewVector.OFFSET_WIDTH);
      long size = vector.getSizeBuffer().getLong((long) rowId * LargeListViewVector.SIZE_WIDTH);
      return arrayFactory.ofRow(arrayData, (int) start, (int) size);
    }
  }

  /**
   * Accessor for {@link LargeListVector}, which uses 64-bit cumulative offsets and does not
   * implement {@link BaseListVector}, but exposes element ranges through its own {@code long}
   * accessors.
   */
  private static class LargeListArrayAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final LargeListVector vector;
    private final ChildVectorT arrayData;
    private final ArrayFactory<ChildVectorT, ArrayT> arrayFactory;

    LargeListArrayAccessor(
        LargeListVector vector,
        FieldVector dataVector,
        ArrayFactory<ChildVectorT, ArrayT> arrayFactory) {
      super(vector);
      this.vector = vector;
      this.arrayFactory = arrayFactory;
      this.arrayData = arrayFactory.ofChild(dataVector);
    }

    @Override
    public final ArrayT getArray(int rowId) {
      long start = vector.getElementStartIndex(rowId);
      long end = vector.getElementEndIndex(rowId);
      return arrayFactory.ofRow(arrayData, (int) start, (int) (end - start));
    }
  }

  private static class StructAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    StructAccessor(StructVector structVector, StructChildFactory<ChildVectorT> structChildFactory) {
      super(
          structVector,
          IntStream.range(0, structVector.size())
              .mapToObj(structVector::getVectorById)
              .map(structChildFactory::of)
              .toArray(genericArray(structChildFactory.getGenericClass())));
    }
  }

  private static class DecimalAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final DecimalVector vector;
    private final DecimalFactory<DecimalT> decimalFactory;

    DecimalAccessor(DecimalVector vector, DecimalFactory<DecimalT> decimalFactory) {
      super(vector);
      this.vector = vector;
      this.decimalFactory = decimalFactory;
    }

    @Override
    public final DecimalT getDecimal(int rowId, int precision, int scale) {
      return decimalFactory.ofBigDecimal(
          DecimalUtility.getBigDecimalFromArrowBuf(
              vector.getDataBuffer(), rowId, scale, DecimalVector.TYPE_WIDTH),
          precision,
          scale);
    }
  }

  private static class IntBackedDecimalAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final IntVector vector;
    private final DecimalFactory<DecimalT> decimalFactory;

    IntBackedDecimalAccessor(IntVector vector, DecimalFactory<DecimalT> decimalFactory) {
      super(vector);
      this.vector = vector;
      this.decimalFactory = decimalFactory;
    }

    @Override
    public final DecimalT getDecimal(int rowId, int precision, int scale) {
      return decimalFactory.ofLong(vector.get(rowId), precision, scale);
    }
  }

  private static class LongBackedDecimalAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final BigIntVector vector;
    private final DecimalFactory<DecimalT> decimalFactory;

    LongBackedDecimalAccessor(BigIntVector vector, DecimalFactory<DecimalT> decimalFactory) {
      super(vector);
      this.vector = vector;
      this.decimalFactory = decimalFactory;
    }

    @Override
    public final DecimalT getDecimal(int rowId, int precision, int scale) {
      return decimalFactory.ofLong(vector.get(rowId), precision, scale);
    }
  }

  private static class FixedSizeBinaryBackedDecimalAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    private final FixedSizeBinaryVector vector;
    private final DecimalFactory<DecimalT> decimalFactory;

    FixedSizeBinaryBackedDecimalAccessor(
        FixedSizeBinaryVector vector, DecimalFactory<DecimalT> decimalFactory) {
      super(vector);
      this.vector = vector;
      this.decimalFactory = decimalFactory;
    }

    @Override
    public final DecimalT getDecimal(int rowId, int precision, int scale) {
      byte[] bytes = vector.get(rowId);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return decimalFactory.ofBigDecimal(javaDecimal, precision, scale);
    }
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  private abstract static class DictionaryDecimalAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {
    private final DecimalT[] cache;
    private final IntVector offsetVector;
    protected final DecimalFactory<DecimalT> decimalFactory;
    protected final Dictionary parquetDictionary;

    private DictionaryDecimalAccessor(
        IntVector vector, Dictionary dictionary, DecimalFactory<DecimalT> decimalFactory) {
      super(vector);
      this.offsetVector = vector;
      this.parquetDictionary = dictionary;
      this.decimalFactory = decimalFactory;
      this.cache = genericArray(decimalFactory.getGenericClass(), dictionary.getMaxId() + 1);
    }

    @Override
    public final DecimalT getDecimal(int rowId, int precision, int scale) {
      int offset = offsetVector.get(rowId);
      if (cache[offset] == null) {
        cache[offset] = decode(offset, precision, scale);
      }
      return cache[offset];
    }

    protected abstract DecimalT decode(int dictId, int precision, int scale);
  }

  private static class DictionaryDecimalBinaryAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends DictionaryDecimalAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    DictionaryDecimalBinaryAccessor(
        IntVector vector, Dictionary dictionary, DecimalFactory<DecimalT> decimalFactory) {
      super(vector, dictionary, decimalFactory);
    }

    @Override
    protected DecimalT decode(int dictId, int precision, int scale) {
      ByteBuffer byteBuffer = parquetDictionary.decodeToBinary(dictId).toByteBuffer();
      BigDecimal value =
          DecimalUtility.getBigDecimalFromByteBuffer(byteBuffer, scale, byteBuffer.remaining());
      return decimalFactory.ofBigDecimal(value, precision, scale);
    }
  }

  private static class DictionaryDecimalLongAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends DictionaryDecimalAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    DictionaryDecimalLongAccessor(
        IntVector vector, Dictionary dictionary, DecimalFactory<DecimalT> decimalFactory) {
      super(vector, dictionary, decimalFactory);
    }

    @Override
    protected DecimalT decode(int dictId, int precision, int scale) {
      return decimalFactory.ofLong(parquetDictionary.decodeToLong(dictId), precision, scale);
    }
  }

  private static class DictionaryDecimalIntAccessor<
          DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
      extends DictionaryDecimalAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT> {

    DictionaryDecimalIntAccessor(
        IntVector vector, Dictionary dictionary, DecimalFactory<DecimalT> decimalFactory) {
      super(vector, dictionary, decimalFactory);
    }

    @Override
    protected DecimalT decode(int dictId, int precision, int scale) {
      return decimalFactory.ofLong(parquetDictionary.decodeToInt(dictId), precision, scale);
    }
  }

  /**
   * Create a decimal value of type {@code DecimalT} from arrow vector value.
   *
   * @param <DecimalT> A concrete type that can represent a decimal, e.g, Spark's Decimal.
   */
  protected interface DecimalFactory<DecimalT> {
    /** Class of concrete decimal type. */
    Class<DecimalT> getGenericClass();

    /** Create a decimal from the given long value, precision and scale. */
    DecimalT ofLong(long value, int precision, int scale);

    /** Create a decimal from the given {@link BigDecimal} value, precision and scale. */
    DecimalT ofBigDecimal(BigDecimal value, int precision, int scale);
  }

  /**
   * Create a UTF8 String value of type {@code Utf8StringT} from arrow vector value.
   *
   * @param <Utf8StringT> A concrete type that can represent a UTF8 string.
   */
  protected interface StringFactory<Utf8StringT> {
    /** Class of concrete UTF8 String type. */
    Class<Utf8StringT> getGenericClass();

    /** Create a UTF8 String from the row value in the arrow vector. */
    Utf8StringT ofRow(VarCharVector vector, int rowId);

    /** Create a UTF8 String from the row value in the ViewVarCharVector vector. */
    Utf8StringT ofRow(ViewVarCharVector vector, int rowId);

    /** Create a UTF8 String from the row value in the FixedSizeBinaryVector vector. */
    default Utf8StringT ofRow(FixedSizeBinaryVector vector, int rowId) {
      throw new UnsupportedOperationException(
          String.format(
              "Creating %s from a FixedSizeBinaryVector is not supported",
              getGenericClass().getSimpleName()));
    }

    /** Create a UTF8 String from the row value in the Dictionary. */
    default Utf8StringT ofRow(IntVector offsetVector, Dictionary dictionary, int rowId) {
      throw new UnsupportedOperationException(
          String.format(
              "Creating %s from a Dictionary is not supported", getGenericClass().getSimpleName()));
    }

    /** Create a UTF8 String from the byte array. */
    Utf8StringT ofBytes(byte[] bytes);

    /** Create a UTF8 String from the byte buffer. */
    Utf8StringT ofByteBuffer(ByteBuffer byteBuffer);
  }

  /**
   * Create an array value of type {@code ArrayT} from arrow vector value.
   *
   * @param <ArrayT> A concrete type that can represent an array value in a list vector, e.g.
   *     Spark's ColumnarArray.
   * @param <ChildVectorT> A concrete type that can represent a child vector in a struct, e.g.
   *     Spark's ArrowColumnVector.
   */
  protected interface ArrayFactory<ChildVectorT, ArrayT> {
    /** Create a child vector of type {@code ChildVectorT} from the arrow child vector. */
    ChildVectorT ofChild(ValueVector childVector);

    /**
     * Create an array of type {@code ArrayT} from a contiguous range of the child vector. The
     * caller resolves the range (the start position of the first element and the number of
     * elements) from the list or list-view vector's offsets, so this method is independent of the
     * list encoding.
     */
    ArrayT ofRow(ChildVectorT childData, int elementStart, int numElements);
  }

  /**
   * Create a struct child vector of type {@code ChildVectorT} from arrow vector value.
   *
   * @param <ChildVectorT> A concrete type that can represent a child vector in a struct, e.g.
   *     Spark's ArrowColumnVector.
   */
  protected interface StructChildFactory<ChildVectorT> {
    /** Class of concrete child vector type. */
    Class<ChildVectorT> getGenericClass();

    /**
     * Create the child vector of type such as Spark's ArrowColumnVector from the arrow child
     * vector.
     */
    ChildVectorT of(ValueVector childVector);
  }

  private static <T> IntFunction<T[]> genericArray(Class<T> genericClass) {
    return length -> genericArray(genericClass, length);
  }

  @SuppressWarnings("unchecked")
  private static <T> T[] genericArray(Class<T> genericClass, int length) {
    return (T[]) Array.newInstance(genericClass, length);
  }
}
