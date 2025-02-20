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

import java.util.Map;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.arrow.ArrowAllocation;
import org.apache.iceberg.arrow.ArrowSchemaUtil;
import org.apache.iceberg.arrow.vectorized.parquet.VectorizedColumnIterator;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

/**
 * {@link VectorizedReader VectorReader(s)} that read in a batch of values into Arrow vectors. It
 * also takes care of allocating the right kind of Arrow vectors depending on the corresponding
 * Iceberg/Parquet data types.
 */
public class VectorizedArrowReader implements VectorizedReader<VectorHolder> {
  public static final int DEFAULT_BATCH_SIZE = 5000;
  private static final Integer UNKNOWN_WIDTH = null;
  private static final int AVERAGE_VARIABLE_WIDTH_RECORD_SIZE = 10;

  private final ColumnDescriptor columnDescriptor;
  private final VectorizedColumnIterator vectorizedColumnIterator;
  private final Types.NestedField icebergField;
  private final BufferAllocator rootAlloc;

  private int batchSize;
  private FieldVector vec;
  private Integer typeWidth;
  private ReadType readType;
  private NullabilityHolder nullabilityHolder;

  // In cases when Parquet employs fall back to plain encoding, we eagerly decode the dictionary
  // encoded pages
  // before storing the values in the Arrow vector. This means even if the dictionary is present,
  // data
  // present in the vector may not necessarily be dictionary encoded.
  private Dictionary dictionary;

  public VectorizedArrowReader(
      ColumnDescriptor desc,
      Types.NestedField icebergField,
      BufferAllocator ra,
      boolean setArrowValidityVector) {
    this.icebergField = icebergField;
    this.columnDescriptor = desc;
    this.rootAlloc = ra;
    this.vectorizedColumnIterator = new VectorizedColumnIterator(desc, "", setArrowValidityVector);
  }

  private VectorizedArrowReader() {
    this(null);
  }

  private VectorizedArrowReader(Types.NestedField icebergField) {
    this.icebergField = icebergField;
    this.batchSize = DEFAULT_BATCH_SIZE;
    this.columnDescriptor = null;
    this.rootAlloc = null;
    this.vectorizedColumnIterator = null;
  }

  private enum ReadType {
    FIXED_LENGTH_DECIMAL,
    INT_BACKED_DECIMAL,
    LONG_BACKED_DECIMAL,
    VARCHAR,
    VARBINARY,
    FIXED_WIDTH_BINARY,
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    TIMESTAMP_MILLIS,
    TIMESTAMP_INT96,
    TIME_MICROS,
    UUID,
    DICTIONARY
  }

  protected Types.NestedField icebergField() {
    return icebergField;
  }

  @Override
  public void setBatchSize(int batchSize) {
    this.batchSize = (batchSize == 0) ? DEFAULT_BATCH_SIZE : batchSize;
    this.vectorizedColumnIterator.setBatchSize(batchSize);
  }

  @Override
  public VectorHolder read(VectorHolder reuse, int numValsToRead) {
    boolean dictEncoded = vectorizedColumnIterator.producesDictionaryEncodedVector();
    if (reuse == null
        || (!dictEncoded && readType == ReadType.DICTIONARY)
        || (dictEncoded && readType != ReadType.DICTIONARY)) {
      allocateFieldVector(dictEncoded);
      nullabilityHolder = new NullabilityHolder(batchSize);
    } else {
      vec.setValueCount(0);
      nullabilityHolder.reset();
    }
    if (vectorizedColumnIterator.hasNext()) {
      if (dictEncoded) {
        vectorizedColumnIterator.dictionaryBatchReader().nextBatch(vec, -1, nullabilityHolder);
      } else {
        switch (readType) {
          case VARBINARY:
          case VARCHAR:
            vectorizedColumnIterator
                .varWidthTypeBatchReader()
                .nextBatch(vec, -1, nullabilityHolder);
            break;
          case BOOLEAN:
            vectorizedColumnIterator.booleanBatchReader().nextBatch(vec, -1, nullabilityHolder);
            break;
          case INT:
          case INT_BACKED_DECIMAL:
            vectorizedColumnIterator
                .integerBatchReader()
                .nextBatch(vec, typeWidth, nullabilityHolder);
            break;
          case LONG:
          case LONG_BACKED_DECIMAL:
            vectorizedColumnIterator.longBatchReader().nextBatch(vec, typeWidth, nullabilityHolder);
            break;
          case FLOAT:
            vectorizedColumnIterator
                .floatBatchReader()
                .nextBatch(vec, typeWidth, nullabilityHolder);
            break;
          case DOUBLE:
            vectorizedColumnIterator
                .doubleBatchReader()
                .nextBatch(vec, typeWidth, nullabilityHolder);
            break;
          case TIMESTAMP_MILLIS:
            vectorizedColumnIterator
                .timestampMillisBatchReader()
                .nextBatch(vec, typeWidth, nullabilityHolder);
            break;
          case TIMESTAMP_INT96:
            vectorizedColumnIterator
                .timestampInt96BatchReader()
                .nextBatch(vec, typeWidth, nullabilityHolder);
            break;
          case UUID:
          case FIXED_WIDTH_BINARY:
          case FIXED_LENGTH_DECIMAL:
            vectorizedColumnIterator
                .fixedSizeBinaryBatchReader()
                .nextBatch(vec, typeWidth, nullabilityHolder);
            break;
        }
      }
    }
    Preconditions.checkState(
        vec.getValueCount() == numValsToRead,
        "Number of values read, %s, does not equal expected, %s",
        vec.getValueCount(),
        numValsToRead);
    return new VectorHolder(
        columnDescriptor, vec, dictEncoded, dictionary, nullabilityHolder, icebergField);
  }

  private void allocateFieldVector(boolean dictionaryEncodedVector) {
    if (dictionaryEncodedVector) {
      allocateDictEncodedVector();
    } else {
      Field arrowField = ArrowSchemaUtil.convert(getPhysicalType(columnDescriptor, icebergField));
      if (columnDescriptor.getPrimitiveType().getOriginalType() != null) {
        allocateVectorBasedOnOriginalType(columnDescriptor.getPrimitiveType(), arrowField);
      } else {
        allocateVectorBasedOnTypeName(columnDescriptor.getPrimitiveType(), arrowField);
      }
    }
  }

  private static Types.NestedField getPhysicalType(
      ColumnDescriptor desc, Types.NestedField logicalType) {
    PrimitiveType primitive = desc.getPrimitiveType();
    PrimitiveType.PrimitiveTypeName typeName = primitive.getPrimitiveTypeName();
    Types.NestedField physicalType = logicalType;
    if (OriginalType.DECIMAL.equals(primitive.getOriginalType())) {
      org.apache.iceberg.types.Type type;
      if (PrimitiveType.PrimitiveTypeName.INT64.equals(typeName)) {
        // Use BigIntVector for long backed decimal
        type = Types.LongType.get();
      } else if (PrimitiveType.PrimitiveTypeName.INT32.equals(typeName)) {
        // Use IntVector for int backed decimal
        type = Types.IntegerType.get();
      } else {
        // Use FixedSizeBinaryVector for binary backed decimal
        type = Types.FixedType.ofLength(primitive.getTypeLength());
      }
      physicalType = Types.NestedField.from(logicalType).ofType(type).build();
    }

    return physicalType;
  }

  private void allocateDictEncodedVector() {
    Field field =
        new Field(
            icebergField.name(),
            new FieldType(
                icebergField.isOptional(), new ArrowType.Int(Integer.SIZE, true), null, null),
            null);
    this.vec = field.createVector(rootAlloc);
    ((IntVector) vec).allocateNew(batchSize);
    this.typeWidth = (int) IntVector.TYPE_WIDTH;
    this.readType = ReadType.DICTIONARY;
  }

  private void allocateVectorBasedOnOriginalType(PrimitiveType primitive, Field arrowField) {
    switch (primitive.getOriginalType()) {
      case ENUM:
      case JSON:
      case UTF8:
      case BSON:
        this.vec = arrowField.createVector(rootAlloc);
        // TODO: Possibly use the uncompressed page size info to set the initial capacity
        vec.setInitialCapacity(batchSize * AVERAGE_VARIABLE_WIDTH_RECORD_SIZE);
        vec.allocateNewSafe();
        this.readType = ReadType.VARCHAR;
        this.typeWidth = UNKNOWN_WIDTH;
        break;
      case INT_8:
      case INT_16:
      case INT_32:
        this.vec = arrowField.createVector(rootAlloc);
        ((IntVector) vec).allocateNew(batchSize);
        this.readType = ReadType.INT;
        this.typeWidth = (int) IntVector.TYPE_WIDTH;
        break;
      case DATE:
        this.vec = arrowField.createVector(rootAlloc);
        ((DateDayVector) vec).allocateNew(batchSize);
        this.readType = ReadType.INT;
        this.typeWidth = (int) IntVector.TYPE_WIDTH;
        break;
      case INT_64:
        this.vec = arrowField.createVector(rootAlloc);
        ((BigIntVector) vec).allocateNew(batchSize);
        this.readType = ReadType.LONG;
        this.typeWidth = (int) BigIntVector.TYPE_WIDTH;
        break;
      case TIMESTAMP_MILLIS:
        this.vec = arrowField.createVector(rootAlloc);
        ((BigIntVector) vec).allocateNew(batchSize);
        this.readType = ReadType.TIMESTAMP_MILLIS;
        this.typeWidth = (int) BigIntVector.TYPE_WIDTH;
        break;
      case TIMESTAMP_MICROS:
        this.vec = arrowField.createVector(rootAlloc);
        if (((Types.TimestampType) icebergField.type()).shouldAdjustToUTC()) {
          ((TimeStampMicroTZVector) vec).allocateNew(batchSize);
        } else {
          ((TimeStampMicroVector) vec).allocateNew(batchSize);
        }
        this.readType = ReadType.LONG;
        this.typeWidth = (int) BigIntVector.TYPE_WIDTH;
        break;
      case TIME_MICROS:
        this.vec = arrowField.createVector(rootAlloc);
        ((TimeMicroVector) vec).allocateNew(batchSize);
        this.readType = ReadType.LONG;
        this.typeWidth = (int) TimeMicroVector.TYPE_WIDTH;
        break;
      case DECIMAL:
        this.vec = arrowField.createVector(rootAlloc);
        switch (primitive.getPrimitiveTypeName()) {
          case BINARY:
          case FIXED_LEN_BYTE_ARRAY:
            ((FixedSizeBinaryVector) vec).allocateNew(batchSize);
            this.readType = ReadType.FIXED_LENGTH_DECIMAL;
            this.typeWidth = primitive.getTypeLength();
            break;
          case INT64:
            ((BigIntVector) vec).allocateNew(batchSize);
            this.readType = ReadType.LONG_BACKED_DECIMAL;
            this.typeWidth = (int) BigIntVector.TYPE_WIDTH;
            break;
          case INT32:
            ((IntVector) vec).allocateNew(batchSize);
            this.readType = ReadType.INT_BACKED_DECIMAL;
            this.typeWidth = (int) IntVector.TYPE_WIDTH;
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
        }
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported logical type: " + primitive.getOriginalType());
    }
  }

  private void allocateVectorBasedOnTypeName(PrimitiveType primitive, Field arrowField) {
    switch (primitive.getPrimitiveTypeName()) {
      case FIXED_LEN_BYTE_ARRAY:
        int len;
        if (icebergField.type() instanceof Types.UUIDType) {
          len = 16;
          this.readType = ReadType.UUID;
        } else {
          len = ((Types.FixedType) icebergField.type()).length();
          this.readType = ReadType.FIXED_WIDTH_BINARY;
        }
        this.vec = arrowField.createVector(rootAlloc);
        vec.setInitialCapacity(batchSize * len);
        vec.allocateNew();
        this.typeWidth = len;
        break;
      case BINARY:
        this.vec = arrowField.createVector(rootAlloc);
        // TODO: Possibly use the uncompressed page size info to set the initial capacity
        vec.setInitialCapacity(batchSize * AVERAGE_VARIABLE_WIDTH_RECORD_SIZE);
        vec.allocateNewSafe();
        this.readType = ReadType.VARBINARY;
        this.typeWidth = UNKNOWN_WIDTH;
        break;
      case INT32:
        Field intField =
            new Field(
                icebergField.name(),
                new FieldType(
                    icebergField.isOptional(), new ArrowType.Int(Integer.SIZE, true), null, null),
                null);
        this.vec = intField.createVector(rootAlloc);
        ((IntVector) vec).allocateNew(batchSize);
        this.readType = ReadType.INT;
        this.typeWidth = (int) IntVector.TYPE_WIDTH;
        break;
      case INT96:
        // Impala & Spark used to write timestamps as INT96 by default. For backwards
        // compatibility we try to read INT96 as timestamps. But INT96 is not recommended
        // and deprecated (see https://issues.apache.org/jira/browse/PARQUET-323)
        int length = BigIntVector.TYPE_WIDTH;
        this.readType = ReadType.TIMESTAMP_INT96;
        this.vec = arrowField.createVector(rootAlloc);
        vec.setInitialCapacity(batchSize * length);
        vec.allocateNew();
        this.typeWidth = length;
        break;
      case FLOAT:
        Field floatField =
            new Field(
                icebergField.name(),
                new FieldType(
                    icebergField.isOptional(),
                    new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
                    null,
                    null),
                null);
        this.vec = floatField.createVector(rootAlloc);
        ((Float4Vector) vec).allocateNew(batchSize);
        this.readType = ReadType.FLOAT;
        this.typeWidth = (int) Float4Vector.TYPE_WIDTH;
        break;
      case BOOLEAN:
        this.vec = arrowField.createVector(rootAlloc);
        ((BitVector) vec).allocateNew(batchSize);
        this.readType = ReadType.BOOLEAN;
        this.typeWidth = UNKNOWN_WIDTH;
        break;
      case INT64:
        this.vec = arrowField.createVector(rootAlloc);
        ((BigIntVector) vec).allocateNew(batchSize);
        this.readType = ReadType.LONG;
        this.typeWidth = (int) BigIntVector.TYPE_WIDTH;
        break;
      case DOUBLE:
        this.vec = arrowField.createVector(rootAlloc);
        ((Float8Vector) vec).allocateNew(batchSize);
        this.readType = ReadType.DOUBLE;
        this.typeWidth = (int) Float8Vector.TYPE_WIDTH;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported type: " + primitive);
    }
  }

  @Override
  public void setRowGroupInfo(PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {
    ColumnChunkMetaData chunkMetaData = metadata.get(ColumnPath.get(columnDescriptor.getPath()));
    this.dictionary =
        vectorizedColumnIterator.setRowGroupInfo(
            source.getPageReader(columnDescriptor),
            !ParquetUtil.hasNonDictionaryPages(chunkMetaData));
  }

  @Override
  public void close() {
    if (vec != null) {
      vec.close();
    }
  }

  @Override
  public String toString() {
    return columnDescriptor.toString();
  }

  public static VectorizedArrowReader nulls() {
    return NullVectorReader.INSTANCE;
  }

  public static VectorizedArrowReader positions() {
    return new PositionVectorReader(false);
  }

  public static VectorizedArrowReader positionsWithSetArrowValidityVector() {
    return new PositionVectorReader(true);
  }

  private static final class NullVectorReader extends VectorizedArrowReader {
    private static final NullVectorReader INSTANCE = new NullVectorReader();

    @Override
    public VectorHolder read(VectorHolder reuse, int numValsToRead) {
      return VectorHolder.dummyHolder(numValsToRead);
    }

    @Override
    public void setRowGroupInfo(
        PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {}

    @Override
    public String toString() {
      return "NullReader";
    }

    @Override
    public void setBatchSize(int batchSize) {}
  }

  private static final class PositionVectorReader extends VectorizedArrowReader {
    private static final Field ROW_POSITION_ARROW_FIELD =
        ArrowSchemaUtil.convert(MetadataColumns.ROW_POSITION);
    private final boolean setArrowValidityVector;
    private long rowStart;
    private int batchSize;
    private NullabilityHolder nulls;

    PositionVectorReader(boolean setArrowValidityVector) {
      super(MetadataColumns.ROW_POSITION);
      this.setArrowValidityVector = setArrowValidityVector;
    }

    @Override
    public VectorHolder read(VectorHolder reuse, int numValsToRead) {
      FieldVector vec;
      if (reuse == null) {
        vec = newVector(batchSize);
      } else {
        vec = reuse.vector();
        vec.setValueCount(0);
      }

      ArrowBuf dataBuffer = vec.getDataBuffer();
      for (int i = 0; i < numValsToRead; i += 1) {
        dataBuffer.setLong((long) i * Long.BYTES, rowStart + i);
      }

      if (setArrowValidityVector) {
        ArrowBuf validityBuffer = vec.getValidityBuffer();
        for (int i = 0; i < numValsToRead; i += 1) {
          BitVectorHelper.setBit(validityBuffer, i);
        }
      }

      rowStart += numValsToRead;
      vec.setValueCount(numValsToRead);

      return new VectorHolder.PositionVectorHolder(vec, MetadataColumns.ROW_POSITION, nulls);
    }

    private static BigIntVector newVector(int valueCount) {
      BigIntVector vector =
          (BigIntVector) ROW_POSITION_ARROW_FIELD.createVector(ArrowAllocation.rootAllocator());
      vector.allocateNew(valueCount);
      return vector;
    }

    private static NullabilityHolder newNullabilityHolder(int size) {
      NullabilityHolder nullabilityHolder = new NullabilityHolder(size);
      nullabilityHolder.setNotNulls(0, size);
      return nullabilityHolder;
    }

    @Override
    public void setRowGroupInfo(
        PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {
      this.rowStart =
          source
              .getRowIndexOffset()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "PageReadStore does not contain row index offset"));
    }

    @Override
    public String toString() {
      return getClass().toString();
    }

    @Override
    public void setBatchSize(int batchSize) {
      if (nulls == null || nulls.size() < batchSize) {
        this.nulls = newNullabilityHolder(batchSize);
      }
      this.batchSize = (batchSize == 0) ? DEFAULT_BATCH_SIZE : batchSize;
    }

    @Override
    public void close() {
      // don't close vectors as they are not owned by readers
    }
  }

  /**
   * A Dummy Vector Reader which doesn't actually read files, instead it returns a dummy
   * VectorHolder which indicates the constant value which should be used for this column.
   *
   * @param <T> The constant value to use
   */
  public static class ConstantVectorReader<T> extends VectorizedArrowReader {
    private final T value;

    public ConstantVectorReader(Types.NestedField icebergField, T value) {
      super(icebergField);
      this.value = value;
    }

    @Override
    public VectorHolder read(VectorHolder reuse, int numValsToRead) {
      return VectorHolder.constantHolder(icebergField(), numValsToRead, value);
    }

    @Override
    public void setRowGroupInfo(
        PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {}

    @Override
    public String toString() {
      return String.format("ConstantReader: %s", value);
    }

    @Override
    public void setBatchSize(int batchSize) {}
  }

  /**
   * A Dummy Vector Reader which doesn't actually read files. Instead, it returns a Deleted Vector
   * Holder which indicates whether a given row is deleted.
   */
  public static class DeletedVectorReader extends VectorizedArrowReader {
    public DeletedVectorReader() {
      super(MetadataColumns.IS_DELETED);
    }

    @Override
    public VectorHolder read(VectorHolder reuse, int numValsToRead) {
      return VectorHolder.deletedVectorHolder(numValsToRead);
    }

    @Override
    public void setRowGroupInfo(
        PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {}

    @Override
    public String toString() {
      return "DeletedVectorReader";
    }

    @Override
    public void setBatchSize(int batchSize) {}
  }
}
