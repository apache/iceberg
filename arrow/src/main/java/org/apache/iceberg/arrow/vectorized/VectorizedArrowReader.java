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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.PrimitiveType;

/**
 * {@link VectorizedReader VectorReader(s)} that read in a batch of values into Arrow vectors. It also takes care of
 * allocating the right kind of Arrow vectors depending on the corresponding Iceberg/Parquet data types.
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

  // In cases when Parquet employs fall back to plain encoding, we eagerly decode the dictionary encoded pages
  // before storing the values in the Arrow vector. This means even if the dictionary is present, data
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
    this.icebergField = null;
    this.batchSize = DEFAULT_BATCH_SIZE;
    this.columnDescriptor = null;
    this.rootAlloc = null;
    this.vectorizedColumnIterator = null;
  }

  private enum ReadType {
    FIXED_LENGTH_DECIMAL,
    INT_LONG_BACKED_DECIMAL,
    VARCHAR,
    VARBINARY,
    FIXED_WIDTH_BINARY,
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    TIMESTAMP_MILLIS
  }

  @Override
  public void setBatchSize(int batchSize) {
    this.batchSize = (batchSize == 0) ? DEFAULT_BATCH_SIZE : batchSize;
    this.vectorizedColumnIterator.setBatchSize(batchSize);
  }

  @Override
  public VectorHolder read(VectorHolder reuse, int numValsToRead) {
    if (reuse == null) {
      allocateFieldVector(this.vectorizedColumnIterator.producesDictionaryEncodedVector());
      nullabilityHolder = new NullabilityHolder(batchSize);
    } else {
      vec.setValueCount(0);
      nullabilityHolder.reset();
    }
    boolean dictEncoded = vectorizedColumnIterator.producesDictionaryEncodedVector();
    if (vectorizedColumnIterator.hasNext()) {
      if (dictEncoded) {
        vectorizedColumnIterator.nextBatchDictionaryIds((IntVector) vec, nullabilityHolder);
      } else {
        switch (readType) {
          case FIXED_LENGTH_DECIMAL:
            ((IcebergArrowVectors.DecimalArrowVector) vec).setNullabilityHolder(nullabilityHolder);
            vectorizedColumnIterator.nextBatchFixedLengthDecimal(vec, typeWidth, nullabilityHolder);
            break;
          case INT_LONG_BACKED_DECIMAL:
            ((IcebergArrowVectors.DecimalArrowVector) vec).setNullabilityHolder(nullabilityHolder);
            vectorizedColumnIterator.nextBatchIntLongBackedDecimal(vec, typeWidth, nullabilityHolder);
            break;
          case VARBINARY:
            vectorizedColumnIterator.nextBatchVarWidthType(vec, nullabilityHolder);
            break;
          case VARCHAR:
            ((IcebergArrowVectors.VarcharArrowVector) vec).setNullabilityHolder(nullabilityHolder);
            vectorizedColumnIterator.nextBatchVarWidthType(vec, nullabilityHolder);
            break;
          case FIXED_WIDTH_BINARY:
            vectorizedColumnIterator.nextBatchFixedWidthBinary(vec, typeWidth, nullabilityHolder);
            break;
          case BOOLEAN:
            vectorizedColumnIterator.nextBatchBoolean(vec, nullabilityHolder);
            break;
          case INT:
            vectorizedColumnIterator.nextBatchIntegers(vec, typeWidth, nullabilityHolder);
            break;
          case LONG:
            vectorizedColumnIterator.nextBatchLongs(vec, typeWidth, nullabilityHolder);
            break;
          case FLOAT:
            vectorizedColumnIterator.nextBatchFloats(vec, typeWidth, nullabilityHolder);
            break;
          case DOUBLE:
            vectorizedColumnIterator.nextBatchDoubles(vec, typeWidth, nullabilityHolder);
            break;
          case TIMESTAMP_MILLIS:
            vectorizedColumnIterator.nextBatchTimestampMillis(vec, typeWidth, nullabilityHolder);
            break;
        }
      }
    }
    Preconditions.checkState(vec.getValueCount() == numValsToRead,
        "Number of values read, %s, does not equal expected, %s", vec.getValueCount(), numValsToRead);
    return new VectorHolder(columnDescriptor, vec, dictEncoded, dictionary,
        nullabilityHolder, icebergField.type());
  }

  private void allocateFieldVector(boolean dictionaryEncodedVector) {
    if (dictionaryEncodedVector) {
      Field field = new Field(
          icebergField.name(),
          new FieldType(icebergField.isOptional(), new ArrowType.Int(Integer.SIZE, true), null, null),
          null);
      this.vec = field.createVector(rootAlloc);
      ((IntVector) vec).allocateNew(batchSize);
      this.typeWidth = (int) IntVector.TYPE_WIDTH;
    } else {
      PrimitiveType primitive = columnDescriptor.getPrimitiveType();
      Field arrowField = ArrowSchemaUtil.convert(icebergField);
      if (primitive.getOriginalType() != null) {
        switch (primitive.getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
          case BSON:
            this.vec = new IcebergArrowVectors.VarcharArrowVector(icebergField.name(), rootAlloc);
            //TODO: Possibly use the uncompressed page size info to set the initial capacity
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
            ((TimeStampMicroTZVector) vec).allocateNew(batchSize);
            this.readType = ReadType.LONG;
            this.typeWidth = (int) BigIntVector.TYPE_WIDTH;
            break;
          case DECIMAL:
            DecimalMetadata decimal = primitive.getDecimalMetadata();
            this.vec = new IcebergArrowVectors.DecimalArrowVector(icebergField.name(), rootAlloc,
                decimal.getPrecision(), decimal.getScale());
            ((DecimalVector) vec).allocateNew(batchSize);
            switch (primitive.getPrimitiveTypeName()) {
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                this.readType = ReadType.FIXED_LENGTH_DECIMAL;
                this.typeWidth = primitive.getTypeLength();
                break;
              case INT64:
                this.readType = ReadType.INT_LONG_BACKED_DECIMAL;
                this.typeWidth = (int) BigIntVector.TYPE_WIDTH;
                break;
              case INT32:
                this.readType = ReadType.INT_LONG_BACKED_DECIMAL;
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
      } else {
        switch (primitive.getPrimitiveTypeName()) {
          case FIXED_LEN_BYTE_ARRAY:
            int len = ((Types.FixedType) icebergField.type()).length();
            this.vec = arrowField.createVector(rootAlloc);
            vec.setInitialCapacity(batchSize * len);
            vec.allocateNew();
            this.readType = ReadType.FIXED_WIDTH_BINARY;
            this.typeWidth = len;
            break;
          case BINARY:
            this.vec = arrowField.createVector(rootAlloc);
            //TODO: Possibly use the uncompressed page size info to set the initial capacity
            vec.setInitialCapacity(batchSize * AVERAGE_VARIABLE_WIDTH_RECORD_SIZE);
            vec.allocateNewSafe();
            this.readType = ReadType.VARBINARY;
            this.typeWidth = UNKNOWN_WIDTH;
            break;
          case INT32:
            this.vec = arrowField.createVector(rootAlloc);
            ((IntVector) vec).allocateNew(batchSize);
            this.readType = ReadType.INT;
            this.typeWidth = (int) IntVector.TYPE_WIDTH;
            break;
          case FLOAT:
            this.vec = arrowField.createVector(rootAlloc);
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
    }
  }

  @Override
  public void setRowGroupInfo(PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {
    ColumnChunkMetaData chunkMetaData = metadata.get(ColumnPath.get(columnDescriptor.getPath()));
    this.dictionary = vectorizedColumnIterator.setRowGroupInfo(
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

  private static final class NullVectorReader extends VectorizedArrowReader {
    private static final NullVectorReader INSTANCE = new NullVectorReader();

    @Override
    public VectorHolder read(VectorHolder reuse, int numValsToRead) {
      return VectorHolder.dummyHolder(numValsToRead);
    }

    @Override
    public void setRowGroupInfo(PageReadStore source, Map<ColumnPath, ColumnChunkMetaData> metadata) {
    }

    @Override
    public String toString() {
      return "NullReader";
    }

    @Override
    public void setBatchSize(int batchSize) {}
  }

}

