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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.iceberg.arrow.ArrowSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.PrimitiveType;

/***
 * Creates and allocates space for Arrow field vectors based on Iceberg data type mapped to Arrow type.
 * Iceberg to Arrow Type mapping :
 *   icebergType : LONG       -   Field Vector Type : org.apache.arrow.vector.BigIntVector
 *   icebergType : STRING     -   Field Vector Type : org.apache.arrow.vector.VarCharVector
 *   icebergType : BOOLEAN    -   Field Vector Type : org.apache.arrow.vector.BitVector
 *   icebergType : INTEGER    -   Field Vector Type : org.apache.arrow.vector.IntVector
 *   icebergType : FLOAT      -   Field Vector Type : org.apache.arrow.vector.Float4Vector
 *   icebergType : DOUBLE     -   Field Vector Type : org.apache.arrow.vector.Float8Vector
 *   icebergType : DATE       -   Field Vector Type : org.apache.arrow.vector.DateDayVector
 *   icebergType : TIMESTAMP  -   Field Vector Type : org.apache.arrow.vector.TimeStampMicroTZVector
 *   icebergType : STRING     -   Field Vector Type : org.apache.arrow.vector.VarCharVector
 *   icebergType : BINARY     -   Field Vector Type : org.apache.arrow.vector.VarBinaryVector
 *   icebergField : DECIMAL   -   Field Vector Type : org.apache.arrow.vector.DecimalVector
 */
public class VectorReader implements BatchedReader {
  public static final int DEFAULT_NUM_ROWS_IN_BATCH = 10000;
  public static final int UNKNOWN_WIDTH = -1;

  private final ColumnDescriptor columnDescriptor;
  private FieldVector vec;
  private final int rowsInBatch;
  private final BatchedColumnIterator batchedColumnIterator;
  private final int typeWidth;
  private final boolean isFixedLengthDecimal;
  private final boolean isVarWidthType;
  private final boolean isFixedWidthBinary;
  private final boolean isBooleanType;
  private final boolean isPaddedDecimal;
  private final boolean isIntType;
  private final boolean isLongType;
  private final boolean isFloatType;
  private final boolean isDoubleType;

  public VectorReader(
      ColumnDescriptor desc,
      Types.NestedField icebergField,
      BufferAllocator rootAlloc,
      int batchSize) {
    this.rowsInBatch = (batchSize == 0) ? DEFAULT_NUM_ROWS_IN_BATCH : batchSize;
    this.columnDescriptor = desc;
    this.typeWidth = allocateFieldVector(rootAlloc, icebergField, desc);

    this.isFixedLengthDecimal = ParquetUtil.isFixedLengthDecimal(desc);
    this.isVarWidthType = ParquetUtil.isVarWidthType(desc);
    this.isFixedWidthBinary = ParquetUtil.isFixedWidthBinary(desc);
    this.isBooleanType = ParquetUtil.isBooleanType(desc);
    this.isPaddedDecimal = ParquetUtil.isIntLongBackedDecimal(desc);
    this.isIntType = ParquetUtil.isIntType(desc);
    this.isLongType = ParquetUtil.isLongType(desc);
    this.isFloatType = ParquetUtil.isFloatType(desc);
    this.isDoubleType = ParquetUtil.isDoubleType(desc);
    this.batchedColumnIterator = new BatchedColumnIterator(desc, "", batchSize);

  }

  private int allocateFieldVector(BufferAllocator rootAlloc, Types.NestedField icebergField, ColumnDescriptor desc) {

    PrimitiveType primitive = desc.getPrimitiveType();
    if (primitive.getOriginalType() != null) {
      switch (desc.getPrimitiveType().getOriginalType()) {
        case ENUM:
        case JSON:
        case UTF8:
        case BSON:
          this.vec = new IcebergVarcharArrowVector(icebergField.name(), rootAlloc);
          vec.setInitialCapacity(rowsInBatch * 10);
          //TODO: samarth use the uncompressed page size info here
          vec.allocateNewSafe();
          return UNKNOWN_WIDTH;
        case INT_8:
        case INT_16:
        case INT_32:
          this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
          ((IntVector) vec).allocateNew(rowsInBatch * IntVector.TYPE_WIDTH);
          return IntVector.TYPE_WIDTH;
        case DATE:
          this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
          ((DateDayVector) vec).allocateNew(rowsInBatch * IntVector.TYPE_WIDTH);
          return IntVector.TYPE_WIDTH;
        case INT_64:
        case TIMESTAMP_MILLIS:
          this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
          ((BigIntVector) vec).allocateNew(rowsInBatch * BigIntVector.TYPE_WIDTH);
          return BigIntVector.TYPE_WIDTH;
        case TIMESTAMP_MICROS:
          this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
          ((TimeStampMicroTZVector) vec).allocateNew(rowsInBatch * BigIntVector.TYPE_WIDTH);
          return BigIntVector.TYPE_WIDTH;
        case DECIMAL:
          DecimalMetadata decimal = primitive.getDecimalMetadata();
          this.vec = new IcebergDecimalArrowVector(icebergField.name(), rootAlloc, decimal.getPrecision(),
              decimal.getScale());
          ((DecimalVector) vec).allocateNew(rowsInBatch * DecimalVector.TYPE_WIDTH);
          switch (primitive.getPrimitiveTypeName()) {
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
              return primitive.getTypeLength();
            case INT64:
              return BigIntVector.TYPE_WIDTH;
            case INT32:
              return IntVector.TYPE_WIDTH;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
          }
        default:
          throw new UnsupportedOperationException(
              "Unsupported logical type: " + primitive.getOriginalType());
      }
    } else {
      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
          int len = ((Types.FixedType) icebergField.type()).length();
          this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
          vec.setInitialCapacity(rowsInBatch * len);
          vec.allocateNew();
          return len;
        case BINARY:
          this.vec = new IcebergVarBinaryArrowVector(icebergField.name(), rootAlloc);
          vec.setInitialCapacity(rowsInBatch * 10);
          //TODO: samarth use the uncompressed page size info here
          vec.allocateNewSafe();
          return UNKNOWN_WIDTH;
        case INT32:
          this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
          ((IntVector) vec).allocateNew(rowsInBatch * IntVector.TYPE_WIDTH);
          return IntVector.TYPE_WIDTH;
        case FLOAT:
          this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
          ((Float4Vector) vec).allocateNew(rowsInBatch * Float4Vector.TYPE_WIDTH);
          return Float4Vector.TYPE_WIDTH;
        case BOOLEAN:
          this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
          ((BitVector) vec).allocateNew(rowsInBatch);
          return UNKNOWN_WIDTH;
        case INT64:
          this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
          ((BigIntVector) vec).allocateNew(rowsInBatch * BigIntVector.TYPE_WIDTH);
          return BigIntVector.TYPE_WIDTH;
        case DOUBLE:
          this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
          ((Float8Vector) vec).allocateNew(rowsInBatch * Float8Vector.TYPE_WIDTH);
          return Float8Vector.TYPE_WIDTH;
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }
  }

  public FieldVector read(NullabilityHolder nullabilityHolder) {
    //TODO: samarth confirm this
    vec.setValueCount(0);
    if (batchedColumnIterator.hasNext()) {
      if (isFixedLengthDecimal) {
        batchedColumnIterator.nextBatchFixedLengthDecimal(vec, typeWidth, nullabilityHolder);
        ((IcebergDecimalArrowVector) vec).setNullabilityHolder(nullabilityHolder);
      } else if (isVarWidthType) {
        if (vec instanceof IcebergVarcharArrowVector) {
          //TODO: samarth this could possibly be a general varwidth vector and not just varchar
          ((IcebergVarcharArrowVector) vec).setNullabilityHolder(nullabilityHolder);
        } else if (vec instanceof IcebergVarBinaryArrowVector) {
          ((IcebergVarBinaryArrowVector) vec).setNullabilityHolder(nullabilityHolder);
        }
        batchedColumnIterator.nextBatchVarWidthType(vec, nullabilityHolder);
      } else if (isFixedWidthBinary) {
        batchedColumnIterator.nextBatchFixedWidthBinary(vec, typeWidth, nullabilityHolder);
      } else if (isBooleanType) {
        batchedColumnIterator.nextBatchBoolean(vec, nullabilityHolder);
      } else if (isPaddedDecimal) {
        ((IcebergDecimalArrowVector) vec).setNullabilityHolder(nullabilityHolder);
        batchedColumnIterator.nextBatchIntLongBackedDecimal(vec, typeWidth, nullabilityHolder);
      } else if (isIntType) {
        batchedColumnIterator.nextBatchIntegers(vec, typeWidth, nullabilityHolder);
      } else if (isLongType) {
        batchedColumnIterator.nextBatchLongs(vec, typeWidth, nullabilityHolder);
      } else if (isFloatType) {
        batchedColumnIterator.nextBatchFloats(vec, typeWidth, nullabilityHolder);
      } else if (isDoubleType) {
        batchedColumnIterator.nextBatchDoubles(vec, typeWidth, nullabilityHolder);
      }
    }
    return vec;
  }

  public void setPageSource(PageReadStore source) {
    batchedColumnIterator.setPageSource(source);
  }

  @Override
  public String toString() {
    return columnDescriptor.toString();
  }

  public int batchSize() {
    return rowsInBatch;
  }
}

