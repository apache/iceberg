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

package org.apache.iceberg.parquet.vectorized;

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
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.parquet.arrow.ArrowSchemaUtil;
import org.apache.iceberg.parquet.arrow.IcebergDecimalArrowVector;
import org.apache.iceberg.parquet.arrow.IcebergVarBinaryArrowVector;
import org.apache.iceberg.parquet.arrow.IcebergVarcharArrowVector;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.PrimitiveType;

/***
 * {@link VectorizedReader VectorReader(s)} that read in a batch of values into Arrow vectors.
 * It also takes care of allocating the right kind of Arrow vectors depending on the corresponding
 * Iceberg/Parquet data types.
 */
public class VectorizedArrowReader implements VectorizedReader {
  public static final int DEFAULT_BATCH_SIZE = 5000;
  public static final int UNKNOWN_WIDTH = -1;

  private final ColumnDescriptor columnDescriptor;
  private final int batchSize;
  private final VectorizedColumnIterator vectorizedColumnIterator;
  private final boolean isFixedLengthDecimal;
  private final boolean isVarWidthType;
  private final boolean isFixedWidthBinary;
  private final boolean isBooleanType;
  private final boolean isPaddedDecimal;
  private final boolean isIntType;
  private final boolean isLongType;
  private final boolean isFloatType;
  private final boolean isDoubleType;
  private final Types.NestedField icebergField;
  private final BufferAllocator rootAlloc;
  private FieldVector vec;
  private int typeWidth;

  // In cases when Parquet employs fall back encoding, we eagerly decode the dictionary encoded data
  // before storing the values in the Arrow vector. This means even if the dictionary is present, data
  // present in the vector may not be dictionary encoded.
  private Dictionary dictionary;
  private boolean allPagesDictEncoded;

  // This value is copied from Arrow's BaseVariableWidthVector. We may need to change
  // this value if Arrow ends up changing this default.
  private static final int DEFAULT_RECORD_BYTE_COUNT = 8;

  public VectorizedArrowReader(
      ColumnDescriptor desc,
      Types.NestedField icebergField,
      BufferAllocator ra,
      int batchSize) {
    this.icebergField = icebergField;
    this.batchSize = (batchSize == 0) ? DEFAULT_BATCH_SIZE : batchSize;
    this.columnDescriptor = desc;
    this.rootAlloc = ra;
    this.isFixedLengthDecimal = ParquetUtil.isFixedLengthDecimal(desc);
    this.isVarWidthType = ParquetUtil.isVarWidthType(desc);
    this.isFixedWidthBinary = ParquetUtil.isFixedWidthBinary(desc);
    this.isBooleanType = ParquetUtil.isBooleanType(desc);
    this.isPaddedDecimal = ParquetUtil.isIntLongBackedDecimal(desc);
    this.isIntType = ParquetUtil.isIntType(desc);
    this.isLongType = ParquetUtil.isLongType(desc);
    this.isFloatType = ParquetUtil.isFloatType(desc);
    this.isDoubleType = ParquetUtil.isDoubleType(desc);
    this.vectorizedColumnIterator = new VectorizedColumnIterator(desc, "", batchSize);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public VectorHolder read(NullabilityHolder nullabilityHolder) {
    if (vec == null) {
      typeWidth = allocateFieldVector();
    }
    vec.setValueCount(0);
    if (vectorizedColumnIterator.hasNext()) {
      if (allPagesDictEncoded) {
        vectorizedColumnIterator.nextBatchDictionaryIds((IntVector) vec, nullabilityHolder);
      } else {
        if (isFixedLengthDecimal) {
          vectorizedColumnIterator.nextBatchFixedLengthDecimal(vec, typeWidth, nullabilityHolder);
          ((IcebergDecimalArrowVector) vec).setNullabilityHolder(nullabilityHolder);
        } else if (isFixedWidthBinary) {
          // Fixed width binary type values are stored in an IcebergVarBinaryArrowVector as well
          if (vec instanceof IcebergVarBinaryArrowVector) {
            ((IcebergVarBinaryArrowVector) vec).setNullabilityHolder(nullabilityHolder);
          }
          vectorizedColumnIterator.nextBatchFixedWidthBinary(vec, typeWidth, nullabilityHolder);
        } else if (isVarWidthType) {
          if (vec instanceof IcebergVarcharArrowVector) {
            ((IcebergVarcharArrowVector) vec).setNullabilityHolder(nullabilityHolder);
          } else if (vec instanceof IcebergVarBinaryArrowVector) {
            ((IcebergVarBinaryArrowVector) vec).setNullabilityHolder(nullabilityHolder);
          }
          vectorizedColumnIterator.nextBatchVarWidthType(vec, nullabilityHolder);
        } else if (isBooleanType) {
          vectorizedColumnIterator.nextBatchBoolean(vec, nullabilityHolder);
        } else if (isPaddedDecimal) {
          ((IcebergDecimalArrowVector) vec).setNullabilityHolder(nullabilityHolder);
          vectorizedColumnIterator.nextBatchIntLongBackedDecimal(vec, typeWidth, nullabilityHolder);
        } else if (isIntType) {
          vectorizedColumnIterator.nextBatchIntegers(vec, typeWidth, nullabilityHolder);
        } else if (isLongType) {
          vectorizedColumnIterator.nextBatchLongs(vec, typeWidth, nullabilityHolder);
        } else if (isFloatType) {
          vectorizedColumnIterator.nextBatchFloats(vec, typeWidth, nullabilityHolder);
        } else if (isDoubleType) {
          vectorizedColumnIterator.nextBatchDoubles(vec, typeWidth, nullabilityHolder);
        }
      }
    }
    return new VectorHolder(columnDescriptor, vec, allPagesDictEncoded, dictionary);
  }

  private int allocateFieldVector() {
    if (allPagesDictEncoded) {
      Field field = new Field(
          icebergField.name(),
          new FieldType(icebergField.isOptional(), new ArrowType.Int(Integer.SIZE, true), null, null),
          null);
      this.vec = field.createVector(rootAlloc);
      ((IntVector) vec).allocateNew(batchSize);
      return IntVector.TYPE_WIDTH;
    } else {
      PrimitiveType primitive = columnDescriptor.getPrimitiveType();
      if (primitive.getOriginalType() != null) {
        switch (columnDescriptor.getPrimitiveType().getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
          case BSON:
            this.vec = new IcebergVarcharArrowVector(icebergField.name(), rootAlloc);
            vec.setInitialCapacity(batchSize * 10);
            //TODO: samarth use the uncompressed page size info here
            vec.allocateNewSafe();
            return UNKNOWN_WIDTH;
          case INT_8:
          case INT_16:
          case INT_32:
            this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
            ((IntVector) vec).allocateNew(batchSize);
            return IntVector.TYPE_WIDTH;
          case DATE:
            this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
            ((DateDayVector) vec).allocateNew(batchSize);
            return IntVector.TYPE_WIDTH;
          case INT_64:
          case TIMESTAMP_MILLIS:
            this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
            ((BigIntVector) vec).allocateNew(batchSize);
            return BigIntVector.TYPE_WIDTH;
          case TIMESTAMP_MICROS:
            this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
            ((TimeStampMicroTZVector) vec).allocateNew(batchSize);
            return BigIntVector.TYPE_WIDTH;
          case DECIMAL:
            DecimalMetadata decimal = primitive.getDecimalMetadata();
            this.vec = new IcebergDecimalArrowVector(icebergField.name(), rootAlloc, decimal.getPrecision(),
                decimal.getScale());
            ((DecimalVector) vec).allocateNew(batchSize);
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
            this.vec = new IcebergVarBinaryArrowVector(icebergField.name(), rootAlloc);
            int factor = (len + DEFAULT_RECORD_BYTE_COUNT - 1) / DEFAULT_RECORD_BYTE_COUNT;
            vec.setInitialCapacity(batchSize * factor);
            vec.allocateNew();
            return len;
          case BINARY:
            this.vec = new IcebergVarBinaryArrowVector(icebergField.name(), rootAlloc);
            vec.setInitialCapacity(batchSize * 10);
            //TODO: samarth use the uncompressed page size info here
            vec.allocateNewSafe();
            return UNKNOWN_WIDTH;
          case INT32:
            this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
            ((IntVector) vec).allocateNew(batchSize);
            return IntVector.TYPE_WIDTH;
          case FLOAT:
            this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
            ((Float4Vector) vec).allocateNew(batchSize);
            return Float4Vector.TYPE_WIDTH;
          case BOOLEAN:
            this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
            ((BitVector) vec).allocateNew(batchSize);
            return UNKNOWN_WIDTH;
          case INT64:
            this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
            ((BigIntVector) vec).allocateNew(batchSize);
            return BigIntVector.TYPE_WIDTH;
          case DOUBLE:
            this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
            ((Float8Vector) vec).allocateNew(batchSize);
            return Float8Vector.TYPE_WIDTH;
          default:
            throw new UnsupportedOperationException("Unsupported type: " + primitive);
        }
      }
    }
  }

  public void setRowGroupInfo(
      PageReadStore source,
      DictionaryPageReadStore dictionaryPageReadStore,
      Map<ColumnPath, Boolean> columnDictEncoded) {
    allPagesDictEncoded = columnDictEncoded.get(ColumnPath.get(columnDescriptor.getPath()));
    dictionary = vectorizedColumnIterator.setRowGroupInfo(source, dictionaryPageReadStore, allPagesDictEncoded);
  }

  @Override
  public String toString() {
    return columnDescriptor.toString();
  }

  public int batchSize() {
    return batchSize;
  }
}

