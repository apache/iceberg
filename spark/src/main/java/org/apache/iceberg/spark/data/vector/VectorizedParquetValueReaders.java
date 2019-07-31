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

package org.apache.iceberg.spark.data.vector;

import java.math.BigDecimal;
import java.math.BigInteger;
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
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.arrow.ArrowSchemaUtil;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.types.Decimal;

/***
 * Parquet Value Reader implementations for Vectorization.
 * Contains type-wise readers to read parquet data as vectors.
 * - Returns Arrow's Field Vector for each type.
 * - Null values are explicitly handled.
 * - Type serialization is done based on types in Arrow.
 * - Creates One Vector per RowGroup. So a Batch would have as many rows as there are in the underlying RowGroup.
 * - Mapping of Iceberg type to Arrow type is done in ArrowSchemaUtil.convert()
 * - Iceberg to Arrow Type mapping :
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
public class VectorizedParquetValueReaders {

  public abstract static class VectorReader extends ParquetValueReaders.PrimitiveReader<FieldVector> {

    public static final int DEFAULT_NUM_ROWS_IN_BATCH = 10000;
    // private static final Logger LOG = LoggerFactory.getLogger(VectorReader.class);

    private FieldVector vec;
    private boolean isOptional;
    private int rowsInBatch = DEFAULT_NUM_ROWS_IN_BATCH;
    private ColumnDescriptor desc;

    VectorReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc) {

      super(desc);
      this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
      this.isOptional = desc.getPrimitiveType().isRepetition(Type.Repetition.OPTIONAL);
      this.desc = desc;
    }

    VectorReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc,
        int rowsInBatch) {

      this(desc, icebergField, rootAlloc);
      this.rowsInBatch = (rowsInBatch == 0) ? DEFAULT_NUM_ROWS_IN_BATCH : rowsInBatch;
      // LOG.info("=> [VectorReader] rowsInBatch = " + this.rowsInBatch);
    }

    protected FieldVector getVector() {
      return vec;
    }

    protected boolean isOptional() {
      return isOptional;
    }

    @Override
    public FieldVector read(FieldVector ignore) {

      vec.reset();
      int ordinal = 0;

      for (; ordinal < rowsInBatch; ordinal++) {
        if (column.hasNext()) {
          // while (column.hasNext()) {
          // Todo: this check works for flat schemas only
          // need to get max definition level to do proper check
          if (isOptional && column.currentDefinitionLevel() == 0) {
            // handle null
            column.nextNull();
            nextNullAt(ordinal);
          } else {
            nextValueAt(ordinal);
          }
        } else {
          // proceed to next rowgroup Or exit.
          // LOG.info("**** No more in RowGroup. Exiting!");
          break;
        }
        // }
      }
      vec.setValueCount(ordinal);
      // LOG.info("=> Vector col:" + desc.getPrimitiveType().getPrimitiveTypeName() +
      //     ", for setting batch size :" + rowsInBatch + ", with " + ordinal + " values");
      return vec;
    }


    public int getRowCount() {
      return vec.getValueCount();
    }

    protected abstract void nextNullAt(int ordinal);

    protected abstract void nextValueAt(int ordinal);
  }

  protected static class StringReader extends VectorReader {

    StringReader(ColumnDescriptor desc, Types.NestedField icebergField,
        BufferAllocator rootAlloc, int recordsPerBatch) {
      super(desc, icebergField, rootAlloc, recordsPerBatch);
    }

    @Override
    protected void nextNullAt(int ordinal) {
      ((VarCharVector) getVector()).setNull(ordinal);
    }

    @Override
    protected void nextValueAt(int ordinal) {

      Binary binary = column.nextBinary();
      if (binary == null) {

        ((VarCharVector) getVector()).setNull(ordinal);

      } else {
        String utf8Str = binary.toStringUsingUTF8();
        ((VarCharVector) getVector()).setSafe(ordinal, utf8Str.getBytes());
      }
    }

  }

  protected static class IntegerReader extends VectorReader {

    IntegerReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc, int recordsPerBatch) {

      super(desc, icebergField, rootAlloc, recordsPerBatch);
    }

    @Override
    protected void nextNullAt(int ordinal) {
      ((IntVector) getVector()).setNull(ordinal);
    }

    protected void nextValueAt(int ordinal) {

      int intValue = column.nextInteger();
      ((IntVector) getVector()).setSafe(ordinal, intValue);

    }
  }

  protected static class LongReader extends VectorReader {

    LongReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc, int recordsPerBatch) {

      super(desc, icebergField, rootAlloc, recordsPerBatch);
    }

    protected void nextNullAt(int ordinal) {
      ((BigIntVector) getVector()).setNull(ordinal);
    }

    protected void nextValueAt(int ordinal) {

      long longValue = column.nextLong();
      ((BigIntVector) getVector()).setSafe(ordinal, longValue);

    }
  }

  protected static class TimestampMillisReader extends LongReader {

    TimestampMillisReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc, int recordsPerBatch) {
      super(desc, icebergField, rootAlloc, recordsPerBatch);
    }

    protected void nextValueAt(int ordinal) {

      long longValue = column.nextLong();
      ((BigIntVector) getVector()).setSafe(ordinal, 1000 * longValue);

    }
  }

  protected static class TimestampMicroReader extends VectorReader {

    TimestampMicroReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc, int recordsPerBatch) {
      super(desc, icebergField, rootAlloc, recordsPerBatch);
    }

    protected void nextNullAt(int ordinal) {
      ((TimeStampMicroTZVector) getVector()).setNull(ordinal);
    }

    protected void nextValueAt(int ordinal) {

      long longValue = column.nextLong();
      ((TimeStampMicroTZVector) getVector()).setSafe(ordinal, longValue);

    }
  }

  protected static class BooleanReader extends VectorReader {

    BooleanReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc, int recordsPerBatch) {
      super(desc, icebergField, rootAlloc, recordsPerBatch);
    }

    protected void nextNullAt(int ordinal) {
      ((BitVector) getVector()).setNull(ordinal);
    }

    protected void nextValueAt(int ordinal) {

      boolean bool = column.nextBoolean();
      ((BitVector) getVector()).setSafe(ordinal, bool ? 1 : 0);

    }
  }


  protected static class FloatReader extends VectorReader {

    FloatReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc, int recordsPerBatch) {
      super(desc, icebergField, rootAlloc, recordsPerBatch);
    }

    protected void nextNullAt(int ordinal) {
      ((Float4Vector) getVector()).setNull(ordinal);
    }

    protected void nextValueAt(int ordinal) {

      float floatValue = column.nextFloat();
      ((Float4Vector) getVector()).setSafe(ordinal, floatValue);

    }
  }

  protected static class DoubleReader extends VectorReader {

    DoubleReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc, int recordsPerBatch) {
      super(desc, icebergField, rootAlloc, recordsPerBatch);
    }

    protected void nextNullAt(int ordinal) {
      ((Float8Vector) getVector()).setNull(ordinal);
    }

    protected void nextValueAt(int ordinal) {

      double doubleValue = column.nextDouble();
      ((Float8Vector) getVector()).setSafe(ordinal, doubleValue);

    }
  }


  protected static class BinaryReader extends VectorReader {

    BinaryReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc, int recordsPerBatch) {
      super(desc, icebergField, rootAlloc, recordsPerBatch);
    }

    protected void nextNullAt(int ordinal) {
      ((VarBinaryVector) getVector()).setNull(ordinal);
    }

    protected void nextValueAt(int ordinal) {

      Binary binaryValue = column.nextBinary();
      ((VarBinaryVector) getVector()).setSafe(ordinal, binaryValue.getBytes());

    }
  }


  protected static class DateReader extends VectorReader {

    DateReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc, int recordsPerBatch) {
      super(desc, icebergField, rootAlloc, recordsPerBatch);
    }

    protected void nextNullAt(int ordinal) {
      ((DateDayVector) getVector()).setNull(ordinal);
    }

    protected void nextValueAt(int ordinal) {

      int dateValue = column.nextInteger();
      ((DateDayVector) getVector()).setSafe(ordinal, dateValue);

    }
  }


  protected static class IntegerDecimalReader extends VectorReader {
    private final int precision;
    private final int scale;

    IntegerDecimalReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc,
        int precision, int scale, int recordsPerBatch) {

      super(desc, icebergField, rootAlloc, recordsPerBatch);
      this.precision = precision;
      this.scale = scale;
    }

    protected void nextNullAt(int ordinal) {
      ((DecimalVector) getVector()).setNull(ordinal);
    }

    protected void nextValueAt(int ordinal) {

      int decimalIntValue = column.nextInteger();
      Decimal decimalValue = Decimal.apply(decimalIntValue, precision, scale);

      ((DecimalVector) getVector()).setSafe(ordinal, decimalValue.toJavaBigDecimal());

    }
  }

  protected static class LongDecimalReader extends VectorReader {
    private final int precision;
    private final int scale;

    LongDecimalReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc,
        int precision, int scale, int recordsPerBatch) {

      super(desc, icebergField, rootAlloc, recordsPerBatch);
      this.precision = precision;
      this.scale = scale;
    }

    protected void nextNullAt(int ordinal) {
      ((DecimalVector) getVector()).setNull(ordinal);
    }

    protected void nextValueAt(int ordinal) {

      long decimalLongValue = column.nextLong();
      Decimal decimalValue = Decimal.apply(decimalLongValue, precision, scale);

      ((DecimalVector) getVector()).setSafe(ordinal, decimalValue.toJavaBigDecimal());

    }
  }

  protected static class BinaryDecimalReader extends VectorReader {
    private final int precision;
    private final int scale;

    BinaryDecimalReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        BufferAllocator rootAlloc,
        int precision, int scale, int recordsPerBatch) {

      super(desc, icebergField, rootAlloc, recordsPerBatch);
      this.precision = precision;
      this.scale = scale;
    }

    protected void nextNullAt(int ordinal) {
      ((DecimalVector) getVector()).setNull(ordinal);
    }

    protected void nextValueAt(int ordinal) {

      Binary binaryValue = column.nextBinary();
      Decimal decimalValue = Decimal.fromDecimal(new BigDecimal(new BigInteger(binaryValue.getBytes()), scale));

      ((DecimalVector) getVector()).setSafe(ordinal, decimalValue.toJavaBigDecimal());

    }
  }
}
