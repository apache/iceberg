package org.apache.iceberg.spark.data.vector;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.arrow.memory.RootAllocator;
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
 * 	 icebergType : LONG   		-> 		Field Vector Type : org.apache.arrow.vector.BigIntVector
 * 	 icebergType : STRING  		-> 		Field Vector Type : org.apache.arrow.vector.VarCharVector
 * 	 icebergType : BOOLEAN 		-> 		Field Vector Type : org.apache.arrow.vector.BitVector
 * 	 icebergType : INTEGER 		-> 		Field Vector Type : org.apache.arrow.vector.IntVector
 * 	 icebergType : FLOAT   		-> 		Field Vector Type : org.apache.arrow.vector.Float4Vector
 * 	 icebergType : DOUBLE  		-> 		Field Vector Type : org.apache.arrow.vector.Float8Vector
 * 	 icebergType : DATE    		-> 		Field Vector Type : org.apache.arrow.vector.DateDayVector
 * 	 icebergType : TIMESTAMP  -> 		Field Vector Type : org.apache.arrow.vector.TimeStampMicroTZVector
 * 	 icebergType : STRING  		-> 		Field Vector Type : org.apache.arrow.vector.VarCharVector
 * 	 icebergType : BINARY  		-> 		Field Vector Type : org.apache.arrow.vector.VarBinaryVector
 * 	 icebergField : DECIMAL 	->  	Field Vector Type : org.apache.arrow.vector.DecimalVector
 */
public class VectorizedParquetValueReaders {

  public abstract static class VectorReader extends ParquetValueReaders.PrimitiveReader<FieldVector> {

    protected FieldVector vec;
    protected boolean isOptional;

    VectorReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc) {

      super(desc);
      this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
      this.isOptional = desc.getPrimitiveType().isRepetition(Type.Repetition.OPTIONAL);
      // System.out.println("=> icebergField : "+icebergField.type().typeId().name()+" ,  Field Vector Type : "+vec.getClass().getName());
    }

    @Override
    public FieldVector read(FieldVector ignore) {

      vec.reset();
      int i=0;

      while(column.hasNext()) {
        // Todo: this check works for flat schemas only
        // need to get max definition level to do proper check
        if(isOptional && column.currentDefinitionLevel() == 0) {
          // handle null
          column.nextNull();
          nextNullAt(i);
        } else {
          nextValueAt(i);
        }
        i++;
      }
      vec.setValueCount(i);
      return vec;
    }


    public int getRowCount() {
      return vec.getValueCount();
    }

    protected abstract void nextNullAt(int i);

    protected abstract void nextValueAt(int i);
  }

  protected static class StringReader extends VectorReader {

    StringReader(ColumnDescriptor desc, Types.NestedField icebergField, RootAllocator rootAlloc) {
      super(desc, icebergField, rootAlloc);
    }

    @Override
    protected void nextNullAt(int i) {
      ((VarCharVector) vec).setNull(i);
    }

    @Override
    protected void nextValueAt(int i) {

      Binary binary = column.nextBinary();
      if (binary == null) {

        ((VarCharVector) vec).setNull(i);

      } else {
        String utf8Str = binary.toStringUsingUTF8();
        ((VarCharVector) vec).setSafe(i, utf8Str.getBytes());
      }
    }

  }

  protected static class IntegerReader extends VectorReader {

    IntegerReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc) {

      super(desc, icebergField, rootAlloc);
    }

    @Override
    protected void nextNullAt(int i) {
      ((IntVector) vec).setNull(i);
    }

    protected void nextValueAt(int i) {

      int intValue = column.nextInteger();
      ((IntVector)vec).setSafe(i, intValue);

    }
  }

  protected static class LongReader extends VectorReader {

    LongReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc) {

      super(desc, icebergField, rootAlloc);
    }

    protected void nextNullAt(int i) {
      ((BigIntVector)vec).setNull(i);
    }

    protected void nextValueAt(int i) {

      long longValue = column.nextLong();
      ((BigIntVector)vec).setSafe(i, longValue);

    }
  }

  protected static class TimestampMillisReader extends LongReader {

    TimestampMillisReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc) {
      super(desc, icebergField, rootAlloc);
    }

    protected void nextValueAt(int i) {

      long longValue = column.nextLong();
      ((BigIntVector)vec).setSafe(i, 1000 * longValue);

    }
  }

  protected static class TimestampMicroReader extends VectorReader {

    TimestampMicroReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc) {
      super(desc, icebergField, rootAlloc);
    }

    protected void nextNullAt(int i) {
      ((TimeStampMicroTZVector)vec).setNull(i);
    }

    protected void nextValueAt(int i) {

      long longValue = column.nextLong();
      ((TimeStampMicroTZVector)vec).setSafe(i, longValue);

    }
  }

  protected static class BooleanReader extends VectorReader {

    BooleanReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc) {
      super(desc, icebergField, rootAlloc);
    }

    protected void nextNullAt(int i) {
      ((BitVector)vec).setNull(i);
    }

    protected void nextValueAt(int i) {

      boolean bool = column.nextBoolean();
      ((BitVector)vec).setSafe(i, bool ? 1 : 0);

    }
  }



  protected static class FloatReader extends VectorReader {

    FloatReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc) {
      super(desc, icebergField, rootAlloc);
    }

    protected void nextNullAt(int i) {
      ((Float4Vector)vec).setNull(i);
    }

    protected void nextValueAt(int i) {

      float floatValue = column.nextFloat();
      ((Float4Vector)vec).setSafe(i, floatValue);

    }
  }

  protected static class DoubleReader extends VectorReader {

    DoubleReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc) {
      super(desc, icebergField, rootAlloc);
    }

    protected void nextNullAt(int i) {
      ((Float8Vector)vec).setNull(i);
    }

    protected void nextValueAt(int i) {

      double doubleValue = column.nextDouble();
      ((Float8Vector)vec).setSafe(i, doubleValue);

    }
  }


  protected static class BinaryReader extends VectorReader {

    BinaryReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc) {
      super(desc, icebergField, rootAlloc);
    }

    protected void nextNullAt(int i) {
      ((VarBinaryVector)vec).setNull(i);
    }

    protected void nextValueAt(int i) {

      Binary binaryValue = column.nextBinary();
      ((VarBinaryVector)vec).setSafe(i, binaryValue.getBytes());

    }
  }



  protected static class DateReader extends VectorReader {

    DateReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc) {
      super(desc, icebergField, rootAlloc);
    }

    protected void nextNullAt(int i) {
      ((DateDayVector)vec).setNull(i);
    }

    protected void nextValueAt(int i) {

      int dateValue = column.nextInteger();
      ((DateDayVector)vec).setSafe(i, dateValue);

    }
  }


  protected static class IntegerDecimalReader extends VectorReader {
    private final int precision;
    private final int scale;

    IntegerDecimalReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc,
        int precision, int scale) {

      super(desc, icebergField, rootAlloc);
      this.precision = precision;
      this.scale = scale;
    }

    protected void nextNullAt(int i) {
      ((DecimalVector)vec).setNull(i);
    }

    protected void nextValueAt(int i) {

      int decimalIntValue = column.nextInteger();
      Decimal decimalValue = Decimal.apply(decimalIntValue, precision, scale);

      ((DecimalVector)vec).setSafe(i, decimalValue.toJavaBigDecimal());

    }
  }

  protected static class LongDecimalReader extends VectorReader {
    private final int precision;
    private final int scale;

    LongDecimalReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc,
        int precision, int scale) {

      super(desc, icebergField, rootAlloc);
      this.precision = precision;
      this.scale = scale;
    }

    protected void nextNullAt(int i) {
      ((DecimalVector)vec).setNull(i);
    }

    protected void nextValueAt(int i) {

      long decimalLongValue = column.nextLong();
      Decimal decimalValue = Decimal.apply(decimalLongValue, precision, scale);

      ((DecimalVector)vec).setSafe(i, decimalValue.toJavaBigDecimal());

    }
  }



  protected static class BinaryDecimalReader extends VectorReader {
    private final int precision;
    private final int scale;

    BinaryDecimalReader(ColumnDescriptor desc,
        Types.NestedField icebergField,
        RootAllocator rootAlloc,
        int precision, int scale) {

      super(desc, icebergField, rootAlloc);
      this.precision = precision;
      this.scale = scale;
    }

    protected void nextNullAt(int i) {
      ((DecimalVector)vec).setNull(i);
    }

    protected void nextValueAt(int i) {

      Binary binaryValue = column.nextBinary();
      Decimal decimalValue = Decimal.fromDecimal(new BigDecimal(new BigInteger(binaryValue.getBytes()), scale));

      ((DecimalVector)vec).setSafe(i, decimalValue.toJavaBigDecimal());

    }
  }
}
