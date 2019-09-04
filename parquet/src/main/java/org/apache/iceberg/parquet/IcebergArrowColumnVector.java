package org.apache.iceberg.parquet;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class IcebergArrowColumnVector extends ColumnVector {

  private final ArrowColumnVector vector;
  private final NullabilityVector nullabilityVector;

  public IcebergArrowColumnVector(ArrowColumnVector vector, NullabilityVector nulls) {
    super(vector.dataType());
    this.vector = vector;
    this.nullabilityVector = nulls;
  }
  @Override
  public void close() {
    vector.close();
  }

  @Override
  public boolean hasNull() {
    return nullabilityVector.hasNulls();
  }

  @Override
  public int numNulls() {
    return nullabilityVector.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return nullabilityVector.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return vector.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    return vector.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    return vector.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    return vector.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return vector.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return vector.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return vector.getDouble(rowId);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return vector.getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return vector.getMap(ordinal);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return vector.getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return vector.getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    return vector.getBinary(rowId);
  }

  @Override
  protected ColumnVector getChild(int ordinal) {
    return vector.getChild(ordinal);
  }
}
