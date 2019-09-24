package org.apache.iceberg.parquet.org.apache.iceberg.parquet.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.iceberg.parquet.NullabilityHolder;

/**
 *
 * Extension of Arrow's @{@link DecimalVector}. The whole reason of having
 * this implementation is to override the expensive {@link DecimalVector#isSet(int)} method
 * used by  {@link DecimalVector#getObject(int)}.
 */
public class IcebergDecimalArrowVector extends DecimalVector {
  private NullabilityHolder nullabilityHolder;

  public IcebergDecimalArrowVector(
      String name,
      BufferAllocator allocator, int precision, int scale) {
    super(name, allocator, precision, scale);
  }

  /**
   * Same as {@link #isNull(int)}.
   *
   * @param index position of element
   * @return 1 if element at given index is not null, 0 otherwise
   */
  @Override
  public int isSet(int index) {
    return nullabilityHolder.isNullAt(index) ? 0 : 1;
  }

  public void setNullabilityHolder(NullabilityHolder nullabilityHolder) {
    this.nullabilityHolder = nullabilityHolder;
  }
}
