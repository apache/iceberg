package org.apache.iceberg.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;

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
  public int isSet(int index) {
    return nullabilityHolder.isNullAt(index) ? 0 : 1;
  }

  public void setNullabilityHolder(NullabilityHolder nullabilityHolder) {
    this.nullabilityHolder = nullabilityHolder;
  }
}
