package org.apache.iceberg.parquet;

public class NullabilityVector {
  private final boolean[] isNull;
  private int numNulls;

  public NullabilityVector(int batchSize) {
    this.isNull = new boolean[batchSize];
  }


  public void nullAt(int idx) {
    isNull[idx] =  true;
    numNulls++;
  }

  public boolean isNullAt(int idx) {
    return isNull[idx];
  }

  public boolean hasNulls() {
    return numNulls > 0;
  }

  public int numNulls() {
    return numNulls;
  }
}
