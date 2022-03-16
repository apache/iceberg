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

import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;

/**
 * Container class for holding the Arrow vector storing a batch of values along with other state
 * needed for reading values out of it.
 */
public class VectorHolder {
  private final ColumnDescriptor columnDescriptor;
  private final FieldVector vector;
  private final boolean isDictionaryEncoded;
  private final Dictionary dictionary;
  private final NullabilityHolder nullabilityHolder;
  private final Type physicalType;
  private final Type logicalType;

  public VectorHolder(
      ColumnDescriptor columnDescriptor,
      FieldVector vector,
      boolean isDictionaryEncoded,
      Dictionary dictionary,
      NullabilityHolder holder,
      Type type) {
    // All the fields except dictionary are not nullable unless it is a dummy holder
    Preconditions.checkNotNull(columnDescriptor, "ColumnDescriptor cannot be null");
    Preconditions.checkNotNull(vector, "Vector cannot be null");
    Preconditions.checkNotNull(holder, "NullabilityHolder cannot be null");
    Preconditions.checkNotNull(type, "IcebergType cannot be null");
    this.columnDescriptor = columnDescriptor;
    this.vector = vector;
    this.isDictionaryEncoded = isDictionaryEncoded;
    this.dictionary = dictionary;
    this.nullabilityHolder = holder;
    this.physicalType = type;
    this.logicalType = type;
  }

  public VectorHolder(
      ColumnDescriptor columnDescriptor, FieldVector vector, boolean isDictionaryEncoded,
      Dictionary dictionary, NullabilityHolder holder, Type type, Type logicalType) {
    // All the fields except dictionary are not nullable unless it is a dummy holder
    Preconditions.checkNotNull(columnDescriptor, "ColumnDescriptor cannot be null");
    Preconditions.checkNotNull(vector, "Vector cannot be null");
    Preconditions.checkNotNull(holder, "NullabilityHolder cannot be null");
    Preconditions.checkNotNull(type, "IcebergType cannot be null");
    Preconditions.checkNotNull(logicalType, "LogicalType cannot be null");
    this.columnDescriptor = columnDescriptor;
    this.vector = vector;
    this.isDictionaryEncoded = isDictionaryEncoded;
    this.dictionary = dictionary;
    this.nullabilityHolder = holder;
    this.physicalType = type;
    this.logicalType = logicalType;
  }

  // Only used for returning dummy holder
  private VectorHolder() {
    columnDescriptor = null;
    vector = null;
    isDictionaryEncoded = false;
    dictionary = null;
    nullabilityHolder = null;
    physicalType = null;
    logicalType = null;
  }

  private VectorHolder(FieldVector vec, Type type, NullabilityHolder nulls) {
    columnDescriptor = null;
    vector = vec;
    isDictionaryEncoded = false;
    dictionary = null;
    nullabilityHolder = nulls;
    physicalType = type;
    logicalType = type;
  }

  public ColumnDescriptor descriptor() {
    return columnDescriptor;
  }

  public FieldVector vector() {
    return vector;
  }

  public boolean isDictionaryEncoded() {
    return isDictionaryEncoded;
  }

  public Dictionary dictionary() {
    return dictionary;
  }

  public NullabilityHolder nullabilityHolder() {
    return nullabilityHolder;
  }

  public Type physicalType() {
    return physicalType;
  }

  public Type logicalType() {
    return logicalType;
  }

  public int numValues() {
    return vector.getValueCount();
  }

  public static <T> VectorHolder constantHolder(int numRows, T constantValue) {
    return new ConstantVectorHolder(numRows, constantValue);
  }

  public static VectorHolder deletedVectorHolder(int numRows) {
    return new DeletedVectorHolder(numRows);
  }

  public static VectorHolder dummyHolder(int numRows) {
    return new ConstantVectorHolder(numRows);
  }

  public boolean isDummy() {
    return vector == null;
  }

  /**
   * A Vector Holder which does not actually produce values, consumers of this class should use the
   * constantValue to populate their ColumnVector implementation.
   */
  public static class ConstantVectorHolder<T> extends VectorHolder {
    private final T constantValue;
    private final int numRows;

    public ConstantVectorHolder(int numRows) {
      this.numRows = numRows;
      this.constantValue = null;
    }

    public ConstantVectorHolder(int numRows, T constantValue) {
      this.numRows = numRows;
      this.constantValue = constantValue;
    }

    @Override
    public int numValues() {
      return this.numRows;
    }

    public Object getConstant() {
      return constantValue;
    }
  }

  public static class PositionVectorHolder extends VectorHolder {
    public PositionVectorHolder(FieldVector vector, Type type, NullabilityHolder nulls) {
      super(vector, type, nulls);
    }
  }

  public static class DeletedVectorHolder extends VectorHolder {
    private final int numRows;

    public DeletedVectorHolder(int numRows) {
      this.numRows = numRows;
    }

    @Override
    public int numValues() {
      return numRows;
    }
  }
}
