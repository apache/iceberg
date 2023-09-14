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
import org.apache.iceberg.types.Types;
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
  private final Types.NestedField icebergField;

  public VectorHolder(
      ColumnDescriptor columnDescriptor,
      FieldVector vector,
      boolean isDictionaryEncoded,
      Dictionary dictionary,
      NullabilityHolder holder,
      Types.NestedField icebergField) {
    // All the fields except dictionary are not nullable unless it is a dummy holder
    Preconditions.checkNotNull(columnDescriptor, "ColumnDescriptor cannot be null");
    Preconditions.checkNotNull(vector, "Vector cannot be null");
    Preconditions.checkNotNull(holder, "NullabilityHolder cannot be null");
    Preconditions.checkNotNull(icebergField, "IcebergField cannot be null");
    this.columnDescriptor = columnDescriptor;
    this.vector = vector;
    this.isDictionaryEncoded = isDictionaryEncoded;
    this.dictionary = dictionary;
    this.nullabilityHolder = holder;
    this.icebergField = icebergField;
  }

  /** A constructor used for dummy holders. */
  private VectorHolder() {
    this(null);
  }

  /** A constructor used for typed constant holders. */
  private VectorHolder(Types.NestedField field) {
    columnDescriptor = null;
    vector = null;
    isDictionaryEncoded = false;
    dictionary = null;
    nullabilityHolder = null;
    icebergField = field;
  }

  private VectorHolder(FieldVector vec, Types.NestedField field, NullabilityHolder nulls) {
    columnDescriptor = null;
    vector = vec;
    isDictionaryEncoded = false;
    dictionary = null;
    nullabilityHolder = nulls;
    icebergField = field;
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

  public Type icebergType() {
    return icebergField != null ? icebergField.type() : null;
  }

  public Types.NestedField icebergField() {
    return icebergField;
  }

  public int numValues() {
    return vector.getValueCount();
  }

  public static <T> VectorHolder constantHolder(
      Types.NestedField icebergField, int numRows, T constantValue) {
    return new ConstantVectorHolder<>(icebergField, numRows, constantValue);
  }

  /** @deprecated since 1.4.0, will be removed in 1.5.0; use typed constant holders instead. */
  @Deprecated
  public static <T> VectorHolder constantHolder(int numRows, T constantValue) {
    return new ConstantVectorHolder<>(numRows, constantValue);
  }

  public static VectorHolder deletedVectorHolder(int numRows) {
    return new DeletedVectorHolder(numRows);
  }

  public static VectorHolder dummyHolder(int numRows) {
    return new ConstantVectorHolder<>(numRows);
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

    /** @deprecated since 1.4.0, will be removed in 1.5.0; use typed constant holders instead. */
    @Deprecated
    public ConstantVectorHolder(int numRows, T constantValue) {
      this.numRows = numRows;
      this.constantValue = constantValue;
    }

    public ConstantVectorHolder(Types.NestedField icebergField, int numRows, T constantValue) {
      super(icebergField);
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
    public PositionVectorHolder(
        FieldVector vector, Types.NestedField icebergField, NullabilityHolder nulls) {
      super(vector, icebergField, nulls);
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
