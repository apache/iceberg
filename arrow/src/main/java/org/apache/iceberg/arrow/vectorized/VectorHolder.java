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
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;

/**
 * Container class for holding the Arrow vector holding a batch of values along with other state needed for reading
 * values out of it.
 */
public class VectorHolder {
  private final ColumnDescriptor columnDescriptor;
  private final FieldVector vector;
  private final boolean isDictionaryEncoded;

  private final Dictionary dictionary;
  private final NullabilityHolder nullabilityHolder;

  public static final VectorHolder NULL_VECTOR_HOLDER = new VectorHolder(null, null, false, null, null);

  public VectorHolder(
      ColumnDescriptor columnDescriptor,
      FieldVector vector,
      boolean isDictionaryEncoded,
      Dictionary dictionary,
      NullabilityHolder holder) {
    this.columnDescriptor = columnDescriptor;
    this.vector = vector;
    this.isDictionaryEncoded = isDictionaryEncoded;
    this.dictionary = dictionary;
    this.nullabilityHolder = holder;
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
}
