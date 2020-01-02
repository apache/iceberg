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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;

/**
 * Extension of Arrow's @{@link VarBinaryVector}. The whole reason of having this implementation is to override the
 * expensive {@link VarBinaryVector#isSet(int)} method.
 */
public class IcebergVarBinaryArrowVector extends VarBinaryVector {
  private NullabilityHolder nullabilityHolder;

  public IcebergVarBinaryArrowVector(
      String name,
      BufferAllocator allocator) {
    super(name, allocator);
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
