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
package org.apache.iceberg.io;

import org.apache.iceberg.StructLike;

/** Copy the StructLike's values into a new one. It does not handle list or map values now. */
class StructCopy implements StructLike {
  static StructLike copy(StructLike struct) {
    return struct != null ? new StructCopy(struct) : null;
  }

  private final Object[] values;

  private StructCopy(StructLike toCopy) {
    this.values = new Object[toCopy.size()];

    for (int i = 0; i < values.length; i += 1) {
      Object value = toCopy.get(i, Object.class);

      if (value instanceof StructLike) {
        values[i] = copy((StructLike) value);
      } else {
        values[i] = value;
      }
    }
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(values[pos]);
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException("Struct copy cannot be modified");
  }
}
