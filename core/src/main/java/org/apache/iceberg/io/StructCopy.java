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

import java.io.Serializable;
import java.lang.reflect.Array;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.util.SerializationUtil;

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
      if (value != null) {
        values[i] = copyValue(value);
      }
    }
  }

  private static Object copyValue(Object value) {
    if (value instanceof StructLike) {
      return copy((StructLike) value);
    } else if (value.getClass().isPrimitive() || (value instanceof String)) {
      return value; // As value is immutable.
    } else if (value.getClass().isArray()) {
      int length = Array.getLength(value);
      final Class<?> componentType = value.getClass().getComponentType();
      if (componentType != null && componentType.isPrimitive()) {
        Object copy = Array.newInstance(componentType, length);
        System.arraycopy(value, 0, copy, 0, length);
      } else {
        final Object copy;
        if (componentType != null) {
          copy = Array.newInstance(componentType, length);
        } else {
          copy = new Object[length];
        }
        for (int i = 0; i < length; i++) {
          Array.set(copy, i, Array.get(value, i));
        }
        return copy;
      }
    } else if (value instanceof Serializable) {
      // build a copy
      byte[] bytes = SerializationUtil.serializeToBytes(value);
      return SerializationUtil.deserializeFromBytes(bytes);
    }
    return value; // other cases
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
