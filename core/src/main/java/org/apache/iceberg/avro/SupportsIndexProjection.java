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
package org.apache.iceberg.avro;

import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;

public abstract class SupportsIndexProjection implements StructLike, Serializable {
  private final int[] fromProjectionPos;

  /** Noop constructor that does not project fields */
  protected SupportsIndexProjection(int size) {
    this.fromProjectionPos = new int[size];
    for (int i = 0; i < fromProjectionPos.length; i++) {
      fromProjectionPos[i] = i;
    }
  }

  /** Base constructor for building the type mapping */
  protected SupportsIndexProjection(Types.StructType baseType, Types.StructType projectionType) {
    List<Types.NestedField> allFields = baseType.fields();
    List<Types.NestedField> fields = projectionType.fields();

    this.fromProjectionPos = new int[fields.size()];
    for (int i = 0; i < fromProjectionPos.length; i += 1) {
      boolean found = false;
      for (int j = 0; j < allFields.size(); j += 1) {
        if (fields.get(i).fieldId() == allFields.get(j).fieldId()) {
          found = true;
          fromProjectionPos[i] = j;
        }
      }

      if (!found) {
        throw new IllegalArgumentException("Cannot find projected field: " + fields.get(i));
      }
    }
  }

  /** Copy constructor */
  protected SupportsIndexProjection(SupportsIndexProjection toCopy) {
    this.fromProjectionPos = toCopy.fromProjectionPos;
  }

  protected abstract <T> T internalGet(int pos, Class<T> javaClass);

  protected abstract <T> void internalSet(int pos, T value);

  private int pos(int basePos) {
    return fromProjectionPos[basePos];
  }

  @Override
  public int size() {
    return fromProjectionPos.length;
  }

  @Override
  public <T> T get(int basePos, Class<T> javaClass) {
    return internalGet(pos(basePos), javaClass);
  }

  @Override
  public <T> void set(int basePos, T value) {
    internalSet(pos(basePos), value);
  }
}
