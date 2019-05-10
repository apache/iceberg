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

package org.apache.iceberg.expressions;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class BoundReference<T> implements Reference {
  private final int fieldId;
  private final Accessor<StructLike> accessor;
  private final int pos;

  BoundReference(Schema schema, int fieldId) {
    this.fieldId = fieldId;

    Map<Integer, Accessor<StructLike>> accessors = lazyIdToAccessor(schema);

    this.accessor = accessors.get(fieldId);

    // only look for top level field position
    this.pos = findTopFieldPos(fieldId, schema.asStruct());

  }

  private int findTopFieldPos(int localFieldId, Types.StructType struct) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      if (fields.get(i).fieldId() == localFieldId) {
        return i;
      }
    }
    return -1;
  }

  public Type type() {
    return accessor.type();
  }

  public int fieldId() {
    return fieldId;
  }

  public int pos() {
    if (pos == -1) {
      throw new ValidationException("Cannot find position for non-primitive field id %d", fieldId);
    }
    return pos;
  }

  @SuppressWarnings("unchecked")
  public T get(StructLike struct) {
    return (T) accessor.get(struct);
  }

  @Override
  public String toString() {
    return String.format("ref(id=%d, accessor=%s)", fieldId, accessor);
  }

  private transient Map<Integer, Accessor<StructLike>> idToAccessor = null;

  private Map<Integer, Accessor<StructLike>> lazyIdToAccessor(Schema schema) {

    if (idToAccessor == null) {
      idToAccessor = TypeUtil.visit(schema, new BuildPositionAccessors());
    }
    return idToAccessor;
  }
}
