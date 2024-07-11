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
package org.apache.iceberg;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;

/**
 * A struct of flattened transformed values.
 *
 * <p>Instances of this class can produce transformed values from a row passed to {@link
 * #wrap(StructLike)}.
 */
class StructTransform implements StructLike, Serializable {

  private final int size;
  private final Accessor<StructLike>[] accessors;

  @SuppressWarnings("rawtypes")
  private final SerializableFunction[] transforms;

  private final Schema resultSchema;

  private final Object[] transformedTuple;

  @SuppressWarnings("unchecked")
  StructTransform(Schema schema, List<FieldTransform> fieldTransforms) {
    Preconditions.checkArgument(fieldTransforms != null, "Invalid field transform list: null");

    this.size = fieldTransforms.size();
    this.accessors = (Accessor<StructLike>[]) Array.newInstance(Accessor.class, size);
    this.transforms = new SerializableFunction[size];

    List<Types.NestedField> transformedFields = Lists.newArrayListWithCapacity(size);
    for (int i = 0; i < size; ++i) {
      int sourceFieldId = fieldTransforms.get(i).sourceFieldId();
      Transform<?, ?> transform = fieldTransforms.get(i).transform();
      Types.NestedField sourceField = schema.findField(sourceFieldId);
      Preconditions.checkArgument(
          sourceField != null, "Cannot find source field: %s", sourceFieldId);
      Accessor<StructLike> accessor = schema.accessorForField(sourceFieldId);
      Preconditions.checkArgument(
          accessor != null, "Cannot build accessor for field: %s", sourceField);
      this.accessors[i] = accessor;
      this.transforms[i] = transform.bind(accessor.type());
      Type transformedType = transform.getResultType(sourceField.type());
      // There could be multiple transformations on the same source column,
      // like in the PartitionKey case. To resolve the collision,
      // field id is set to transform index and
      // field name is set to sourceFieldName_transformIndex
      Types.NestedField transformedField =
          Types.NestedField.of(
              i,
              sourceField.isOptional(),
              sourceField.name() + '_' + i,
              transformedType,
              sourceField.doc());
      transformedFields.add(transformedField);
    }

    this.resultSchema = new Schema(transformedFields);
    this.transformedTuple = new Object[size];
  }

  StructTransform(StructTransform toCopy) {
    this.size = toCopy.size;
    this.accessors = toCopy.accessors;
    this.transforms = toCopy.transforms;
    this.resultSchema = toCopy.resultSchema;

    this.transformedTuple = new Object[size];
    System.arraycopy(toCopy.transformedTuple, 0, this.transformedTuple, 0, size);
  }

  public void wrap(StructLike row) {
    for (int i = 0; i < transformedTuple.length; i += 1) {
      @SuppressWarnings("unchecked")
      Function<Object, Object> transform = transforms[i];
      transformedTuple[i] = transform.apply(accessors[i].get(row));
    }
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(transformedTuple[pos]);
  }

  @Override
  public <T> void set(int pos, T value) {
    transformedTuple[pos] = value;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < transformedTuple.length; i += 1) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(transformedTuple[i]);
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof StructTransform)) {
      return false;
    }

    StructTransform that = (StructTransform) o;
    return Arrays.equals(transformedTuple, that.transformedTuple);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(transformedTuple);
  }

  /** Return the schema of transformed result (a flattened structure) */
  public Schema resultSchema() {
    return resultSchema;
  }

  /**
   * Simple POJO for source field id and transform function. {@code Pair} class is not usable here
   * in API module, as it has an Avro dep and is in the core module.
   */
  static class FieldTransform {
    private final int sourceFieldId;
    private final Transform<?, ?> transform;

    FieldTransform(int sourceFieldId, Transform<?, ?> transform) {
      this.sourceFieldId = sourceFieldId;
      this.transform = transform;
    }

    int sourceFieldId() {
      return sourceFieldId;
    }

    Transform<?, ?> transform() {
      return transform;
    }
  }
}
