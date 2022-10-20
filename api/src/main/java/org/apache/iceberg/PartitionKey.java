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
import org.apache.iceberg.util.SerializableFunction;

/**
 * A struct of partition values.
 *
 * <p>Instances of this class can produce partition values from a data row passed to {@link
 * #partition(StructLike)}.
 */
public class PartitionKey implements StructLike, Serializable {

  private final PartitionSpec spec;
  private final int size;
  private final Object[] partitionTuple;
  private final SerializableFunction[] transforms;
  private final Accessor<StructLike>[] accessors;

  @SuppressWarnings("unchecked")
  public PartitionKey(PartitionSpec spec, Schema inputSchema) {
    this.spec = spec;

    List<PartitionField> fields = spec.fields();
    this.size = fields.size();
    this.partitionTuple = new Object[size];
    this.transforms = new SerializableFunction[size];
    this.accessors = (Accessor<StructLike>[]) Array.newInstance(Accessor.class, size);

    Schema schema = spec.schema();
    for (int i = 0; i < size; i += 1) {
      PartitionField field = fields.get(i);
      Accessor<StructLike> accessor = inputSchema.accessorForField(field.sourceId());
      Preconditions.checkArgument(
          accessor != null,
          "Cannot build accessor for field: " + schema.findField(field.sourceId()));
      this.accessors[i] = accessor;
      this.transforms[i] = field.transform().bind(accessor.type());
    }
  }

  private PartitionKey(PartitionKey toCopy) {
    this.spec = toCopy.spec;
    this.size = toCopy.size;
    this.partitionTuple = new Object[toCopy.partitionTuple.length];
    this.transforms = toCopy.transforms;
    this.accessors = toCopy.accessors;

    System.arraycopy(toCopy.partitionTuple, 0, this.partitionTuple, 0, partitionTuple.length);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < partitionTuple.length; i += 1) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(partitionTuple[i]);
    }
    sb.append("]");
    return sb.toString();
  }

  public PartitionKey copy() {
    return new PartitionKey(this);
  }

  public String toPath() {
    return spec.partitionToPath(this);
  }

  /**
   * Replace this key's partition values with the partition values for the row.
   *
   * @param row a StructLike row
   */
  @SuppressWarnings("unchecked")
  public void partition(StructLike row) {
    for (int i = 0; i < partitionTuple.length; i += 1) {
      Function<Object, Object> transform = transforms[i];
      partitionTuple[i] = transform.apply(accessors[i].get(row));
    }
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(partitionTuple[pos]);
  }

  @Override
  public <T> void set(int pos, T value) {
    partitionTuple[pos] = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof PartitionKey)) {
      return false;
    }

    PartitionKey that = (PartitionKey) o;
    return Arrays.equals(partitionTuple, that.partitionTuple);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(partitionTuple);
  }
}
