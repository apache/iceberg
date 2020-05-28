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

package org.apache.iceberg.flink.connector.sink;

import java.util.Arrays;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.transforms.Transform;

@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class AbstractPartitioner<T> implements Partitioner<T> {

  protected final PartitionSpec spec;
  protected final int size;
  protected final Transform[] transforms;
  protected final Object[] partitionTuple;

  AbstractPartitioner(PartitionSpec spec) {
    this.spec = spec;
    this.size = spec.fields().size();
    this.transforms = new Transform[size];
    this.partitionTuple = new Object[size];
    for (int i = 0; i < size; i += 1) {
      PartitionField field = spec.fields().get(i);
      this.transforms[i] = field.transform();
    }
  }

  AbstractPartitioner(AbstractPartitioner toCopy) {
    this.spec = toCopy.spec;
    this.size = toCopy.size;
    this.transforms = toCopy.transforms;
    this.partitionTuple = new Object[toCopy.partitionTuple.length];
    for (int i = 0; i < partitionTuple.length; i += 1) {
      this.partitionTuple[i] = toCopy.partitionTuple[i];
    }
  }

  @Override
  public String toString() {
    return Joiner.on(", ").join(partitionTuple);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T get(int pos, Class<T> javaClass) {
    if (null == partitionTuple[pos]) {
      throw new IllegalArgumentException("partition column not found in data: pos = " + pos);
    }
    if (CharSequence.class.isAssignableFrom(javaClass)) {
      return javaClass.cast(partitionTuple[pos].toString());
    }
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
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractPartitioner that = (AbstractPartitioner) o;
    return Arrays.equals(partitionTuple, that.partitionTuple);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(partitionTuple);
  }

  @Override
  public String toPath() {
    return spec.partitionToPath(this);
  }
}
