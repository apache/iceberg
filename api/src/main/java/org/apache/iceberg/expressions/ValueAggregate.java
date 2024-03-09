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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;

class ValueAggregate<T> extends BoundAggregate<T, T> {
  private final SingleValueStruct valueStruct = new SingleValueStruct();

  protected ValueAggregate(Operation op, BoundTerm<T> term) {
    super(op, term);
  }

  @Override
  public T eval(StructLike struct) {
    return term().eval(struct);
  }

  @Override
  public T eval(DataFile file) {
    valueStruct.setValue(evaluateRef(file));
    return term().eval(valueStruct);
  }

  protected Object evaluateRef(DataFile file) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement eval(DataFile)");
  }

  /** Used to pass a referenced value through term evaluation. */
  private static class SingleValueStruct implements StructLike {
    private Object value;

    private void setValue(Object value) {
      this.value = value;
    }

    @Override
    public int size() {
      return 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      if (javaClass.isAssignableFrom(StructLike.class)) {
        return (T) this;
      } else {
        return (T) value;
      }
    }

    @Override
    public <T> void set(int pos, T value1) {
      throw new UnsupportedOperationException("Cannot update a read-only struct");
    }
  }
}
