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

import org.apache.iceberg.Accessor;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class BoundReference<T> implements BoundTerm<T>, Reference<T> {
  private final Types.NestedField field;
  private final Accessor<StructLike> accessor;
  private final String name;

  BoundReference(Types.NestedField field, Accessor<StructLike> accessor, String name) {
    this.field = field;
    this.accessor = accessor;
    this.name = name;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T eval(StructLike struct) {
    return (T) accessor.get(struct);
  }

  public Types.NestedField field() {
    return field;
  }

  @Override
  public BoundReference<T> ref() {
    return this;
  }

  @Override
  public Type type() {
    return field.type();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean isEquivalentTo(BoundTerm<?> other) {
    if (other instanceof BoundReference) {
      Types.NestedField otherField = ((BoundReference<?>) other).field();
      // equivalence only depends on the field ID, type, and optional. name and accessor are ignored
      return field.fieldId() == otherField.fieldId()
          && field.type().equals(otherField.type())
          && field.isOptional() == otherField.isOptional();
    }

    return other.isEquivalentTo(this);
  }

  public int fieldId() {
    return field.fieldId();
  }

  public Accessor<StructLike> accessor() {
    return accessor;
  }

  @Override
  public String toString() {
    return String.format("ref(id=%d, accessor-type=%s)", field.fieldId(), accessor.type());
  }
}
