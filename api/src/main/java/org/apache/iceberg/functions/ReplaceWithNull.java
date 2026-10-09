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
package org.apache.iceberg.functions;

import java.io.ObjectStreamException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

/**
 * Returns null for every non-null input. Works for any type.
 *
 * <p>Per spec, replace_with_null is only valid for optional (nullable) fields. Callers must
 * validate that the target field is optional before binding; this function cannot check nullability
 * because {@link org.apache.iceberg.types.Type} does not carry the field's required/optional flag.
 */
final class ReplaceWithNull<T> implements IcebergFunction<T, T> {
  static final String NAME = "replace_with_null";

  private static final ReplaceWithNull<?> INSTANCE = new ReplaceWithNull<>();

  @SuppressWarnings("unchecked")
  static <T> ReplaceWithNull<T> get() {
    return (ReplaceWithNull<T>) INSTANCE;
  }

  private ReplaceWithNull() {}

  @Override
  public String toString() {
    return NAME;
  }

  Object writeReplace() throws ObjectStreamException {
    return SerializationProxies.ReplaceWithNullProxy.get();
  }

  @Override
  public boolean canBind(Type type) {
    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SerializableFunction<T, T> bind(Type type) {
    return (SerializableFunction<T, T>) ReplaceWithNullFn.INSTANCE;
  }

  private static final class ReplaceWithNullFn implements SerializableFunction<Object, Object> {
    static final ReplaceWithNullFn INSTANCE = new ReplaceWithNullFn();

    @Override
    public Object apply(Object value) {
      return null;
    }
  }
}
