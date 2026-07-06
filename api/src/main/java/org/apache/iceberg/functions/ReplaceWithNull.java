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

import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

/** Returns null for every non-null input. Works for any type. */
public final class ReplaceWithNull extends IcebergFunction.BaseFunction<Object, Object> {
  public ReplaceWithNull(int fieldId) {
    super(fieldId);
  }

  @Override
  public String name() {
    return REPLACE_WITH_NULL;
  }

  @Override
  public boolean canBind(Type type) {
    return true;
  }

  @Override
  public SerializableFunction<Object, Object> bind(Type type) {
    return ReplaceWithNullFn.INSTANCE;
  }

  private static final class ReplaceWithNullFn implements SerializableFunction<Object, Object> {
    static final ReplaceWithNullFn INSTANCE = new ReplaceWithNullFn();

    @Override
    public Object apply(Object value) {
      return null;
    }
  }
}
