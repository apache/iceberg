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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

/** Hashes values with SHA-256 using a fixed (unsalted) digest. Output is deterministic. */
final class Sha256Global<T> implements IcebergFunction<T, T> {
  static final String NAME = "sha_256_global";

  private static final Sha256Global<?> INSTANCE = new Sha256Global<>();

  @SuppressWarnings("unchecked")
  static <T> Sha256Global<T> get() {
    return (Sha256Global<T>) INSTANCE;
  }

  private Sha256Global() {}

  @Override
  public String toString() {
    return NAME;
  }

  Object writeReplace() throws ObjectStreamException {
    return SerializationProxies.Sha256GlobalProxy.get();
  }

  @Override
  public boolean canBind(Type type) {
    return Sha256.isSupported(type);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SerializableFunction<T, T> bind(Type type) {
    Preconditions.checkArgument(canBind(type), "sha-256 is not supported for type: %s", type);
    return (SerializableFunction<T, T>) Sha256.forType(type, null);
  }
}
