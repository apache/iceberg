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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

/**
 * Hashes values with SHA-256 using a per-query salt. Binding requires the salt via {@link
 * #bind(Type, byte[])}; the no-salt variant fails fast so callers can't accidentally strip the
 * query-local randomness.
 */
public final class Sha256QueryLocal extends IcebergFunction.BaseFunction<Object, Object>
    implements SaltedFunction<Object, Object> {
  public Sha256QueryLocal(int fieldId) {
    super(fieldId);
  }

  @Override
  public String name() {
    return SHA_256_QUERY_LOCAL;
  }

  @Override
  public boolean canBind(Type type) {
    return Sha256.isSupported(type);
  }

  @Override
  public SerializableFunction<Object, Object> bind(Type type) {
    throw new IllegalArgumentException(
        "sha-256-query-local requires a salt; call bind(Type, byte[]) instead");
  }

  @Override
  public SerializableFunction<Object, Object> bind(Type type, byte[] salt) {
    Preconditions.checkArgument(canBind(type), "sha-256 is not supported for type: %s", type);
    Preconditions.checkArgument(
        salt != null && salt.length >= 16, "sha-256-query-local salt must be >= 16 bytes");
    return Sha256.forType(type, salt);
  }
}
