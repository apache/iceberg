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

/** Hashes values with SHA-256 using a fixed (unsalted) digest. Output is deterministic. */
public final class Sha256Global extends Action.BaseAction<Object, Object> {
  public Sha256Global(int fieldId) {
    super(fieldId);
  }

  @Override
  public String actionType() {
    return SHA_256_GLOBAL;
  }

  @Override
  public boolean canBind(Type type) {
    return Sha256Fn.isSupported(type);
  }

  @Override
  public SerializableFunction<Object, Object> bind(Type type) {
    Preconditions.checkArgument(canBind(type), "sha-256 is not supported for type: %s", type);
    return Sha256Fn.forType(type, null);
  }
}
