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
 * Preserves a function with a discriminator string this client doesn't recognize so that newer
 * server-side function types don't break parsing. Callers that intend to enforce the function
 * (engine-side rules) must fail closed when they encounter this — silent skipping would leak
 * unmasked data.
 */
public final class UnknownFunction extends IcebergFunction.BaseFunction<Object, Object> {
  private final String functionName;

  public UnknownFunction(int fieldId, String functionName) {
    super(fieldId);
    Preconditions.checkArgument(functionName != null, "Invalid function name: null");
    this.functionName = functionName;
  }

  @Override
  public String name() {
    return functionName;
  }

  @Override
  public boolean canBind(Type type) {
    return false;
  }

  @Override
  public SerializableFunction<Object, Object> bind(Type type) {
    throw new IllegalArgumentException(
        "Cannot bind unknown function '"
            + functionName
            + "': this client does not recognize the function. Upgrade the client or remove the "
            + "function from the server-side policy.");
  }
}
