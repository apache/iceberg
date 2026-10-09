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

/** Truncates date or timestamp values to the first instant of their year. */
final class TruncateToYear<T> implements IcebergFunction<T, T> {
  static final String NAME = "truncate_to_year";

  private static final TruncateToYear<?> INSTANCE = new TruncateToYear<>();

  @SuppressWarnings("unchecked")
  static <T> TruncateToYear<T> get() {
    return (TruncateToYear<T>) INSTANCE;
  }

  private TruncateToYear() {}

  @Override
  public String toString() {
    return NAME;
  }

  Object writeReplace() throws ObjectStreamException {
    return SerializationProxies.TruncateToYearProxy.get();
  }

  @Override
  public boolean canBind(Type type) {
    switch (type.typeId()) {
      case DATE:
      case TIMESTAMP:
      case TIMESTAMP_NANO:
        return true;
      default:
        return false;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public SerializableFunction<T, T> bind(Type type) {
    Preconditions.checkArgument(
        canBind(type), "truncate_to_year is not supported for type: %s", type);
    return (SerializableFunction<T, T>) TruncateTemporal.forType(TruncateTemporal.Unit.YEAR, type);
  }
}
