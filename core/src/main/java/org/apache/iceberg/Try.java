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
package org.apache.iceberg;

import java.io.Serializable;
import org.apache.iceberg.util.SerializableSupplier;

/**
 * Container for the result of an operation that might throw an exception.
 *
 * @param <T> the type of the result
 */
class Try<T> implements Serializable {
  private static final long serialVersionUID = 1L;

  private final T value;
  private final Throwable exception;

  private Try(T value, Throwable exception) {
    this.value = value;
    this.exception = exception;
  }

  /**
   * Executes the given operation and returns a Try object containing either the result or the
   * exception.
   *
   * @param supplier the operation to execute
   * @param <T> the type of the result
   * @return a Try object containing either the result or the exception
   */
  static <T> Try<T> of(SerializableSupplier<T> supplier) {
    try {
      return new Try<>(supplier.get(), null);
    } catch (Throwable e) {
      return new Try<>(null, e);
    }
  }

  /** Returns the value if present or throws the original exception if the operation failed. */
  T getOrThrow() {
    if (exception != null) {
      sneakyThrow(exception);
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
    throw (E) e;
  }
}
