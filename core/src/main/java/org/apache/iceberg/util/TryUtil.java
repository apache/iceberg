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
package org.apache.iceberg.util;

import java.io.Serializable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Utility for executing code that may throw exceptions and deferring exception handling.
 */
public class TryUtil {
  private TryUtil() {}

  /**
   * Functional interface for operations that may throw exceptions.
   *
   * @param <T> return type of the operation
   */
  @FunctionalInterface
  public interface ThrowingSupplier<T> {
    T get() throws Exception;
  }

  /**
   * Executes the given operation and returns a Try object containing either the result
   * or the exception.
   *
   * @param operation the operation to execute
   * @param <T> the type of the result
   * @return a Try object containing either the result or the exception
   */
  public static <T> Try<T> run(ThrowingSupplier<T> operation) {
    try {
      return Try.success(operation.get());
    } catch (Exception e) {
      return Try.failure(e);
    }
  }

  /**
   * Container for the result of an operation that might throw an exception.
   *
   * @param <T> the type of the result
   */
  public static class Try<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final T value;
    private final Exception exception;

    private Try(T value, Exception exception) {
      this.value = value;
      this.exception = exception;
    }

    /**
     * Creates a successful Try with the given value.
     */
    public static <T> Try<T> success(T value) {
      return new Try<>(value, null);
    }

    /**
     * Creates a failed Try with the given exception.
     */
    public static <T> Try<T> failure(Exception exception) {
      Preconditions.checkNotNull(exception, "Exception cannot be null");
      return new Try<>(null, exception);
    }

    /**
     * Checks if the operation was successful.
     */
    public boolean isSuccess() {
      return exception == null;
    }

    /**
     * Checks if the operation failed.
     */
    public boolean isFailure() {
      return exception != null;
    }

    /**
     * Gets the value if the operation was successful, or throws the original exception if it failed.
     *
     * @return the result value
     * @throws Exception the original exception if the operation failed
     */
    public T get() throws Exception {
      if (exception != null) {
        throw exception;
      }
      return value;
    }

    /**
     * Gets the value if the operation was successful, or returns the provided default value if it failed.
     */
    public T orElse(T defaultValue) {
      return isSuccess() ? value : defaultValue;
    }
  }
}