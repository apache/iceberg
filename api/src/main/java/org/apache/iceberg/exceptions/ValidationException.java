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
package org.apache.iceberg.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/**
 * Exception which is raised when the arguments are valid in isolation, but not in conjunction with
 * other arguments or state, as opposed to {@link IllegalArgumentException} which is raised when an
 * argument value is always invalid.
 *
 * <p>A ValidationException will cause the operation to abort.
 *
 * <p>For example, this is thrown when attempting to create a table with a {@link PartitionSpec}
 * that is not compatible with the table {@link Schema}
 */
public class ValidationException extends RuntimeException implements CleanableFailure {
  @FormatMethod
  public ValidationException(String message, Object... args) {
    super(String.format(message, args));
  }

  @FormatMethod
  public ValidationException(Throwable cause, String message, Object... args) {
    super(String.format(message, args), cause);
  }

  @FormatMethod
  public static void check(boolean test, String message, Object... args) {
    if (!test) {
      throw new ValidationException(message, args);
    }
  }
}
