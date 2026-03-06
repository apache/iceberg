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

import com.google.errorprone.annotations.FormatMethod;
import org.apache.iceberg.exceptions.ValidationException;

/**
 * A {@link ValidationException} that indicates a validation failure that can be fixed and retried.
 *
 * <p>This is specifically not a conflict. This is used when a validation failed because the commit
 * includes stale values, such as a sequence number or first-row-id that is behind the current table
 * state. Retrying the commit with refreshed metadata can resolve the failure.
 */
public class RetryableValidationException extends ValidationException {
  @FormatMethod
  public RetryableValidationException(String message, Object... args) {
    super(message, args);
  }

  @FormatMethod
  public RetryableValidationException(Throwable cause, String message, Object... args) {
    super(cause, message, args);
  }

  @FormatMethod
  public static void check(boolean test, String message, Object... args) {
    if (!test) {
      throw new RetryableValidationException(message, args);
    }
  }
}
