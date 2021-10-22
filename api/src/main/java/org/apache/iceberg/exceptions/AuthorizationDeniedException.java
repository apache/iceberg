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

/**
 * Exception raised when an attempt to perform an operation fails due to invalid permissions.
 */
public class AuthorizationDeniedException extends RuntimeException {
  public AuthorizationDeniedException(String message) {
    super(message);
  }

  public AuthorizationDeniedException(String message, Object... args) {
    super(String.format(message, args));
  }

  public AuthorizationDeniedException(String message, Throwable cause) {
    super(message, cause);
  }

  public AuthorizationDeniedException(Throwable cause) {
    super(cause);
  }
}
