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
package org.apache.iceberg.parquet;

/**
 * Exception class for handling both IOException and general Exception cases when working with Comet
 * FileReader operations. This provides a unified way to handle exceptions that may occur during
 * reflection-based calls to Comet classes.
 */
public class CometIOException extends RuntimeException {

  public CometIOException(String message, Throwable cause) {
    super(message, cause);
  }

  /** Creates a CometIOException with a custom message and cause. */
  public static CometIOException fromException(String message, Exception cause) {
    return new CometIOException(message, cause);
  }
}
