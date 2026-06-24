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
package org.apache.iceberg.io;

import javax.annotation.Nullable;

/**
 * A single per-file failure reported to a {@link FailureHandler} during a bulk file operation.
 *
 * <p>The {@link #category()} is the FileIO's best-effort classification of the error. The {@link
 * #rawErrorCode()} and {@link #cause()} are provided so callers that disagree with the
 * categorization can re-classify based on the underlying evidence.
 */
public class FileFailure {
  private final String path;
  private final FailureCategory category;
  private final String rawErrorCode;
  private final Throwable cause;

  public FileFailure(
      String path,
      FailureCategory category,
      @Nullable String rawErrorCode,
      @Nullable Throwable cause) {
    this.path = path;
    this.category = category;
    this.rawErrorCode = rawErrorCode;
    this.cause = cause;
  }

  public String path() {
    return path;
  }

  public FailureCategory category() {
    return category;
  }

  /** Cloud-native error code (e.g. S3 "AccessDenied"), or null if unavailable. */
  @Nullable
  public String rawErrorCode() {
    return rawErrorCode;
  }

  /** Underlying exception, or null when the failure came from a partial-batch error response. */
  @Nullable
  public Throwable cause() {
    return cause;
  }
}
