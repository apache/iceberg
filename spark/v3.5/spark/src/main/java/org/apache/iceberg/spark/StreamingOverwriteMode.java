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
package org.apache.iceberg.spark;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Enumerates the modes for handling OVERWRITE snapshots during streaming reads. */
public enum StreamingOverwriteMode {
  /** Throw an error when an OVERWRITE snapshot is encountered (default behavior). */
  FAIL("fail"),

  /** Skip OVERWRITE snapshots entirely. */
  SKIP("skip"),

  /** Process only the added files from OVERWRITE snapshots. */
  ADDED_FILES_ONLY("added-files-only");

  private final String modeName;

  StreamingOverwriteMode(String modeName) {
    this.modeName = modeName;
  }

  public String modeName() {
    return modeName;
  }

  public static StreamingOverwriteMode fromName(String modeName) {
    Preconditions.checkArgument(modeName != null, "Mode name is null");

    if (FAIL.modeName().equalsIgnoreCase(modeName)) {
      return FAIL;
    } else if (SKIP.modeName().equalsIgnoreCase(modeName)) {
      return SKIP;
    } else if (ADDED_FILES_ONLY.modeName().equalsIgnoreCase(modeName)) {
      return ADDED_FILES_ONLY;
    } else {
      throw new IllegalArgumentException("Unknown streaming overwrite mode: " + modeName);
    }
  }
}
