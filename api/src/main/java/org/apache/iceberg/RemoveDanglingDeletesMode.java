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

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Enum of supported remove dangling deletes mode, it defines the mode in which the dangling delete
 * files shall be pruned
 *
 * <p>
 *
 * <ul>
 *   <li>metadata: dangling delete files will be pruned from iceberg metadata. Pruning apply to both
 *       position delete and equality delete files.
 *   <li>none: pruning is disabled.
 * </ul>
 *
 * <p>
 */
public enum RemoveDanglingDeletesMode {
  METADATA("metadata"),
  NONE("none");

  private final String mode;

  RemoveDanglingDeletesMode(String modeName) {
    this.mode = modeName;
  }

  public String modeName() {
    return mode;
  }

  public static RemoveDanglingDeletesMode fromName(String modeName) {
    String errorMessage = "Invalid remove dangling deletes mode name: %s";
    Preconditions.checkArgument(modeName != null, String.format(errorMessage, "null"));
    try {
      return RemoveDanglingDeletesMode.valueOf(modeName.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format(errorMessage, modeName), e);
    }
  }
}
