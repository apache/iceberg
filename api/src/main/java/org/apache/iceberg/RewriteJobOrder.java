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
 * Enum of supported rewrite job order, it defines the order in which the file groups should be
 * written.
 *
 * <p>
 *
 * <ul>
 *   <li>bytes-asc: rewrite the smallest job groups first.
 *   <li>bytes-desc: rewrite the largest job groups first.
 *   <li>files-asc: rewrite the job groups with the least files first.
 *   <li>files-desc: rewrite the job groups with the most files first.
 *   <li>none: rewrite job groups in the order they were planned (no specific ordering).
 * </ul>
 *
 * <p>
 */
public enum RewriteJobOrder {
  BYTES_ASC("bytes-asc"),
  BYTES_DESC("bytes-desc"),
  FILES_ASC("files-asc"),
  FILES_DESC("files-desc"),
  DELETES_DESC("deletes-desc"),
  NONE("none");

  private final String orderName;

  RewriteJobOrder(String orderName) {
    this.orderName = orderName;
  }

  public String orderName() {
    return orderName;
  }

  public static RewriteJobOrder fromName(String orderName) {
    Preconditions.checkArgument(orderName != null, "Invalid rewrite job order name: null");
    // Replace the hyphen in order name with underscore to map to the enum value. For example:
    // bytes-asc to BYTES_ASC
    try {
      return RewriteJobOrder.valueOf(orderName.replaceFirst("-", "_").toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Invalid rewrite job order name: %s", orderName), e);
    }
  }
}
