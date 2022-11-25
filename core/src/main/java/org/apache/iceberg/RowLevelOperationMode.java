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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Iceberg supports two ways to modify records in a table: copy-on-write and merge-on-read.
 *
 * <p>In copy-on-write, changes are materialized immediately and matching data files are replaced
 * with new data files that represent the new table state. For example, if there is a record that
 * has to be deleted, the data file that contains this record has to be replaced with another data
 * file without that record. All unchanged rows have to be copied over to the new data file.
 *
 * <p>In merge-on-read, changes aren't materialized immediately. Instead, IDs of deleted and updated
 * records are written into delete files that are applied during reads and updated/inserted records
 * are written into new data files that are committed together with the delete files.
 *
 * <p>Copy-on-write changes tend to consume more time and resources during writes but don't
 * introduce any performance overhead during reads. Merge-on-read operations, on the other hand,
 * tend to be much faster during writes but require more time and resources to apply delete files
 * during reads.
 */
public enum RowLevelOperationMode {
  COPY_ON_WRITE("copy-on-write"),
  MERGE_ON_READ("merge-on-read");

  private final String modeName;

  RowLevelOperationMode(String modeName) {
    this.modeName = modeName;
  }

  public static RowLevelOperationMode fromName(String modeName) {
    Preconditions.checkArgument(modeName != null, "Mode name is null");
    if (COPY_ON_WRITE.modeName().equalsIgnoreCase(modeName)) {
      return COPY_ON_WRITE;
    } else if (MERGE_ON_READ.modeName().equalsIgnoreCase(modeName)) {
      return MERGE_ON_READ;
    } else {
      throw new IllegalArgumentException("Unknown row-level operation mode: " + modeName);
    }
  }

  public String modeName() {
    return modeName;
  }
}
