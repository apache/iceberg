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
package org.apache.iceberg.deletes;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * An enum that represents the granularity of deletes.
 *
 * <p>Under partition granularity, delete writers are directed to group deletes for different data
 * files into one delete file. This strategy tends to reduce the total number of delete files in the
 * table. However, a scan for a single data file will require reading delete information for
 * multiple data files even if those other files are not required for the scan. All irrelevant
 * deletes will be discarded by readers but reading this extra information will cause overhead. The
 * overhead can potentially be mitigated via delete file caching.
 *
 * <p>Under file granularity, delete writers always organize deletes by their target data file,
 * creating separate delete files for each referenced data file. This strategy ensures the job
 * planning does not assign irrelevant deletes to data files and readers only load necessary delete
 * information. However, it also increases the total number of delete files in the table and may
 * require a more aggressive approach for delete file compaction.
 *
 * <p>Currently, this configuration is only applicable to position deletes.
 *
 * <p>Each granularity has its own benefits and drawbacks and should be picked based on a use case.
 * Regular delete compaction is still required regardless of which granularity is chosen. It is also
 * possible to use one granularity for ingestion and another one for table maintenance.
 */
public enum DeleteGranularity {
  FILE,
  PARTITION;

  @Override
  public String toString() {
    switch (this) {
      case FILE:
        return "file";
      case PARTITION:
        return "partition";
      default:
        throw new IllegalArgumentException("Unknown delete granularity: " + this);
    }
  }

  public static DeleteGranularity fromString(String valueAsString) {
    Preconditions.checkArgument(valueAsString != null, "Value is null");
    if (FILE.toString().equalsIgnoreCase(valueAsString)) {
      return FILE;
    } else if (PARTITION.toString().equalsIgnoreCase(valueAsString)) {
      return PARTITION;
    } else {
      throw new IllegalArgumentException("Unknown delete granularity: " + valueAsString);
    }
  }
}
