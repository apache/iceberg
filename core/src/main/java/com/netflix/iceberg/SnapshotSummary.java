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

package com.netflix.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

public class SnapshotSummary {
  public static final String ADDED_FILES_PROP = "added-data-files";
  public static final String DELETED_FILES_PROP = "deleted-data-files";
  public static final String TOTAL_FILES_PROP = "total-data-files";
  public static final String ADDED_RECORDS_PROP = "added-records";
  public static final String DELETED_RECORDS_PROP = "deleted-records";
  public static final String TOTAL_RECORDS_PROP = "total-records";
  public static final String DELETED_DUPLICATE_FILES = "deleted-duplicate-files";
  public static final String CHANGED_PARTITION_COUNT_PROP = "changed-partition-count";

  private SnapshotSummary() {
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    // commit summary tracking
    private Set<String> changedPartitions = Sets.newHashSet();
    private long addedFiles = 0L;
    private long deletedFiles = 0L;
    private long deletedDuplicateFiles = 0L;
    private long addedRecords = 0L;
    private long deletedRecords = 0L;

    public void clear() {
      changedPartitions.clear();
      this.addedFiles = 0L;
      this.deletedFiles = 0L;
      this.deletedDuplicateFiles = 0L;
      this.addedRecords = 0L;
      this.deletedRecords = 0L;
    }

    public void incrementDuplicateDeletes() {
      this.deletedDuplicateFiles += 1;
    }

    public void deletedFile(PartitionSpec spec, DataFile file) {
      changedPartitions.add(spec.partitionToPath(file.partition()));
      this.deletedFiles += 1;
      this.deletedRecords += file.recordCount();
    }

    public void addedFile(PartitionSpec spec, DataFile file) {
      changedPartitions.add(spec.partitionToPath(file.partition()));
      this.addedFiles += 1;
      this.addedRecords += file.recordCount();
    }

    public Map<String, String> build() {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      setIf(addedFiles > 0, builder, ADDED_FILES_PROP, addedFiles);
      setIf(deletedFiles > 0, builder, DELETED_FILES_PROP, deletedFiles);
      setIf(deletedDuplicateFiles > 0, builder, DELETED_DUPLICATE_FILES, deletedDuplicateFiles);
      setIf(addedRecords > 0, builder, ADDED_RECORDS_PROP, addedRecords);
      setIf(deletedRecords > 0, builder, DELETED_RECORDS_PROP, deletedRecords);
      setIf(true, builder, CHANGED_PARTITION_COUNT_PROP, changedPartitions.size());

      return builder.build();
    }

    private static void setIf(boolean expression, ImmutableMap.Builder<String, String> builder,
                              String property, Object value) {
      if (expression) {
        builder.put(property, String.valueOf(value));
      }
    }
  }
}
