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
package org.apache.iceberg.actions;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;

/** Result of the data file rewrite planning. */
public class RewriteFilePlan
    extends FileRewritePlan<
        RewriteDataFiles.FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> {
  private final int outputSpecId;

  public RewriteFilePlan(
      Stream<RewriteFileGroup> groups,
      int totalGroupCount,
      Map<StructLike, Integer> groupsInPartition,
      long writeMaxFileSize,
      int outputSpecId) {
    super(groups, totalGroupCount, groupsInPartition, writeMaxFileSize);
    this.outputSpecId = outputSpecId;
  }

  /** Partition specification id for the target files */
  public int outputSpecId() {
    return outputSpecId;
  }
}
