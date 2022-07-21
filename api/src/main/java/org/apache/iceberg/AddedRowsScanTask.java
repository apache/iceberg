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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * A scan task for inserts generated by adding a data file to the table.
 * <p>
 * Note that added data files may have matching delete files. This may happen if a matching position
 * delete file is committed in the same snapshot or if changes for multiple snapshots are squashed together.
 * <p>
 * Suppose snapshot S1 adds data files F1, F2, F3 and a position delete file, D1, that marks particular
 * records in F1 as deleted. A scan for changes generated by S1 should include the following tasks:
 * <ul>
 *   <li>AddedRowsScanTask(file=F1, deletes=[D1], snapshot=S1)</li>
 *   <li>AddedRowsScanTask(file=F2, deletes=[], snapshot=S1)</li>
 *   <li>AddedRowsScanTask(file=F3, deletes=[], snapshot=S1)</li>
 * </ul>
 * <p>
 * Readers consuming these tasks should produce added records with metadata like change ordinal and
 * commit snapshot ID.
 */
public interface AddedRowsScanTask extends ChangelogScanTask, ContentScanTask<DataFile> {
  /**
   * A list of {@link DeleteFile delete files} to apply when reading the data file in this task.
   *
   * @return a list of delete files to apply
   */
  List<DeleteFile> deletes();

  @Override
  default List<DataFile> referencedDataFiles() {
    return file() == null ? ImmutableList.of() : ImmutableList.of(file());
  }

  @Override
  default List<DeleteFile> referencedDeleteFiles() {
    return deletes();
  }

  @Override
  default ChangelogOperation operation() {
    return ChangelogOperation.INSERT;
  }
}
