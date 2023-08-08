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
package org.apache.iceberg.flink.source.split;

import java.util.Collection;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;

public class IcebergSourceChangeLogSplit extends IcebergSourceSplit {

  protected IcebergSourceChangeLogSplit(
      ScanTaskGroup<ChangelogScanTask> task, int fileOffset, long recordOffset) {
    super(task, fileOffset, recordOffset);
  }

  @Override
  public String splitId() {
    return task().toString();
  }

  @Override
  protected String toString(Collection<? extends ScanTask> files) {
    return files.toString();
  }

  @Override
  @SuppressWarnings("unchecked")
  public ScanTaskGroup<ChangelogScanTask> task() {
    return (ScanTaskGroup<ChangelogScanTask>) super.task();
  }
}
