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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class InitialSnapshotPlan {
  private final long snapshotId;
  private final List<FileScanTask> files;

  private InitialSnapshotPlan(long snapshotId, List<FileScanTask> files) {
    this.snapshotId = snapshotId;
    this.files = files;
  }

  static InitialSnapshotPlan forSnapshot(Table table, Snapshot snapshot) {
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
      return new InitialSnapshotPlan(snapshot.snapshotId(), Lists.newArrayList(tasks));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  long snapshotId() {
    return snapshotId;
  }

  int size() {
    return files.size();
  }

  FileScanTask get(int index) {
    return files.get(index);
  }

  List<FileScanTask> slice(int fromIndex, int toIndex) {
    return files.subList(fromIndex, toIndex);
  }
}
