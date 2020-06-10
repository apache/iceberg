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

package org.apache.iceberg.mr.mapred.iterables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

/**
 * Creates an Iterable of Records with all snapshot metadata that can be used with the RecordReader.
 */
public class SnapshotIterable implements CloseableIterable {

  private Table table;

  public SnapshotIterable(Table table) {
    this.table = table;
  }

  public CloseableIterator iterator() {
    Iterable<Snapshot> snapshots = table.snapshots();
    List<Record> snapRecords = new ArrayList<>();
    snapshots.forEach(snapshot -> snapRecords.add(createSnapshotRecord(snapshot)));

    return (CloseableIterator) snapRecords.iterator();
  }

  /**
   * Populates a Record with snapshot metadata.
   */
  private Record createSnapshotRecord(Snapshot snapshot) {
    Record snapRecord = GenericRecord.create(table.schema());
    snapRecord.setField("committed_at", snapshot.timestampMillis());
    snapRecord.setField("snapshot_id", snapshot.snapshotId());
    snapRecord.setField("parent_id", snapshot.parentId());
    snapRecord.setField("operation", snapshot.operation());
    snapRecord.setField("manifest_list", snapshot.manifestListLocation());
    snapRecord.setField("summary", snapshot.summary());
    return snapRecord;
  }

  @Override
  public void close() throws IOException {
    iterator().close();
  }
}
