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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimitedSourceReader;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Monitors an Iceberg table for changes */
public class MonitorSource extends SingleThreadedIteratorSource<TableChange> {
  private static final Logger LOG = LoggerFactory.getLogger(MonitorSource.class);

  private final TableLoader tableLoader;
  private final RateLimiterStrategy rateLimiterStrategy;
  private final long maxReadBack;

  /**
   * Creates a {@link org.apache.flink.api.connector.source.Source} which monitors an Iceberg table
   * for changes.
   *
   * @param tableLoader used for accessing the table
   * @param rateLimiterStrategy limits the frequency the table is checked
   * @param maxReadBack sets the number of snapshots read before stopping change collection
   */
  public MonitorSource(
      TableLoader tableLoader, RateLimiterStrategy rateLimiterStrategy, long maxReadBack) {
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");
    Preconditions.checkNotNull(rateLimiterStrategy, "Rate limiter strategy should no be null");
    Preconditions.checkArgument(maxReadBack > 0, "Need to read at least 1 snapshot to work");

    this.tableLoader = tableLoader;
    this.rateLimiterStrategy = rateLimiterStrategy;
    this.maxReadBack = maxReadBack;
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public TypeInformation<TableChange> getProducedType() {
    return TypeInformation.of(TableChange.class);
  }

  @Override
  public Iterator<TableChange> createIterator() {
    return new SchedulerEventIterator(tableLoader, null, maxReadBack);
  }

  @Override
  public SimpleVersionedSerializer<Iterator<TableChange>> getIteratorSerializer() {
    return new SchedulerEventIteratorSerializer(tableLoader, maxReadBack);
  }

  @Override
  public SourceReader<TableChange, GlobalSplit<TableChange>> createReader(
      SourceReaderContext readerContext) throws Exception {
    RateLimiter rateLimiter = rateLimiterStrategy.createRateLimiter(1);
    return new RateLimitedSourceReader<>(super.createReader(readerContext), rateLimiter);
  }

  /** The Iterator which returns the latest changes on an Iceberg table. */
  @VisibleForTesting
  static class SchedulerEventIterator implements Iterator<TableChange> {
    private Long lastSnapshotId;
    private final long maxReadBack;
    private final Table table;

    SchedulerEventIterator(TableLoader tableLoader, Long lastSnapshotId, long maxReadBack) {
      this.lastSnapshotId = lastSnapshotId;
      this.maxReadBack = maxReadBack;
      tableLoader.open();
      this.table = tableLoader.loadTable();
    }

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public TableChange next() {
      try {
        table.refresh();
        Snapshot currentSnapshot = table.currentSnapshot();
        Long current = currentSnapshot != null ? currentSnapshot.snapshotId() : null;
        Long checking = current;
        TableChange event = new TableChange(0, 0, 0L, 0L, 0);
        long readBack = 0;
        while (checking != null && !checking.equals(lastSnapshotId) && ++readBack <= maxReadBack) {
          Snapshot snapshot = table.snapshot(checking);
          if (snapshot != null) {
            LOG.debug("Reading snapshot {}", snapshot.snapshotId());
            event.merge(new TableChange(snapshot, table.io()));
            checking = snapshot.parentId();
          } else {
            // If the last snapshot has been removed from the history
            checking = null;
          }
        }

        lastSnapshotId = current;
        return event;
      } catch (Exception e) {
        LOG.warn("Failed to fetch table changes for {}", table, e);
        return new TableChange(0, 0, 0L, 0L, 0);
      }
    }
  }

  private static final class SchedulerEventIteratorSerializer
      implements SimpleVersionedSerializer<Iterator<TableChange>> {

    private static final int CURRENT_VERSION = 1;
    private final TableLoader tableLoader;
    private final long maxReadBack;

    SchedulerEventIteratorSerializer(TableLoader tableLoader, long maxReadBack) {
      this.tableLoader = tableLoader;
      this.maxReadBack = maxReadBack;
    }

    @Override
    public int getVersion() {
      return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Iterator<TableChange> iterator) throws IOException {
      Preconditions.checkArgument(
          iterator instanceof SchedulerEventIterator,
          "Can not serialize different type: %s",
          iterator.getClass());

      SchedulerEventIterator schedulerEventIterator = (SchedulerEventIterator) iterator;
      DataOutputSerializer out = new DataOutputSerializer(8);
      long toStore =
          schedulerEventIterator.lastSnapshotId != null
              ? schedulerEventIterator.lastSnapshotId
              : -1L;
      out.writeLong(toStore);
      return out.getCopyOfBuffer();
    }

    @Override
    public SchedulerEventIterator deserialize(int version, byte[] serialized) throws IOException {
      Preconditions.checkArgument(version == CURRENT_VERSION, "Unrecognized version: %s", version);
      DataInputDeserializer in = new DataInputDeserializer(serialized);
      long fromStore = in.readLong();
      return new SchedulerEventIterator(
          tableLoader, fromStore != -1 ? fromStore : null, maxReadBack);
    }
  }
}
