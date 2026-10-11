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
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks which equality keys the primary key index holds in each data file, and reads the files it
 * is asked to index. The first input is keyed by data file path, so a file is owned by a single
 * subtask; the second input carries the {@link DvOnlyRecord.Type#CLEANUP} broadcast.
 *
 * <p>The index resolves a delete into the position of the row it removes, which stops being valid
 * once the data file holding that row leaves the table. This operator is what makes those positions
 * removable: every row that enters the index passes through here first, so when {@link
 * DvOnlyCoordinator} reports a file as gone, the keys that have to forget it are known without
 * rebuilding the index.
 *
 * <p>When the files that left the table can no longer be enumerated, the coordinator rebuilds the
 * index under a new generation and broadcasts a cleanup. A file is then dropped unless it was read
 * for that generation or holds rows of a checkpoint that is not committed yet. The order in which
 * the cleanup and the reads of the same generation arrive does not matter: a file dropped before it
 * is read again is indexed from scratch by that read.
 *
 * <p>The mapping may name a key whose positions in the file were already consumed by an earlier
 * delete. Dropping such a key is a no-op, whereas missing one would leave a position pointing at a
 * file that is gone, so the mapping is allowed to be generous but never short. It is discarded as a
 * whole once the file leaves the table, which is the only point at which entries stop being useful.
 */
@Internal
class DvOnlyFileIndexOperator extends AbstractStreamOperator<DvOnlyRecord>
    implements TwoInputStreamOperator<DvOnlyRecord, DvOnlyRecord, DvOnlyRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(DvOnlyFileIndexOperator.class);

  // A map is used as a set, since the same key is reported again whenever a file is read again.
  private static final MapStateDescriptor<SerializedEqualityValues, Boolean> KEYS_DESCRIPTOR =
      new MapStateDescriptor<>(
          "dvOnlyFileKeys", TypeInformation.of(SerializedEqualityValues.class), Types.BOOLEAN);

  private static final ValueStateDescriptor<Long> GENERATION_DESCRIPTOR =
      new ValueStateDescriptor<>("dvOnlyFileGeneration", Types.LONG);

  private static final ValueStateDescriptor<Long> CHECKPOINT_DESCRIPTOR =
      new ValueStateDescriptor<>("dvOnlyFileCheckpoint", Types.LONG);

  private final TableLoader tableLoader;
  private final Set<Integer> equalityFieldIds;

  /** Keys the index holds positions for in the current data file. */
  private transient MapState<SerializedEqualityValues, Boolean> keys;

  /** Latest generation the current data file was read for, absent when it never was. */
  private transient ValueState<Long> generation;

  /** Latest checkpoint of this sink that wrote rows to the current data file, if any. */
  private transient ValueState<Long> checkpoint;

  private transient PkIndexFileReader reader;

  DvOnlyFileIndexOperator(TableLoader tableLoader, Set<Integer> equalityFieldIds) {
    this.tableLoader = tableLoader;
    this.equalityFieldIds = ImmutableSet.copyOf(equalityFieldIds);
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    Table table = tableLoader.loadTable();
    reader = new PkIndexFileReader(table, equalityFieldIds);
    keys = getRuntimeContext().getMapState(KEYS_DESCRIPTOR);
    generation = getRuntimeContext().getState(GENERATION_DESCRIPTOR);
    checkpoint = getRuntimeContext().getState(CHECKPOINT_DESCRIPTOR);
  }

  @Override
  public void processElement1(StreamRecord<DvOnlyRecord> element) throws Exception {
    DvOnlyRecord record = element.getValue();
    switch (record.type()) {
      case ADD_ROW -> {
        keys.put(record.key(), true);
        checkpoint.update(max(checkpoint.value(), record.checkpointId()));
        output.collect(element);
      }
      case READ_FILE -> indexFile(record);
      case DROP_FILE -> dropFile(record.filePath());
      default -> output.collect(element);
    }
  }

  @Override
  public void processElement2(StreamRecord<DvOnlyRecord> element) throws Exception {
    DvOnlyRecord record = element.getValue();
    Preconditions.checkState(
        record.type() == DvOnlyRecord.Type.CLEANUP,
        "Unexpected record on the broadcast input of the primary key index: %s",
        record.type());
    cleanup(record.generation(), record.checkpointId());
  }

  private void indexFile(DvOnlyRecord record) throws IOException {
    long rows =
        reader.read(
            PkIndexReadTask.decode(record.readTask()),
            entry -> {
              register(entry.key());
              output.collect(new StreamRecord<>(DvOnlyRecord.bootstrapRow(entry)));
            });

    if (rows > 0) {
      generation.update(max(generation.value(), record.generation()));
    }

    LOG.debug("Indexed {} live row(s) of data file {}", rows, record.filePath());
  }

  private void cleanup(long currentGeneration, long committedCheckpointId) throws Exception {
    KeyedStateBackend<String> backend = getKeyedStateBackend();
    List<String> files;
    try (Stream<String> tracked =
        backend.getKeys(KEYS_DESCRIPTOR.getName(), VoidNamespace.INSTANCE)) {
      files = tracked.toList();
    }

    long dropped = 0;
    for (String file : files) {
      setCurrentKey(file);
      if (isStale(currentGeneration, committedCheckpointId)) {
        dropFile(file);
        dropped++;
      }
    }

    LOG.info(
        "Dropped {} of {} data file(s) the primary key index no longer covers after its rebuild for "
            + "generation {}",
        dropped,
        files.size(),
        currentGeneration);
  }

  private boolean isStale(long currentGeneration, long committedCheckpointId) throws IOException {
    Long readFor = generation.value();
    Long writtenBy = checkpoint.value();
    return (readFor == null || readFor < currentGeneration)
        && (writtenBy == null || writtenBy <= committedCheckpointId);
  }

  private void dropFile(String filePath) throws Exception {
    long dropped = 0;
    for (SerializedEqualityValues key : keys.keys()) {
      output.collect(new StreamRecord<>(DvOnlyRecord.dropPositions(key, filePath)));
      dropped++;
    }

    keys.clear();
    generation.clear();
    checkpoint.clear();
    if (dropped > 0) {
      LOG.debug("Dropped {} key(s) indexed in data file {}", dropped, filePath);
    }
  }

  private void register(SerializedEqualityValues key) {
    try {
      keys.put(key, true);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to record the keys of a data file", e);
    }
  }

  private static long max(Long current, long candidate) {
    return current != null ? Math.max(current, candidate) : candidate;
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }
}
