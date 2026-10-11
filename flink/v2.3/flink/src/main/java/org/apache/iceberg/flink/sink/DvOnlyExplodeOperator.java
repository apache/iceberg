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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;

/**
 * Splits a writer committable into the streams the pre-commit topology handles separately: the rows
 * that enter the primary key index, the deletes that have to be resolved against it, the positions
 * the writer already resolved itself, and the files that are committed as they are.
 *
 * <p>Rows leave on the main output because they are shuffled by data file, deletes on {@link
 * #DELETES_STREAM} because they are shuffled by equality key, positions on {@link
 * #POSITIONS_STREAM} because they join the resolved deletes keyed by data file, and the files on
 * {@link #FILES_STREAM} because they need no shuffle at all.
 *
 * <p>{@link org.apache.flink.streaming.api.connector.sink2.CommittableSummary} records are dropped:
 * {@link IcebergWriteAggregator} ignores incoming summaries and emits a single one of its own, so
 * the pre-commit topology is free to repartition.
 */
@Internal
class DvOnlyExplodeOperator extends AbstractStreamOperator<DvOnlyRecord>
    implements OneInputStreamOperator<CommittableMessage<SinkWriteResult>, DvOnlyRecord> {

  // The type must be stated explicitly: an OutputTag cannot capture a parameterized type from an
  // anonymous subclass, which would leave the side output typed as a raw CommittableMessage and
  // make it impossible to union with the resolved deletion vectors.
  static final OutputTag<CommittableMessage<SinkWriteResult>> FILES_STREAM =
      new OutputTag<>(
          "dv-only-files", CommittableMessageTypeInfo.of(SinkWriteResultSerializer::dvOnly));

  static final OutputTag<DvOnlyRecord> DELETES_STREAM =
      new OutputTag<>("dv-only-deletes", TypeInformation.of(DvOnlyRecord.class));

  static final OutputTag<DVPosition> POSITIONS_STREAM =
      new OutputTag<>("dv-only-positions", TypeInformation.of(DVPosition.class));

  @Override
  public void processElement(StreamRecord<CommittableMessage<SinkWriteResult>> element) {
    CommittableMessage<SinkWriteResult> message = element.getValue();
    if (!(message instanceof CommittableWithLineage)) {
      return;
    }

    CommittableWithLineage<SinkWriteResult> lineage =
        (CommittableWithLineage<SinkWriteResult>) message;
    SinkWriteResult result = lineage.getCommittable();
    long checkpointId = lineage.getCheckpointId();

    for (SerializedEqualityValues key : result.deleteKeys()) {
      output.collect(DELETES_STREAM, new StreamRecord<>(DvOnlyRecord.delete(key, checkpointId)));
    }

    for (PkIndexEntry entry : result.liveRows()) {
      output.collect(new StreamRecord<>(DvOnlyRecord.addRow(entry, checkpointId)));
    }

    for (DVPosition position : result.positionDeletes()) {
      output.collect(POSITIONS_STREAM, new StreamRecord<>(position));
    }

    // Keep the files and drop the payload that the index consumes.
    output.collect(
        FILES_STREAM,
        new StreamRecord<>(
            lineage.map(committable -> new SinkWriteResult(committable.writeResult()))));
  }
}
