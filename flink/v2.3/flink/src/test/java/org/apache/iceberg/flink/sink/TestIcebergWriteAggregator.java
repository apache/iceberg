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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

/**
 * Covers what the pre-commit aggregator accepts from a writer. Resolving the reported deletes and
 * indexing the reported rows arrives in a later change, so anything the writer reports that nobody
 * consumes yet has to be rejected rather than dropped.
 */
class TestIcebergWriteAggregator {

  private final IcebergWriteAggregator aggregator = new IcebergWriteAggregator(null, "sink");

  @Test
  void acceptsAResultWithoutPayload() {
    SinkWriteResult result = new SinkWriteResult(WriteResult.builder().build());

    assertThatCode(() -> aggregator.processElement(record(result))).doesNotThrowAnyException();
  }

  @Test
  void rejectsUnresolvedDeleteKeys() {
    SinkWriteResult result =
        new SinkWriteResult(
            WriteResult.builder().build(),
            ImmutableList.of(new SerializedEqualityValues(new byte[] {1})),
            ImmutableList.of());

    assertThatThrownBy(() -> aggregator.processElement(record(result)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("cannot commit yet")
        .hasMessageContaining("1 delete key(s)");
  }

  @Test
  void rejectsReportedRows() {
    SinkWriteResult result =
        new SinkWriteResult(
            WriteResult.builder().build(), ImmutableList.of(), ImmutableList.of(row()));

    assertThatThrownBy(() -> aggregator.processElement(record(result)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("cannot commit yet")
        .hasMessageContaining("1 row(s)");
  }

  @Test
  void ignoresTheMessagesThatCarryNoResult() throws Exception {
    assertThatCode(
            () ->
                aggregator.processElement(
                    new StreamRecord<>(
                        new org.apache.flink.streaming.api.connector.sink2.CommittableSummary<>(
                            0, 1, 1L, 1, 1, 0))))
        .doesNotThrowAnyException();
  }

  private static StreamRecord<CommittableMessage<SinkWriteResult>> record(SinkWriteResult result) {
    return new StreamRecord<>(new CommittableWithLineage<>(result, 1L, 0));
  }

  private static PkIndexEntry row() {
    return new PkIndexEntry(
        new SerializedEqualityValues(new byte[] {1}),
        new DVPosition(
            "s3://bucket/table/data/file.parquet",
            0L,
            0,
            StructLikeSerializer.EMPTY_PARTITION,
            PkIndexEntry.UNKNOWN_SEQUENCE));
  }
}
