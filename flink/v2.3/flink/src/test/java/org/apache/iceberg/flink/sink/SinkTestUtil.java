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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.Table;

public class SinkTestUtil {

  private SinkTestUtil() {}

  @SuppressWarnings("unchecked")
  static List<StreamElement> transformsToStreamElement(Collection<Object> elements) {
    return elements.stream()
        .map(
            element -> {
              if (element instanceof StreamRecord) {
                return new StreamRecord<>(
                    ((StreamRecord<CommittableMessage<?>>) element).getValue());
              }
              return (StreamElement) element;
            })
        .collect(Collectors.toList());
  }

  static CommittableSummary<?> extractAndAssertCommittableSummary(StreamElement element) {
    final Object value = element.asRecord().getValue();
    assertThat(value).isInstanceOf(CommittableSummary.class);
    return (CommittableSummary<?>) value;
  }

  static CommittableWithLineage<IcebergCommittable> extractAndAssertCommittableWithLineage(
      StreamElement element) {
    final Object value = element.asRecord().getValue();
    assertThat(value).isInstanceOf(CommittableWithLineage.class);
    return (CommittableWithLineage<IcebergCommittable>) value;
  }

  /** Expires every snapshot but the branch heads, keeping their files. */
  public static void expireAllButHeads(Table table) {
    table
        .expireSnapshots()
        .expireOlderThan(System.currentTimeMillis() + 1)
        .retainLast(1)
        .cleanExpiredFiles(false)
        .commit();
  }

  /** Asserts the writer's last committed checkpoint, without recording anything. */
  static void assertCommittedCheckpoint(
      Table table, String branch, String jobId, String operatorId, long expected) {
    table.refresh();
    assertThat(FlinkCommitMarkers.maxCommittedCheckpointId(table, branch, jobId, operatorId))
        .isEqualTo(expected);
    long durable =
        FlinkCommitMarkers.durableCheckpointId(table.properties(), branch, jobId, operatorId);
    if (expected == IcebergStreamWriter.END_INPUT_CHECKPOINT_ID) {
      // The end-of-input checkpoint is recorded in the summary only.
      assertThat(durable).isLessThan(expected);
    } else {
      assertThat(durable).isEqualTo(expected);
    }
  }
}
