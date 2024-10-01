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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

class SinkTestUtil {

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

  static <R> R invokeIcebergSinkBuilderMethod(IcebergSinkBuilder<?> sinkBuilder,
                                              Function<FlinkSink.Builder, R> processSinkV1Builder,
                                              Function<IcebergSink.Builder, R> processSinkV2Builder) {
      if (sinkBuilder instanceof FlinkSink.Builder) {
          return processSinkV1Builder.apply((FlinkSink.Builder) sinkBuilder);
      } else if (sinkBuilder instanceof IcebergSink.Builder) {
          return processSinkV2Builder.apply((IcebergSink.Builder) sinkBuilder);
      } else {
          throw new IllegalArgumentException("Not expected sinkBuilder class: " + sinkBuilder.getClass());
      }
  }

}
