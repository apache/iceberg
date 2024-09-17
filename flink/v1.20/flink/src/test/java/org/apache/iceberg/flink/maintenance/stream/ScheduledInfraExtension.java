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
package org.apache.iceberg.flink.maintenance.stream;

import static org.apache.iceberg.flink.maintenance.operator.OperatorTestBase.IGNORED_OPERATOR_NAME;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.maintenance.operator.CollectingSink;
import org.apache.iceberg.flink.maintenance.operator.ManualSource;
import org.apache.iceberg.flink.maintenance.operator.TaskResult;
import org.apache.iceberg.flink.maintenance.operator.Trigger;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * {@link org.junit.jupiter.api.extension.Extension} used to generate the common elements for the
 * {@link MaintenanceTaskBuilder} implementations. These are the following:
 *
 * <ul>
 *   <li>{@link StreamExecutionEnvironment} - environment for testing
 *   <li>{@link ManualSource} - source for manually emitting {@link Trigger}s
 *   <li>{@link DataStream} - which generated from the {@link ManualSource}
 *   <li>{@link CollectingSink} - which could be used poll for the records emitted by the
 *       maintenance tasks
 * </ul>
 */
class ScheduledInfraExtension implements BeforeEachCallback {
  private StreamExecutionEnvironment env;
  private ManualSource<Trigger> source;
  private DataStream<Trigger> triggerStream;
  private CollectingSink<TaskResult> sink;

  @Override
  public void beforeEach(ExtensionContext context) {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    source = new ManualSource<>(env, TypeInformation.of(Trigger.class));
    // Adds the watermark to mimic the behaviour expected for the input of the maintenance tasks
    triggerStream =
        source
            .dataStream()
            .assignTimestampsAndWatermarks(new TableMaintenance.PunctuatedWatermarkStrategy())
            .name(IGNORED_OPERATOR_NAME)
            .forceNonParallel();
    sink = new CollectingSink<>();
  }

  StreamExecutionEnvironment env() {
    return env;
  }

  ManualSource<Trigger> source() {
    return source;
  }

  DataStream<Trigger> triggerStream() {
    return triggerStream;
  }

  CollectingSink<TaskResult> sink() {
    return sink;
  }
}
