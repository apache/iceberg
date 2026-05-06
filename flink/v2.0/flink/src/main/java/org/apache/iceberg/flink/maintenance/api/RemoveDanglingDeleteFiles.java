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
package org.apache.iceberg.flink.maintenance.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.iceberg.flink.maintenance.operator.RemoveDanglingDeleteFilesProcessor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Removes dangling delete files from the current snapshot. A delete file is dangling if its deletes
 * no longer apply to any live data files.
 */
public class RemoveDanglingDeleteFiles {
  private static final String EXECUTOR_OPERATOR_NAME = "Remove Dangling Deletes";

  private RemoveDanglingDeleteFiles() {}

  /** Creates the builder for creating a stream which removes dangling delete files. */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends MaintenanceTaskBuilder<RemoveDanglingDeleteFiles.Builder> {
    @Override
    String maintenanceTaskName() {
      return "RemoveDanglingDeleteFiles";
    }

    @Override
    DataStream<TaskResult> append(DataStream<Trigger> trigger) {
      Preconditions.checkNotNull(tableLoader(), "TableLoader should not be null");

      return trigger
          .process(new RemoveDanglingDeleteFilesProcessor(tableLoader()))
          .name(operatorName(EXECUTOR_OPERATOR_NAME))
          .uid(EXECUTOR_OPERATOR_NAME + uidSuffix())
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }
  }
}
