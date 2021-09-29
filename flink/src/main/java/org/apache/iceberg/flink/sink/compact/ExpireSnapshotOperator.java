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

package org.apache.iceberg.flink.sink.compact;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.CommonControllerMessage;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.EndExpireSnapshot;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.SnapshotsUnit;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpireSnapshotOperator extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<CommonControllerMessage, Void>, BoundedOneInput {
  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotOperator.class);

  private final TableLoader tableLoader;
  private transient Table table;
  private TableOperations ops;

  private Consumer<String> deleteFunc;
  private ExecutorService deleteExecutorService;

  public ExpireSnapshotOperator(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.tableLoader.open();
    this.table = this.tableLoader.loadTable();
    this.ops = ((HasTableOperations) table).operations();
    this.deleteExecutorService = MoreExecutors.newDirectExecutorService();
    this.deleteFunc = file -> ops.io().deleteFile(file);
  }

  @Override
  public void processElement(StreamRecord<CommonControllerMessage> element) throws Exception {
    CommonControllerMessage value = element.getValue();
    if (value instanceof SnapshotsUnit) {
      SnapshotsUnit unit = (SnapshotsUnit) value;
      if (unit.isTaskMessage(
          getRuntimeContext().getNumberOfParallelSubtasks(),
          getRuntimeContext().getIndexOfThisSubtask())) {
        LOG.info("Delete SnapshotsUnit (id: {}, to delete {} files: {})",
            unit.getUnitId(), unit.getDeletedFiles().size(), Joiner.on(", ").join(unit.getDeletedFiles()));

        Tasks.foreach(unit.getDeletedFiles())
            .executeWith(deleteExecutorService)
            .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
            .onFailure((list, exc) -> LOG.warn("Delete failed for manifest list: {}", list, exc))
            .run(deleteFunc::accept);
      }
    } else if (value instanceof EndExpireSnapshot) {
      LOG.info("Received a EndExpireSnapshot message");
    }
  }

  /**
   * It is notified that no more data will arrive on the input.
   */
  @Override
  public void endInput() throws Exception {
  }

  @Override
  public void dispose() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }
  }

}
