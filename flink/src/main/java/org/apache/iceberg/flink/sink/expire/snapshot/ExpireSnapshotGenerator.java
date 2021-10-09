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

package org.apache.iceberg.flink.sink.expire.snapshot;

import java.util.Date;
import java.util.Set;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.RemoveSnapshots;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.expire.snapshot.ExpireSnapshotMessage.CommonControllerMessage;
import org.apache.iceberg.flink.sink.expire.snapshot.ExpireSnapshotMessage.EndCheckpoint;
import org.apache.iceberg.flink.sink.expire.snapshot.ExpireSnapshotMessage.EndExpireSnapshot;
import org.apache.iceberg.flink.sink.expire.snapshot.ExpireSnapshotMessage.SnapshotsUnit;
import org.apache.iceberg.io.ExpireSnapshotResult;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ExpireSnapshotUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpireSnapshotGenerator extends AbstractStreamOperator<CommonControllerMessage>
    implements OneInputStreamOperator<EndCheckpoint, CommonControllerMessage>, BoundedOneInput {
  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotGenerator.class);

  private final TableLoader tableLoader;
  private transient Table table;
  private TableOperations ops;

  private Integer retainLastValue = null;
  private Long retainMaxAgeMs = null;

  public ExpireSnapshotGenerator(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    this.tableLoader.open();
    this.table = tableLoader.loadTable();
    this.ops = ((HasTableOperations) table).operations();

    this.retainMaxAgeMs = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.FLINK_AUTO_MAX_SNAPSHOT_AGE_MS,
        TableProperties.FLINK_AUTO_MAX_SNAPSHOT_AGE_MS_DEFAULT);

    this.retainLastValue = PropertyUtil
        .propertyAsInt(
            table.properties(),
            TableProperties.FLINK_AUTO_MIN_SNAPSHOTS_TO_KEEP,
            TableProperties.FLINK_AUTO_MIN_SNAPSHOTS_TO_KEEP_DEFAULT);
  }

  @Override
  public void processElement(StreamRecord<EndCheckpoint> element) throws Exception {
    //  received a EndCheckpoint message
    EndCheckpoint endCheckpoint =  element.getValue();
    long currentTimeMillis = System.currentTimeMillis();
    LOG.info("Received an EndCheckpoint {}, begin to compute expired snapshots", endCheckpoint.getCheckpointId());

    if (ExpireSnapshotUtil.shouldExpireSnapshot(table)) {
      RemoveSnapshots removeSnapshots = new RemoveSnapshots(this.ops);
      removeSnapshots
          .cleanExpiredFiles(false)
          .expireOlderThan(currentTimeMillis - this.retainMaxAgeMs)
          .retainLast(this.retainLastValue);
      removeSnapshots.commit();
      LOG.info("update last expired snapshot timestamps：{}", new Date(currentTimeMillis));
      table.updateProperties()
          .set(TableProperties.FLINK_LAST_EXPIRE_SNAPSHOT_MS, Long.toString(currentTimeMillis))
          .commit();

      ExpireSnapshotResult result = removeSnapshots.findExpiredSnapshotResult();
      if (result != null) {
        Set<String> manifestFiles = result.getManifestsToDelete();
        Set<String> manifestListFiles = result.getManifestListsToDelete();
        Set<String> dataFiles = result.getFilesToDelete();

        Set<String> allFilesToDelete = Sets.newHashSet();
        allFilesToDelete.addAll(manifestFiles);
        allFilesToDelete.addAll(manifestListFiles);
        allFilesToDelete.addAll(dataFiles);
        LOG.info("Generated {} expired files: {}", allFilesToDelete.size(), Joiner.on(", ").join(allFilesToDelete));

        int groupNums = PropertyUtil.propertyAsInt(
            table.properties(),
            TableProperties.FLINK_AUTO_SNAPSHOTS_GROUP_NUMS,
            TableProperties.FLINK_AUTO_SNAPSHOTS_GROUP_NUMS_DEFAULT);
        int indexOfFile = 0;
        int index = 0;
        Set<String> fileBuffer = Sets.newHashSet();

        for (String file : allFilesToDelete) {
          fileBuffer.add(file);
          indexOfFile++;
          if (indexOfFile % groupNums == 0) {
            emit(new SnapshotsUnit(fileBuffer, index));
            LOG.info("Emit a SnapshotUnit (id :{}, files: {})", index, fileBuffer);
            index++;
            fileBuffer.clear();
          }
        }
        emit(new SnapshotsUnit(fileBuffer, index));
        LOG.info("Emit a SnapshotUnit (id :{}, files: {})", index, fileBuffer);
        fileBuffer.clear();

        //  send a expire snapshot end message
        emit(new EndExpireSnapshot(endCheckpoint.getCheckpointId()));
      }
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

  private void emit(CommonControllerMessage result) {
    output.collect(new StreamRecord<>(result));
  }
}
