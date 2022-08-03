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
package org.apache.iceberg.spark.actions;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.Table;
import org.apache.spark.sql.SparkSession;

/**
 * An action to expire snapshots.
 *
 * @deprecated since 0.14.0, will be removed in 1.0.0; use {@link SparkActions} and {@link
 *     ExpireSnapshotsSparkAction} instead.
 */
@Deprecated
public class BaseExpireSnapshotsSparkAction extends ExpireSnapshotsSparkAction {

  public static final String STREAM_RESULTS = "stream-results";
  public static final boolean STREAM_RESULTS_DEFAULT = false;

  public BaseExpireSnapshotsSparkAction(SparkSession spark, Table table) {
    super(spark, table);
  }

  @Override
  public BaseExpireSnapshotsSparkAction executeDeleteWith(ExecutorService executorService) {
    super.executeDeleteWith(executorService);
    return this;
  }

  @Override
  public BaseExpireSnapshotsSparkAction expireSnapshotId(long snapshotId) {
    super.expireSnapshotId(snapshotId);
    return this;
  }

  @Override
  public BaseExpireSnapshotsSparkAction expireOlderThan(long timestampMillis) {
    super.expireOlderThan(timestampMillis);
    return this;
  }

  @Override
  public BaseExpireSnapshotsSparkAction retainLast(int numSnapshots) {
    super.retainLast(numSnapshots);
    return this;
  }

  @Override
  public BaseExpireSnapshotsSparkAction deleteWith(Consumer<String> newDeleteFunc) {
    super.deleteWith(newDeleteFunc);
    return this;
  }
}
