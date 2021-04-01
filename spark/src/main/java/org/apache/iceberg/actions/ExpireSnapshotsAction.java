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

package org.apache.iceberg.actions;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.spark.actions.BaseExpireSnapshotsSparkAction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * An action which performs the same operation as {@link org.apache.iceberg.ExpireSnapshots} but uses Spark
 * to determine the delta in files between the pre and post-expiration table metadata. All of the same
 * restrictions of Remove Snapshots also apply to this action.
 * <p>
 * This implementation uses the metadata tables for the table being expired to list all Manifest and DataFiles. This
 * is made into a Dataframe which are anti-joined with the same list read after the expiration. This operation will
 * require a shuffle so parallelism can be controlled through spark.sql.shuffle.partitions. The expiration is done
 * locally using a direct call to RemoveSnapshots. The snapshot expiration will be fully committed before any deletes
 * are issued. Deletes are still performed locally after retrieving the results from the Spark executors.
 *
 * @deprecated since 0.12.0, will be removed in 0.13.0; use {@link BaseExpireSnapshotsSparkAction} instead.
 */
@Deprecated
public class ExpireSnapshotsAction implements Action<ExpireSnapshotsAction, ExpireSnapshotsActionResult> {
  private final BaseExpireSnapshotsSparkAction delegate;

  ExpireSnapshotsAction(BaseExpireSnapshotsSparkAction delegate) {
    this.delegate = delegate;
  }

  /**
   * By default, all files to delete are brought to the driver at once which may be an issue with very long file lists.
   * Set this to true to use toLocalIterator if you are running into memory issues when collecting
   * the list of files to be deleted.
   * @param stream whether to use toLocalIterator to stream results instead of collect.
   * @return this for method chaining
   */
  public ExpireSnapshotsAction streamDeleteResults(boolean stream) {
    delegate.option("stream-results", Boolean.toString(stream));
    return this;
  }

  /**
   * An executor service used when deleting files. Only used during the local delete phase of this Spark action.
   * Similar to {@link ExpireSnapshots#executeDeleteWith(ExecutorService)}
   * @param executorService the service to use
   * @return this for method chaining
   */
  public ExpireSnapshotsAction executeDeleteWith(ExecutorService executorService) {
    delegate.executeDeleteWith(executorService);
    return this;
  }

  /**
   * A specific snapshot to expire.
   * Identical to {@link ExpireSnapshots#expireSnapshotId(long)}
   * @param expireSnapshotId Id of the snapshot to expire
   * @return this for method chaining
   */
  public ExpireSnapshotsAction expireSnapshotId(long expireSnapshotId) {
    delegate.expireSnapshotId(expireSnapshotId);
    return this;
  }

  /**
   * Expire all snapshots older than a given timestamp.
   * Identical to {@link ExpireSnapshots#expireOlderThan(long)}
   * @param timestampMillis all snapshots before this time will be expired
   * @return this for method chaining
   */
  public ExpireSnapshotsAction expireOlderThan(long timestampMillis) {
    delegate.expireOlderThan(timestampMillis);
    return this;
  }

  /**
   * Retain at least x snapshots when expiring
   * Identical to {@link ExpireSnapshots#retainLast(int)}
   * @param numSnapshots number of snapshots to leave
   * @return this for method chaining
   */
  public ExpireSnapshotsAction retainLast(int numSnapshots) {
    delegate.retainLast(numSnapshots);
    return this;
  }

  /**
   * The Consumer used on files which have been determined to be expired. By default uses a filesystem delete.
   * Identical to {@link ExpireSnapshots#deleteWith(Consumer)}
   * @param newDeleteFunc Consumer which takes a path and deletes it
   * @return this for method chaining
   */
  public ExpireSnapshotsAction deleteWith(Consumer<String> newDeleteFunc) {
    delegate.deleteWith(newDeleteFunc);
    return this;
  }

  /**
   * Expires snapshots and commits the changes to the table, returning a Dataset of files to delete.
   * <p>
   * This does not delete data files. To delete data files, run {@link #execute()}.
   * <p>
   * This may be called before or after {@link #execute()} is called to return the expired file list.
   *
   * @return a Dataset of files that are no longer referenced by the table
   */
  public Dataset<Row> expire() {
    return delegate.expire();
  }

  @Override
  public ExpireSnapshotsActionResult execute() {
    org.apache.iceberg.actions.ExpireSnapshots.Result result = delegate.execute();
    return ExpireSnapshotsActionResult.wrap(result);
  }
}
