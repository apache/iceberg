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

import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * An action that removes orphan metadata and data files by listing a given location and comparing
 * the actual files in that location with data and metadata files referenced by all valid snapshots.
 * The location must be accessible for listing via the Hadoop {@link FileSystem}.
 * <p>
 * By default, this action cleans up the table location returned by {@link Table#location()} and
 * removes unreachable files that are older than 3 days using {@link Table#io()}. The behavior can be modified
 * by passing a custom location to {@link #location} and a custom timestamp to {@link #olderThan(long)}.
 * For example, someone might point this action to the data folder to clean up only orphan data files.
 * In addition, there is a way to configure an alternative delete method via {@link #deleteWith(Consumer)}.
 * <p>
 * <em>Note:</em> It is dangerous to call this action with a short retention interval as it might corrupt
 * the state of the table if another operation is writing at the same time.
 *
 * @deprecated since 0.12.0, will be removed in 0.13.0; use {@link DeleteOrphanFiles} instead.
 */
@Deprecated
public class RemoveOrphanFilesAction implements Action<RemoveOrphanFilesAction, List<String>> {
  private final DeleteOrphanFiles delegate;

  RemoveOrphanFilesAction(DeleteOrphanFiles delegate) {
    this.delegate = delegate;
  }

  /**
   * Removes orphan files in the given location.
   *
   * @param newLocation a location
   * @return this for method chaining
   */
  public RemoveOrphanFilesAction location(String newLocation) {
    delegate.location(newLocation);
    return this;
  }

  /**
   * Removes orphan files that are older than the given timestamp.
   *
   * @param newOlderThanTimestamp a timestamp in milliseconds
   * @return this for method chaining
   */
  public RemoveOrphanFilesAction olderThan(long newOlderThanTimestamp) {
    delegate.olderThan(newOlderThanTimestamp);
    return this;
  }

  /**
   * Passes an alternative delete implementation that will be used to delete orphan files.
   *
   * @param newDeleteFunc a delete func
   * @return this for method chaining
   */
  public RemoveOrphanFilesAction deleteWith(Consumer<String> newDeleteFunc) {
    delegate.deleteWith(newDeleteFunc);
    return this;
  }

  @Override
  public List<String> execute() {
    DeleteOrphanFiles.Result result = delegate.execute();
    return ImmutableList.copyOf(result.orphanFileLocations());
  }
}
