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

package org.apache.iceberg;

import java.util.function.Consumer;

/**
 * API for table changes that produce snapshots. This interface contains common methods for all
 * updates that create a new table {@link Snapshot}.
 *
 * @param <ThisT> the child Java API class, returned by method chaining.
 */
public interface SnapshotUpdate<ThisT> extends PendingUpdate<Snapshot> {
  /**
   * Set a summary property in the snapshot produced by this update.
   *
   * @param property a String property name
   * @param value a String property value
   * @return this for method chaining
   */
  ThisT set(String property, String value);

  /**
   * Set a callback to delete files instead of the table's default.
   *
   * @param deleteFunc a String consumer used to delete locations.
   * @return this for method chaining
   */
  ThisT deleteWith(Consumer<String> deleteFunc);

  /**
   * Called to stage a snapshot in table metadata, but not update the current snapshot id.
   *
   * @return this for method chaining
   */
  ThisT stageOnly();

}
