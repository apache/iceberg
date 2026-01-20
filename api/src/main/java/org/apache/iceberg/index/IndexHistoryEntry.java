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
package org.apache.iceberg.index;

/**
 * Index history entry.
 *
 * <p>An entry contains a change to the index state. At the given timestamp, the current version was
 * set to the given version ID.
 *
 * <p>The version log tracks changes to the index's current version. This is the index's history and
 * allows reconstructing what version of the index would have been used at some point in time.
 *
 * <p>Note that this is not the version's creation time, which is stored in each version's metadata.
 * A version can appear multiple times in the version log, indicating that the index definition was
 * rolled back.
 */
public interface IndexHistoryEntry {

  /**
   * Return the timestamp in milliseconds of the change.
   *
   * <p>Timestamp when the index's current-version-id was updated (ms from epoch).
   *
   * @return the timestamp in milliseconds
   */
  long timestampMillis();

  /**
   * Return ID of the new current version.
   *
   * <p>ID that current-version-id was set to.
   *
   * @return the version ID
   */
  int versionId();
}
