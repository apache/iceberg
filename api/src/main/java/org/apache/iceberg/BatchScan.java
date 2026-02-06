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

/** API for configuring a batch scan. */
public interface BatchScan extends Scan<BatchScan, ScanTask, ScanTaskGroup<ScanTask>> {
  /**
   * Returns the {@link Table} from which this scan loads data.
   *
   * @return this scan's table
   */
  Table table();

  /**
   * Create a new {@link BatchScan} from this scan's configuration that will use a snapshot with the
   * given ID.
   *
   * @param snapshotId a snapshot ID
   * @return a new scan based on this with the given snapshot ID
   * @throws IllegalArgumentException if the snapshot cannot be found
   */
  BatchScan useSnapshot(long snapshotId);

  /**
   * Create a new {@link BatchScan} from this scan's configuration that will use the given
   * reference.
   *
   * @param ref a reference
   * @return a new scan based on this with the given reference
   * @throws IllegalArgumentException if the reference with the given name could not be found
   */
  BatchScan useRef(String ref);

  /**
   * Create a new {@link BatchScan} from this scan's configuration that will use the most recent
   * snapshot as of the given time in milliseconds on the branch in the scan or main if no branch is
   * set.
   *
   * @param timestampMillis a timestamp in milliseconds
   * @return a new scan based on this with the current snapshot at the given time
   * @throws IllegalArgumentException if the snapshot cannot be found or time travel is attempted on
   *     a tag
   */
  BatchScan asOfTime(long timestampMillis);

  /**
   * Returns the {@link Snapshot} that will be used by this scan.
   *
   * <p>If the snapshot was not configured using {@link #asOfTime(long)} or {@link
   * #useSnapshot(long)}, the current table snapshot will be used.
   *
   * @return the Snapshot this scan will use
   */
  Snapshot snapshot();
}
