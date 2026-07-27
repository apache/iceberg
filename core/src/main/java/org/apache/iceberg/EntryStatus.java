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

/** Status of an entry in a manifest file. */
enum EntryStatus {
  EXISTING(0, true),
  ADDED(1, true),
  DELETED(2, false),
  /**
   * The old (replaced) state of an entry that has been modified. Paired with MODIFIED. Added in v4.
   */
  REPLACED(3, false),
  /** The new (live) state of an entry that has been modified. Added in v4. */
  MODIFIED(4, true);

  private static final EntryStatus[] VALUES = EntryStatus.values();

  private final int id;
  private final boolean live;

  EntryStatus(int id, boolean live) {
    this.id = id;
    this.live = live;
  }

  public int id() {
    return id;
  }

  boolean isLive() {
    return live;
  }

  static EntryStatus fromId(int id) {
    return VALUES[id];
  }
}
