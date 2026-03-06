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

/**
 * Status of an entry in a manifest file.
 *
 * <p>This is a top-level enum to avoid duplication across manifest entry types (v3 ManifestEntry
 * and v4 TrackingInfo).
 *
 * <p>For v4: Only ADDED and EXISTING entries are considered live for scan planning. DELETED and
 * REPLACED entries are used for change detection but are not live. REPLACED may only be used for
 * entries that have had data column updates or deletion vector changes, and every REPLACED entry
 * will have a corresponding EXISTING entry for the same location.
 */
public enum ManifestEntryStatus {
  EXISTING(0),
  ADDED(1),
  DELETED(2),
  // v4 only: indicates an entry that has been replaced by a column update or DV change.
  REPLACED(3);

  private final int id;

  ManifestEntryStatus(int id) {
    this.id = id;
  }

  public int id() {
    return id;
  }
}
