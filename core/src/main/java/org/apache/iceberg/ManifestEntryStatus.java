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
 * <p>This is a top-level enum to avoid duplication across manifest entry types (V3 ManifestEntry
 * and V4 TrackingInfo).
 */
public enum ManifestEntryStatus {
  EXISTING(0),
  ADDED(1),
  DELETED(2);

  private final int id;

  ManifestEntryStatus(int id) {
    this.id = id;
  }

  public int id() {
    return id;
  }
}
