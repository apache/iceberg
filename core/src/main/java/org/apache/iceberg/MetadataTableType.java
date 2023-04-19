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

import java.util.Locale;

public enum MetadataTableType {
  ENTRIES,
  FILES,
  DATA_FILES,
  DELETE_FILES,
  HISTORY,
  METADATA_LOG_ENTRIES,
  SNAPSHOTS,
  REFS,
  MANIFESTS,
  PARTITIONS,
  ALL_DATA_FILES,
  ALL_DELETE_FILES,
  ALL_FILES,
  ALL_MANIFESTS,
  ALL_ENTRIES,
  POSITION_DELETES;

  public static MetadataTableType from(String name) {
    try {
      return MetadataTableType.valueOf(name.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException ignored) {
      return null;
    }
  }
}
