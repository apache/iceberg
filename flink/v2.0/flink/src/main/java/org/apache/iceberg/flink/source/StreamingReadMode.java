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
package org.apache.iceberg.flink.source;

/**
 * Streaming read mode for Flink Iceberg source.
 *
 * <p>APPEND_ONLY: Traditional append-only streaming mode. Only reads INSERT operations. This is the
 * default mode for streaming reads.
 *
 * <p>CHANGELOG: CDC (Change Data Capture) streaming mode. Reads all changelog operations including
 * INSERT, UPDATE_BEFORE, UPDATE_AFTER, and DELETE. This mode uses IncrementalChangelogScan to read
 * changelog data from Iceberg tables.
 */
public enum StreamingReadMode {
  /**
   * Append-only streaming mode. Only reads newly appended data files. This is the default streaming
   * mode.
   */
  APPEND_ONLY,

  /**
   * Changelog streaming mode for CDC. Reads all changelog operations including INSERT,
   * UPDATE_BEFORE, UPDATE_AFTER, and DELETE.
   */
  CHANGELOG
}
