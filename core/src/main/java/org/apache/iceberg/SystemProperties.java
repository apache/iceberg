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

/** Configuration properties that are controlled by Java system properties. */
public class SystemProperties {

  private SystemProperties() {}

  /**
   * Sets the size of the worker pool. The worker pool limits the number of tasks concurrently
   * processing manifests in the base table implementation across all concurrent planning or commit
   * operations.
   */
  public static final String WORKER_THREAD_POOL_SIZE_PROP = "iceberg.worker.num-threads";

  /** Whether to use the shared worker pool when planning table scans. */
  public static final String SCAN_THREAD_POOL_ENABLED = "iceberg.scan.plan-in-worker-pool";

  /**
   * Maximum number of distinct {@link org.apache.iceberg.io.FileIO} that is allowed to have
   * associated {@link org.apache.iceberg.io.ContentCache} in memory at a time.
   */
  public static final String IO_MANIFEST_CACHE_MAX_FILEIO = "iceberg.io.manifest.cache.fileio-max";

  public static final int IO_MANIFEST_CACHE_MAX_FILEIO_DEFAULT = 8;

  static boolean getBoolean(String systemProperty, boolean defaultValue) {
    String value = System.getProperty(systemProperty);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }
}
