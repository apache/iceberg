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

import java.util.Map;

/**
 * A builder interface for creating {@link IndexSnapshot} instances.
 *
 * <p>This API collects snapshot configuration for fluent chaining.
 *
 * @param <T> the concrete builder type for method chaining
 */
public interface SnapshotBuilder<T> {
  /**
   * Sets the table snapshot ID which is the base of the index snapshot.
   *
   * @param tableSnapshotId the table snapshot ID
   */
  T withTableSnapshotId(long tableSnapshotId);

  /**
   * Sets the index snapshot ID.
   *
   * @param indexSnapshotId the index snapshot ID
   */
  T withIndexSnapshotId(long indexSnapshotId);

  /**
   * Sets properties for the index snapshot.
   *
   * @param properties a map of string properties
   */
  T withSnapshotProperties(Map<String, String> properties);

  /**
   * Adds a key/value property to the index snapshot.
   *
   * @param key the property key
   * @param value the property value
   */
  T withSnapshotProperty(String key, String value);
}
