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
 * Builder interface for constructing {@link IndexVersion} instances with custom properties.
 *
 * <p>Properties from the {@link Index}'s current version are not inherited. Only properties added
 * through this builder are applied to the newly built version.
 *
 * <p>This API collects version configuration for fluent chaining.
 *
 * @param <T> the concrete builder type for method chaining
 */
public interface VersionBuilder<T> {
  /**
   * Adds key/value properties to the index.
   *
   * @param properties key/value properties
   */
  T withProperties(Map<String, String> properties);

  /**
   * Adds a key/value property to the index version.
   *
   * @param key the property key
   * @param value the property value
   */
  T withProperty(String key, String value);
}
