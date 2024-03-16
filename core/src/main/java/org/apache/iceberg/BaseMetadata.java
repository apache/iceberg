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

import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.util.PropertyUtil;

/** A base class for {@link TableMetadata} and {@link org.apache.iceberg.view.ViewMetadata} */
public interface BaseMetadata extends Serializable {

  String location();

  Map<String, String> properties();

  @Nullable
  String metadataFileLocation();

  Schema schema();

  default String property(String property, String defaultValue) {
    return properties().getOrDefault(property, defaultValue);
  }

  default boolean propertyAsBoolean(String property, boolean defaultValue) {
    return PropertyUtil.propertyAsBoolean(properties(), property, defaultValue);
  }

  default int propertyAsInt(String property, int defaultValue) {
    return PropertyUtil.propertyAsInt(properties(), property, defaultValue);
  }

  default long propertyAsLong(String property, long defaultValue) {
    return PropertyUtil.propertyAsLong(properties(), property, defaultValue);
  }
}
