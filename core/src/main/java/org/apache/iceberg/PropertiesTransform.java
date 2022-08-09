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

import static org.apache.iceberg.TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

class PropertiesTransform {
  private PropertiesTransform() {}

  static Map<String, String> applySchemaChanges(
      Map<String, String> props, List<String> deletedColumns, Map<String, String> renamedColumns) {
    if (props.keySet().stream()
        .noneMatch(
            key ->
                key.startsWith(METRICS_MODE_COLUMN_CONF_PREFIX)
                    || key.startsWith(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX))) {
      return props;
    } else {
      Map<String, String> updatedProperties = Maps.newHashMap();
      props
          .keySet()
          .forEach(
              key -> {
                String prefix = null;
                if (key.startsWith(METRICS_MODE_COLUMN_CONF_PREFIX)) {
                  prefix = METRICS_MODE_COLUMN_CONF_PREFIX;
                } else if (key.startsWith(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX)) {
                  prefix = PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
                }

                if (prefix != null) {
                  String columnAlias = key.replaceFirst(prefix, "");
                  if (renamedColumns.get(columnAlias) != null) {
                    // The name has changed.
                    String newKey = prefix + renamedColumns.get(columnAlias);
                    updatedProperties.put(newKey, props.get(key));
                  } else if (!deletedColumns.contains(columnAlias)) {
                    // Copy over the original.
                    updatedProperties.put(key, props.get(key));
                  }
                } else {
                  updatedProperties.put(key, props.get(key));
                }
              });

      return updatedProperties;
    }
  }
}
