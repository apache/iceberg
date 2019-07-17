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

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.iceberg.MetricsModes.MetricsMode;

import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE;
import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE_DEFAULT;

public class MetricsConfig {

  private static final String COLUMN_CONF_PREFIX = "write.metadata.metrics.column.";

  private Map<String, MetricsMode> columnModes = Maps.newHashMap();
  private MetricsMode defaultMode;

  private MetricsConfig() {}

  public static MetricsConfig getDefault() {
    MetricsConfig spec = new MetricsConfig();
    spec.defaultMode = MetricsModes.fromString(DEFAULT_WRITE_METRICS_MODE_DEFAULT);
    return spec;
  }

  public static MetricsConfig fromProperties(Map<String, String> props) {
    MetricsConfig spec = new MetricsConfig();
    props.keySet().stream()
        .filter(key -> key.startsWith(COLUMN_CONF_PREFIX))
        .forEach(key -> {
          MetricsMode mode = MetricsModes.fromString(props.get(key));
          String columnAlias = key.replaceFirst(COLUMN_CONF_PREFIX, "");
          spec.columnModes.put(columnAlias, mode);
        });
    String defaultModeAsString = props.getOrDefault(DEFAULT_WRITE_METRICS_MODE, DEFAULT_WRITE_METRICS_MODE_DEFAULT);
    spec.defaultMode = MetricsModes.fromString(defaultModeAsString);
    return spec;
  }

  public MetricsMode columnMode(String columnAlias) {
    return columnModes.getOrDefault(columnAlias, defaultMode);
  }
}
