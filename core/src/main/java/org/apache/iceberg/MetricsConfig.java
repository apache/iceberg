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
import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE;
import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE_DEFAULT;

public class MetricsConfig implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsConfig.class);

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
    String defaultModeAsString = props.getOrDefault(DEFAULT_WRITE_METRICS_MODE, DEFAULT_WRITE_METRICS_MODE_DEFAULT);
    try {
      spec.defaultMode = MetricsModes.fromString(defaultModeAsString);
    } catch (IllegalArgumentException err) {
      // Mode was invalid, log the error and use the default
      LOG.warn("Ignoring invalid default metrics mode: {}", defaultModeAsString, err);
      spec.defaultMode = MetricsModes.fromString(DEFAULT_WRITE_METRICS_MODE_DEFAULT);
    }

    props.keySet().stream()
        .filter(key -> key.startsWith(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX))
        .forEach(key -> {
          String columnAlias = key.replaceFirst(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX, "");
          MetricsMode mode;
          try {
            mode = MetricsModes.fromString(props.get(key));
          } catch (IllegalArgumentException err) {
            // Mode was invalid, log the error and use the default
            LOG.warn("Ignoring invalid metrics mode for column {}: {}", columnAlias, props.get(key), err);
            mode = spec.defaultMode;
          }
          spec.columnModes.put(columnAlias, mode);
        });

    return spec;
  }

  public MetricsMode columnMode(String columnAlias) {
    return columnModes.getOrDefault(columnAlias, defaultMode);
  }
}
