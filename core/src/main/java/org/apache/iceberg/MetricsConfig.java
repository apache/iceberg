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
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.Immutable;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SortOrderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE;
import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX;

@Immutable
public final class MetricsConfig implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsConfig.class);
  private static final Joiner DOT = Joiner.on('.');

  // Disable metrics by default for wide tables to prevent excessive metadata
  private static final int MAX_COLUMNS = 32;
  private static final MetricsConfig DEFAULT = new MetricsConfig(ImmutableMap.of(),
      MetricsModes.fromString(DEFAULT_WRITE_METRICS_MODE_DEFAULT));

  private final Map<String, MetricsMode> columnModes;
  private final MetricsMode defaultMode;

  private MetricsConfig(Map<String, MetricsMode> columnModes, MetricsMode defaultMode) {
    this.columnModes = SerializableMap.copyOf(columnModes).immutableMap();
    this.defaultMode = defaultMode;
  }

  public static MetricsConfig getDefault() {
    return DEFAULT;
  }

  static Map<String, String> updateProperties(Map<String, String> props, List<String> deletedColumns,
                                              Map<String, String> renamedColumns) {
    if (props.keySet().stream().noneMatch(key -> key.startsWith(METRICS_MODE_COLUMN_CONF_PREFIX))) {
      return props;
    } else {
      Map<String, String> updatedProperties = Maps.newHashMap();
      // Put all of the non metrics columns we aren't modifying
      props.keySet().forEach(key -> {
        if (key.startsWith(METRICS_MODE_COLUMN_CONF_PREFIX)) {
          String columnAlias = key.replaceFirst(METRICS_MODE_COLUMN_CONF_PREFIX, "");
          if (renamedColumns.get(columnAlias) != null) {
            // The name has changed.
            String newKey = METRICS_MODE_COLUMN_CONF_PREFIX + renamedColumns.get(columnAlias);
            updatedProperties.put(newKey, props.get(key));
          } else if (!deletedColumns.contains(columnAlias)) {
            // Copy over the original
            updatedProperties.put(key, props.get(key));
          }
          // Implicit drop if deleted
        } else {
          // Not a metric property
          updatedProperties.put(key, props.get(key));
        }
      });
      return updatedProperties;
    }
  }

  /**
   * Creates a metrics config from table configuration.
   * @param props table configuration
   * @deprecated use {@link MetricsConfig#forTable(Table)}
   **/
  @Deprecated
  public static MetricsConfig fromProperties(Map<String, String> props) {
    return from(props, null, DEFAULT_WRITE_METRICS_MODE_DEFAULT);
  }

  /**
   * Creates a metrics config from a table.
   * @param table iceberg table
   */
  public static MetricsConfig forTable(Table table) {
    String defaultMode;
    if (table.schema().columns().size() <= MAX_COLUMNS) {
      defaultMode = DEFAULT_WRITE_METRICS_MODE_DEFAULT;
    } else {
      defaultMode = MetricsModes.None.get().toString();
    }
    return from(table.properties(), table.sortOrder(), defaultMode);
  }

  /**
   * Creates a metrics config for a position delete file.
   *
   * @param table an Iceberg table
   */
  public static MetricsConfig forPositionDelete(Table table) {
    ImmutableMap.Builder<String, MetricsMode> columnModes = ImmutableMap.builder();

    columnModes.put(MetadataColumns.DELETE_FILE_PATH.name(), MetricsModes.Full.get());
    columnModes.put(MetadataColumns.DELETE_FILE_POS.name(), MetricsModes.Full.get());

    MetricsConfig tableConfig = forTable(table);

    MetricsMode defaultMode = tableConfig.defaultMode;
    tableConfig.columnModes.forEach((columnAlias, mode) -> {
      String positionDeleteColumnAlias = DOT.join(MetadataColumns.DELETE_FILE_ROW_FIELD_NAME, columnAlias);
      columnModes.put(positionDeleteColumnAlias, mode);
    });

    return new MetricsConfig(columnModes.build(), defaultMode);
  }

  /**
   * Generate a MetricsConfig for all columns based on overrides, sortOrder, and defaultMode.
   * @param props will be read for metrics overrides (write.metadata.metrics.column.*) and default
   *              (write.metadata.metrics.default)
   * @param order sort order columns, will be promoted to truncate(16)
   * @param defaultMode default, if not set by user property
   * @return metrics configuration
   */
  private static MetricsConfig from(Map<String, String> props, SortOrder order, String defaultMode) {
    Map<String, MetricsMode> columnModes = Maps.newHashMap();

    // Handle user override of default mode
    MetricsMode finalDefaultMode;
    String defaultModeAsString = props.getOrDefault(DEFAULT_WRITE_METRICS_MODE, defaultMode);
    try {
      finalDefaultMode = MetricsModes.fromString(defaultModeAsString);
    } catch (IllegalArgumentException err) {
      // User override was invalid, log the error and use the default
      LOG.warn("Ignoring invalid default metrics mode: {}", defaultModeAsString, err);
      finalDefaultMode = MetricsModes.fromString(defaultMode);
    }

    // First set sorted column with sorted column default (can be overridden by user)
    MetricsMode sortedColDefaultMode = sortedColumnDefaultMode(finalDefaultMode);
    Set<String> sortedCols = SortOrderUtil.orderPreservingSortedColumns(order);
    sortedCols.forEach(sc -> columnModes.put(sc, sortedColDefaultMode));

    // Handle user overrides of defaults
    MetricsMode finalDefaultModeVal = finalDefaultMode;
    props.keySet().stream()
        .filter(key -> key.startsWith(METRICS_MODE_COLUMN_CONF_PREFIX))
        .forEach(key -> {
          String columnAlias = key.replaceFirst(METRICS_MODE_COLUMN_CONF_PREFIX, "");
          MetricsMode mode;
          try {
            mode = MetricsModes.fromString(props.get(key));
          } catch (IllegalArgumentException err) {
            // Mode was invalid, log the error and use the default
            LOG.warn("Ignoring invalid metrics mode for column {}: {}", columnAlias, props.get(key), err);
            mode = finalDefaultModeVal;
          }
          columnModes.put(columnAlias, mode);
        });

    return new MetricsConfig(columnModes, finalDefaultMode);
  }

  /**
   * Auto promote sorted columns to truncate(16) if default is set at Counts or None.
   * @param defaultMode default mode
   * @return mode to use
   */
  private static MetricsMode sortedColumnDefaultMode(MetricsMode defaultMode) {
    if (defaultMode == MetricsModes.None.get() || defaultMode == MetricsModes.Counts.get()) {
      return MetricsModes.Truncate.withLength(16);
    } else {
      return defaultMode;
    }
  }

  public void validateReferencedColumns(Schema schema) {
    for (String column : columnModes.keySet()) {
      ValidationException.check(
          schema.findField(column) != null,
          "Invalid metrics config, could not find column %s from table prop %s in schema %s",
          column, METRICS_MODE_COLUMN_CONF_PREFIX + column, schema);
    }
  }

  public MetricsMode columnMode(String columnAlias) {
    return columnModes.getOrDefault(columnAlias, defaultMode);
  }
}
