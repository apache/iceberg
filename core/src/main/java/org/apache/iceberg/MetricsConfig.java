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

import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE;
import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE_DEFAULT;
import static org.apache.iceberg.TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS;
import static org.apache.iceberg.TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT;
import static org.apache.iceberg.TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX;

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
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SortOrderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public final class MetricsConfig implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsConfig.class);
  private static final Joiner DOT = Joiner.on('.');

  // Disable metrics by default for wide tables to prevent excessive metadata
  private static final MetricsMode DEFAULT_MODE =
      MetricsModes.fromString(DEFAULT_WRITE_METRICS_MODE_DEFAULT);
  private static final MetricsConfig DEFAULT = new MetricsConfig(ImmutableMap.of(), DEFAULT_MODE);

  private final Map<String, MetricsMode> columnModes;
  private final MetricsMode defaultMode;

  private MetricsConfig(Map<String, MetricsMode> columnModes, MetricsMode defaultMode) {
    this.columnModes = SerializableMap.copyOf(columnModes).immutableMap();
    this.defaultMode = defaultMode;
  }

  public static MetricsConfig getDefault() {
    return DEFAULT;
  }

  static Map<String, String> updateProperties(
      Map<String, String> props, List<String> deletedColumns, Map<String, String> renamedColumns) {
    if (props.keySet().stream().noneMatch(key -> key.startsWith(METRICS_MODE_COLUMN_CONF_PREFIX))) {
      return props;
    } else {
      Map<String, String> updatedProperties = Maps.newHashMap();
      // Put all of the non metrics columns we aren't modifying
      props
          .keySet()
          .forEach(
              key -> {
                if (key.startsWith(METRICS_MODE_COLUMN_CONF_PREFIX)) {
                  String columnAlias = key.replaceFirst(METRICS_MODE_COLUMN_CONF_PREFIX, "");
                  if (renamedColumns.get(columnAlias) != null) {
                    // The name has changed.
                    String newKey =
                        METRICS_MODE_COLUMN_CONF_PREFIX + renamedColumns.get(columnAlias);
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
   *
   * @param props table configuration
   * @deprecated use {@link MetricsConfig#forTable(Table)}
   */
  @Deprecated
  public static MetricsConfig fromProperties(Map<String, String> props) {
    return from(props, null, null);
  }

  /**
   * Creates a metrics config from a table.
   *
   * @param table iceberg table
   */
  public static MetricsConfig forTable(Table table) {
    return from(table.properties(), table.schema(), table.sortOrder());
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
    tableConfig.columnModes.forEach(
        (columnAlias, mode) -> {
          String positionDeleteColumnAlias =
              DOT.join(MetadataColumns.DELETE_FILE_ROW_FIELD_NAME, columnAlias);
          columnModes.put(positionDeleteColumnAlias, mode);
        });

    return new MetricsConfig(columnModes.build(), defaultMode);
  }

  /**
   * Generate a MetricsConfig for all columns based on overrides, schema, and sort order.
   *
   * @param props will be read for metrics overrides (write.metadata.metrics.column.*) and default
   *     (write.metadata.metrics.default)
   * @param schema table schema
   * @param order sort order columns, will be promoted to truncate(16)
   * @return metrics configuration
   */
  private static MetricsConfig from(Map<String, String> props, Schema schema, SortOrder order) {
    int maxInferredDefaultColumns = maxInferredColumnDefaults(props);
    Map<String, MetricsMode> columnModes = Maps.newHashMap();

    // Handle user override of default mode
    MetricsMode defaultMode;
    String configuredDefault = props.get(DEFAULT_WRITE_METRICS_MODE);
    if (configuredDefault != null) {
      // a user-configured default mode is applied for all columns
      defaultMode = parseMode(configuredDefault, DEFAULT_MODE, "default");

    } else if (schema == null || schema.columns().size() <= maxInferredDefaultColumns) {
      // there are less than the inferred limit, so the default is used everywhere
      defaultMode = DEFAULT_MODE;

    } else {
      // an inferred default mode is applied to the first few columns, up to the limit
      Schema subSchema = new Schema(schema.columns().subList(0, maxInferredDefaultColumns));
      for (Integer id : TypeUtil.getProjectedIds(subSchema)) {
        columnModes.put(subSchema.findColumnName(id), DEFAULT_MODE);
      }

      // all other columns don't use metrics
      defaultMode = MetricsModes.None.get();
    }

    // First set sorted column with sorted column default (can be overridden by user)
    MetricsMode sortedColDefaultMode = sortedColumnDefaultMode(defaultMode);
    Set<String> sortedCols = SortOrderUtil.orderPreservingSortedColumns(order);
    sortedCols.forEach(sc -> columnModes.put(sc, sortedColDefaultMode));

    // Handle user overrides of defaults
    for (String key : props.keySet()) {
      if (key.startsWith(METRICS_MODE_COLUMN_CONF_PREFIX)) {
        String columnAlias = key.replaceFirst(METRICS_MODE_COLUMN_CONF_PREFIX, "");
        MetricsMode mode = parseMode(props.get(key), defaultMode, "column " + columnAlias);
        columnModes.put(columnAlias, mode);
      }
    }

    return new MetricsConfig(columnModes, defaultMode);
  }

  /**
   * Auto promote sorted columns to truncate(16) if default is set at Counts or None.
   *
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

  private static int maxInferredColumnDefaults(Map<String, String> properties) {
    int maxInferredDefaultColumns =
        PropertyUtil.propertyAsInt(
            properties,
            METRICS_MAX_INFERRED_COLUMN_DEFAULTS,
            METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT);
    if (maxInferredDefaultColumns < 0) {
      LOG.warn(
          "Invalid value for {} (negative): {}, falling back to {}",
          METRICS_MAX_INFERRED_COLUMN_DEFAULTS,
          maxInferredDefaultColumns,
          METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT);
      return METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT;
    } else {
      return maxInferredDefaultColumns;
    }
  }

  private static MetricsMode parseMode(String modeString, MetricsMode fallback, String context) {
    try {
      return MetricsModes.fromString(modeString);
    } catch (IllegalArgumentException err) {
      // User override was invalid, log the error and use the default
      LOG.warn("Ignoring invalid metrics mode ({}): {}", context, modeString, err);
      return fallback;
    }
  }

  public void validateReferencedColumns(Schema schema) {
    for (String column : columnModes.keySet()) {
      ValidationException.check(
          schema.findField(column) != null,
          "Invalid metrics config, could not find column %s from table prop %s in schema %s",
          column,
          METRICS_MODE_COLUMN_CONF_PREFIX + column,
          schema);
    }
  }

  public MetricsMode columnMode(String columnAlias) {
    return columnModes.getOrDefault(columnAlias, defaultMode);
  }
}
