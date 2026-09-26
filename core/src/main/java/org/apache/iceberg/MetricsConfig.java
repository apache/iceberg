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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.concurrent.Immutable;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SortOrderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public final class MetricsConfig implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsConfig.class);

  // Disable metrics by default for wide tables to prevent excessive metadata
  private static final MetricsMode DEFAULT_MODE =
      MetricsModes.fromString(DEFAULT_WRITE_METRICS_MODE_DEFAULT);
  private static final MetricsConfig DEFAULT =
      new MetricsConfig(ImmutableMap.of(), DEFAULT_MODE, ImmutableMap.of());
  private static final MetricsConfig POSITION_DELETE_MODE =
      new MetricsConfig(
          ImmutableMap.of(
              MetadataColumns.DELETE_FILE_PATH.name(),
              MetricsModes.Full.get(),
              MetadataColumns.DELETE_FILE_POS.name(),
              MetricsModes.Full.get()),
          MetricsModes.None.get(),
          ImmutableMap.of(
              MetadataColumns.DELETE_FILE_PATH.fieldId(),
              MetadataColumns.DELETE_FILE_PATH.name(),
              MetadataColumns.DELETE_FILE_POS.fieldId(),
              MetadataColumns.DELETE_FILE_POS.name()));

  private final Map<String, MetricsMode> columnModes;
  private final MetricsMode defaultMode;
  private final Map<Integer, String> idToName;

  private MetricsConfig(
      Map<String, MetricsMode> columnModes,
      MetricsMode defaultMode,
      Map<Integer, String> idToName) {
    this.columnModes = SerializableMap.copyOf(columnModes).immutableMap();
    this.defaultMode = defaultMode;
    this.idToName = idToName != null ? SerializableMap.copyOf(idToName).immutableMap() : null;
  }

  public static MetricsConfig getDefault() {
    return DEFAULT;
  }

  public static MetricsConfig forPositionDelete() {
    return POSITION_DELETE_MODE;
  }

  /**
   * Creates a metrics config from table configuration.
   *
   * @param props table configuration
   * @deprecated use {@link MetricsConfig#forTable(Table)}. Will be removed in 1.13.0
   */
  @Deprecated
  public static MetricsConfig fromProperties(Map<String, String> props) {
    return from(props, null, null);
  }

  /**
   * Validates metrics config properties with the given schema.
   *
   * @param props table properties
   * @param schema table schema
   */
  public static void validate(Map<String, String> props, Schema schema) {
    from(props, schema, null).validateReferencedColumns(schema);
  }

  /**
   * Creates a metrics config from a table.
   *
   * @param table iceberg table
   * @return a metrics config for the given table
   */
  public static MetricsConfig forTable(Table table) {
    return from(table.properties(), table.schema(), table.sortOrder());
  }

  public Iterable<Integer> metricsFieldIds() {
    Preconditions.checkState(idToName != null, "Cannot resolve column mode by ID: missing schema");
    return idToName.keySet();
  }

  public MetricsMode columnMode(int id) {
    Preconditions.checkState(idToName != null, "Cannot resolve column mode by ID: missing schema");
    String name = idToName.get(id);
    if (name != null) {
      return columnMode(name);
    }

    return defaultMode;
  }

  public MetricsMode columnMode(String columnAlias) {
    return columnModes.getOrDefault(columnAlias, defaultMode);
  }

  public void validateReferencedColumns(Schema schema) {
    for (String column : columnModes.keySet()) {
      Types.NestedField field = schema.findField(column);
      ValidationException.check(
          field != null,
          "Invalid metrics config, could not find column %s from table prop %s in schema %s",
          column,
          METRICS_MODE_COLUMN_CONF_PREFIX + column,
          schema);

      ValidationException.check(
          null == idToName || column.equals(idToName.get(field.fieldId())),
          "Incorrect field name for id %s: %s (expected %s)",
          field.fieldId(),
          column,
          idToName.get(field.fieldId()));
    }
  }

  static Set<Integer> limitFieldIds(Schema schema, int limit) {
    return TypeUtil.visit(
        schema,
        new TypeUtil.CustomOrderSchemaVisitor<>() {
          private final Set<Integer> idSet = Sets.newHashSet();

          private boolean shouldContinue() {
            return idSet.size() < limit;
          }

          private boolean metricsEligible(Type type) {
            return type.isPrimitiveType() || type.isVariantType();
          }

          @Override
          @SuppressWarnings("ReturnValueIgnored")
          public Set<Integer> schema(Schema schema, Supplier<Set<Integer>> structResult) {
            // We need to call structResult.get() to visit the schema
            structResult.get();
            return idSet;
          }

          @Override
          public Set<Integer> struct(Types.StructType struct, Iterable<Set<Integer>> fieldResults) {
            Iterator<Types.NestedField> fields = struct.fields().iterator();
            while (shouldContinue() && fields.hasNext()) {
              Types.NestedField field = fields.next();
              if (metricsEligible(field.type())) {
                idSet.add(field.fieldId());
              }
            }

            Iterator<Set<Integer>> iter = fieldResults.iterator();
            while (shouldContinue() && iter.hasNext()) {
              // visit children lazily to add more ids
              iter.next();
            }

            return null;
          }

          @Override
          @SuppressWarnings("ReturnValueIgnored")
          public Set<Integer> field(Types.NestedField field, Supplier<Set<Integer>> fieldResult) {
            fieldResult.get();
            return null;
          }

          @Override
          public Set<Integer> variant(Types.VariantType variant) {
            return null;
          }

          @Override
          @SuppressWarnings("ReturnValueIgnored")
          public Set<Integer> list(Types.ListType list, Supplier<Set<Integer>> elementResult) {
            if (shouldContinue() && metricsEligible(list.elementType())) {
              idSet.add(list.elementId());
            }

            elementResult.get();
            return null;
          }

          @Override
          @SuppressWarnings("ReturnValueIgnored")
          public Set<Integer> map(
              Types.MapType map,
              Supplier<Set<Integer>> keyResult,
              Supplier<Set<Integer>> valueResult) {

            if (shouldContinue() && metricsEligible(map.keyType())) {
              idSet.add(map.keyId());
            }

            if (shouldContinue() && metricsEligible(map.valueType())) {
              idSet.add(map.valueId());
            }

            keyResult.get();
            valueResult.get();
            return null;
          }
        });
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
  public static MetricsConfig from(Map<String, String> props, Schema schema, SortOrder order) {
    int maxDefaultColumns = maxInferredColumnDefaults(props);

    // Handle configured default mode
    MetricsMode configuredDefault = configuredDefault(props);
    Map<String, MetricsMode> defaultColumnConf = defaultColumnModes(schema, maxDefaultColumns);

    MetricsMode defaultMode;
    if (configuredDefault != null) {
      defaultMode = configuredDefault;
    } else if (defaultColumnConf.size() < maxDefaultColumns) {
      // an additional column should use the default mode
      defaultMode = DEFAULT_MODE;
    } else {
      // an additional column should not store metrics
      defaultMode = MetricsModes.None.get();
    }

    Map<String, MetricsMode> columnModes = Maps.newHashMap();

    if (configuredDefault == null) {
      columnModes.putAll(defaultColumnConf);
    }

    // Default sort columns to at least truncate (overridden by config)
    columnModes.putAll(sortColumnModes(order, configuredDefault));

    // Override automatic modes with configured modes
    columnModes.putAll(configuredColumnModes(props));

    Map<Integer, String> idToName = idToName(schema, columnModes);

    return new MetricsConfig(columnModes, defaultMode, idToName);
  }

  private static MetricsMode configuredDefault(Map<String, String> props) {
    String configuredDefault = props.get(DEFAULT_WRITE_METRICS_MODE);
    if (configuredDefault != null) {
      // a user-configured default mode is applied for all columns
      return parseMode(configuredDefault, null, "default");
    }

    return null;
  }

  private static Map<String, MetricsMode> defaultColumnModes(Schema schema, int maxColumns) {
    ImmutableMap.Builder<String, MetricsMode> builder = ImmutableMap.builder();
    if (schema != null) {
      for (int id : limitFieldIds(schema, maxColumns)) {
        builder.put(schema.findColumnName(id), DEFAULT_MODE);
      }
    }

    return builder.build();
  }

  private static Map<String, MetricsMode> sortColumnModes(
      SortOrder order, MetricsMode configuredDefault) {
    ImmutableMap.Builder<String, MetricsMode> builder = ImmutableMap.builder();
    MetricsMode sortDefault = promoteToIncludeBounds(configuredDefault);
    for (String name : SortOrderUtil.orderPreservingSortedColumns(order)) {
      builder.put(name, sortDefault);
    }

    return builder.build();
  }

  private static Map<String, MetricsMode> configuredColumnModes(Map<String, String> props) {
    ImmutableMap.Builder<String, MetricsMode> builder = ImmutableMap.builder();
    for (String key : props.keySet()) {
      if (key.startsWith(METRICS_MODE_COLUMN_CONF_PREFIX)) {
        String columnAlias = key.replaceFirst(METRICS_MODE_COLUMN_CONF_PREFIX, "");
        MetricsMode mode = parseMode(props.get(key), null, "column " + columnAlias);
        if (mode != null) {
          builder.put(columnAlias, mode);
        }
      }
    }

    return builder.build();
  }

  private static Map<Integer, String> idToName(
      Schema schema, Map<String, MetricsMode> columnModes) {
    if (schema != null) {
      ImmutableMap.Builder<Integer, String> builder = ImmutableMap.builder();
      for (String name : columnModes.keySet()) {
        builder.put(schema.findField(name).fieldId(), name);
      }

      return builder.build();
    }

    return null;
  }

  /**
   * Mode used for automatic metrics for sort and partitioning. Uses truncate(16) if default is
   * Counts or None.
   *
   * @param defaultMode default mode
   * @return mode to use
   */
  private static MetricsMode promoteToIncludeBounds(MetricsMode defaultMode) {
    if (defaultMode == null
        || defaultMode == MetricsModes.None.get()
        || defaultMode == MetricsModes.Counts.get()) {
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
}
