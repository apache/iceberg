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
import static org.apache.iceberg.TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS_STRATEGY;
import static org.apache.iceberg.TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS_STRATEGY_DEFAULT;
import static org.apache.iceberg.TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.concurrent.Immutable;
import org.apache.hadoop.util.Sets;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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

  public interface MetricsMaxInferredColumnDefaultsStrategy {
    Schema subSchemaMetricPriority(Schema schema, int maxInferredDefaultColumns);
  }

  static class OriginalMetricsMaxInferredColumnDefaultsStrategy
      implements MetricsMaxInferredColumnDefaultsStrategy {
    // This doesn't accurately bound the number of metrics as nested fields will not count towards
    // the overall metric count
    @Override
    public Schema subSchemaMetricPriority(Schema schema, int maxInferredDefaultColumns) {
      // an inferred default mode is applied to the first few columns, up to the limit
      int columnLengthCap = Math.min(maxInferredDefaultColumns, schema.columns().size());
      return new Schema(schema.columns().subList(0, columnLengthCap));
    }
  }

  abstract static class FieldOrderBasedPriority
      implements MetricsMaxInferredColumnDefaultsStrategy {
    abstract Iterable<Integer> fieldPriority(Schema schema, int maxInferredDefaultColumns);

    @Override
    public Schema subSchemaMetricPriority(Schema schema, int maxInferredDefaultColumns) {
      Set<Integer> boundedFieldIds =
          Sets.newHashSet(
              Iterables.limit(
                  fieldPriority(schema, maxInferredDefaultColumns), maxInferredDefaultColumns));

      return TypeUtil.project(schema, boundedFieldIds);
    }
  }

  static class DepthFirstFieldPriority extends FieldOrderBasedPriority {
    @Override
    List<Integer> fieldPriority(Schema schema, int maxInferredDefaultColumns) {
      List<Integer> orderedFieldIds = Lists.newArrayList();
      return TypeUtil.visit(
          schema,
          new TypeUtil.SchemaVisitor<>() {

            @Override
            public List<Integer> schema(Schema schema, List<Integer> structResult) {
              return orderedFieldIds;
            }

            @Override
            public List<Integer> list(Types.ListType list, List<Integer> elementResult) {
              if (list.elementType().isPrimitiveType()) {
                orderedFieldIds.add(list.elementId());
              }
              return orderedFieldIds;
            }

            @Override
            public List<Integer> map(
                Types.MapType map, List<Integer> keyResult, List<Integer> valueResult) {
              if (map.valueType().isPrimitiveType()) {
                orderedFieldIds.add(map.valueId());
              }

              if (map.keyType().isPrimitiveType()) {
                orderedFieldIds.add(map.keyId());
              }

              return orderedFieldIds;
            }

            @Override
            public List<Integer> field(Types.NestedField field, List<Integer> fieldResult) {
              if (field.type().isPrimitiveType()) {
                orderedFieldIds.add(field.fieldId());
              }
              return orderedFieldIds;
            }
          });
    }
  }

  static class BreadthFirstFieldPriority extends FieldOrderBasedPriority {

    @Override
    Iterable<Integer> fieldPriority(Schema schema, int maxInferredDefaultColumns) {

      return TypeUtil.visit(
          schema,
          new TypeUtil.CustomOrderSchemaVisitor<Iterable<Integer>>() {

            @Override
            public Iterable<Integer> schema(
                Schema schema, Supplier<Iterable<Integer>> structResult) {
              return structResult.get();
            }

            @Override
            public Iterable<Integer> struct(
                Types.StructType struct, Iterable<Iterable<Integer>> fieldResults) {

              List<Integer> orderedFieldIds =
                  Lists.newArrayListWithExpectedSize(struct.fields().size());
              for (Types.NestedField field : struct.fields()) {
                if (field.type().isPrimitiveType()) {
                  orderedFieldIds.add(field.fieldId());
                }
              }

              Iterable<Integer> returnValue = orderedFieldIds;
              for (Iterable<Integer> otherFieldIds : fieldResults) {
                returnValue = Iterables.concat(returnValue, otherFieldIds);
              }

              return returnValue;
            }

            @Override
            public Iterable<Integer> field(
                Types.NestedField field, Supplier<Iterable<Integer>> future) {
              return future.get();
            }

            @Override
            public Iterable<Integer> list(Types.ListType list, Supplier<Iterable<Integer>> future) {
              List<Integer> returnValue = Lists.newArrayListWithCapacity(1);
              if (list.elementType().isPrimitiveType()) {
                returnValue.add(list.elementId());
              }
              return Iterables.concat(returnValue, future.get());
            }

            @Override
            public Iterable<Integer> map(
                Types.MapType map,
                Supplier<Iterable<Integer>> keyFuture,
                Supplier<Iterable<Integer>> valueFuture) {
              List<Integer> returnValue = Lists.newArrayListWithCapacity(2);
              if (map.keyType().isPrimitiveType()) {
                returnValue.add(map.keyId());
              }
              if (map.valueType().isPrimitiveType()) {
                returnValue.add(map.valueId());
              }

              return Iterables.concat(returnValue, keyFuture.get(), valueFuture.get());
            }

            @Override
            public Iterable<Integer> variant(Types.VariantType variant) {
              return Collections.emptyList();
            }

            @Override
            public Iterable<Integer> primitive(Type.PrimitiveType primitive) {
              return Collections.emptyList();
            }
          });
    }
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
    } else if (schema == null) {
      defaultMode = DEFAULT_MODE;
    } else {
      if (TypeUtil.getProjectedIds(schema).size() <= maxInferredDefaultColumns) {
        // there are less than the inferred limit, so the default is used everywhere
        defaultMode = DEFAULT_MODE;
      } else {

        MetricsMaxInferredColumnDefaultsStrategy strategy =
            maxInferredColumnDefaultsStrategy(props);
        Schema subSchema = strategy.subSchemaMetricPriority(schema, maxInferredDefaultColumns);
        for (Integer id : TypeUtil.getProjectedIds(subSchema)) {
          columnModes.put(schema.findColumnName(id), DEFAULT_MODE);
        }

        // all other columns don't use metrics
        defaultMode = MetricsModes.None.get();
      }
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

  private static final Map<String, Supplier<MetricsMaxInferredColumnDefaultsStrategy>>
      VALID_STRATEGIES =
          ImmutableMap.of(
              "original", () -> new OriginalMetricsMaxInferredColumnDefaultsStrategy(),
              "depth", () -> new DepthFirstFieldPriority(),
              "breadth", () -> new BreadthFirstFieldPriority());

  static MetricsMaxInferredColumnDefaultsStrategy maxInferredColumnDefaultsStrategy(
      Map<String, String> properties) {
    String strategyName =
        PropertyUtil.propertyAsString(
            properties,
            METRICS_MAX_INFERRED_COLUMN_DEFAULTS_STRATEGY,
            METRICS_MAX_INFERRED_COLUMN_DEFAULTS_STRATEGY_DEFAULT);

    if (!VALID_STRATEGIES.keySet().contains(strategyName)) {
      LOG.warn(
          "Invalid value for {} (unknown): {}, falling back to {}",
          METRICS_MAX_INFERRED_COLUMN_DEFAULTS_STRATEGY,
          strategyName,
          METRICS_MAX_INFERRED_COLUMN_DEFAULTS_STRATEGY_DEFAULT);
      strategyName = METRICS_MAX_INFERRED_COLUMN_DEFAULTS_STRATEGY_DEFAULT;
    }

    return VALID_STRATEGIES.get(strategyName).get();
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
