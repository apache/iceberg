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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.TableProperties.CURRENT_SNAPSHOT_ID;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FieldLabel;
import org.apache.iceberg.Labels;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SupportsLabels;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogV2Util;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableSummary;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

abstract class BaseSparkTable
    implements org.apache.spark.sql.connector.catalog.Table, SupportsMetadataColumns {

  private static final String PROVIDER = "provider";
  private static final String FORMAT = "format";
  private static final String LOCATION = "location";
  private static final String SORT_ORDER = "sort-order";
  private static final String IDENTIFIER_FIELDS = "identifier-fields";
  private static final String LABELS_OBJECT_PREFIX = "labels.object.";
  private static final String LABELS_FIELD_PREFIX = "labels.field.";
  private static final Set<String> RESERVED_PROPERTIES =
      ImmutableSet.of(
          PROVIDER,
          FORMAT,
          CURRENT_SNAPSHOT_ID,
          LOCATION,
          FORMAT_VERSION,
          SORT_ORDER,
          IDENTIFIER_FIELDS,
          TableCatalog.PROP_TABLE_TYPE);

  private final Table table;
  private final Schema schema;

  private SparkSession lazySpark = null;
  private StructType lazySparkSchema = null;
  private Column[] lazySparkColumns = null;

  protected BaseSparkTable(Table table, Schema schema) {
    this.table = table;
    this.schema = schema;
  }

  protected SparkSession spark() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.active();
    }
    return lazySpark;
  }

  public Table table() {
    return table;
  }

  @Override
  public String name() {
    return table.toString();
  }

  /**
   * @deprecated since 1.12.0, use {@link #columns()} instead
   */
  @Deprecated
  @Override
  public StructType schema() {
    return sparkSchema();
  }

  @Override
  public Column[] columns() {
    if (lazySparkColumns == null) {
      this.lazySparkColumns = CatalogV2Util.structTypeToV2Columns(sparkSchema());
    }

    return lazySparkColumns;
  }

  private StructType sparkSchema() {
    if (lazySparkSchema == null) {
      this.lazySparkSchema = SparkSchemaUtil.convert(schema);
    }

    return lazySparkSchema;
  }

  @Override
  public Transform[] partitioning() {
    return Spark3Util.toTransforms(table.spec());
  }

  @Override
  public Map<String, String> properties() {
    Map<String, String> properties = Maps.newLinkedHashMap();

    properties.put(FORMAT, "iceberg/" + fileFormat());
    properties.put(PROVIDER, "iceberg");
    properties.put(LOCATION, table.location());
    properties.put(CURRENT_SNAPSHOT_ID, currentSnapshotId());

    // Iceberg tables always have an explicit storage location and dropping a table through the
    // catalog removes only the catalog entry unless purge is requested, which matches Spark's
    // notion of an EXTERNAL table.
    properties.put(TableCatalog.PROP_TABLE_TYPE, TableSummary.EXTERNAL_TABLE_TYPE);

    if (table instanceof BaseTable) {
      TableOperations ops = ((BaseTable) table).operations();
      properties.put(FORMAT_VERSION, String.valueOf(ops.current().formatVersion()));
    }

    if (table.sortOrder().isSorted()) {
      properties.put(SORT_ORDER, Spark3Util.describe(table.sortOrder()));
    }

    Set<String> identifierFields = table.schema().identifierFieldNames();
    if (!identifierFields.isEmpty()) {
      properties.put(IDENTIFIER_FIELDS, "[" + String.join(",", identifierFields) + "]");
    }

    table.properties().entrySet().stream()
        .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
        .forEach(entry -> properties.put(entry.getKey(), entry.getValue()));

    // Surface catalog-provided labels (driver-side only; not part of table state) so they are
    // visible in DESCRIBE EXTENDED. The tbl.labels metadata table is the queryable counterpart.
    // Labels and table properties are distinct concepts: if a label's prefixed key collides with
    // an existing property, the label is surfaced under a de-conflicted key so both remain visible
    // and DESCRIBE EXTENDED / SHOW TBLPROPERTIES never fails.
    if (table instanceof SupportsLabels) {
      Labels labels = ((SupportsLabels) table).labels();
      for (Map.Entry<String, String> label : labels.objectLabels().entrySet()) {
        String key = deconflictedKey(properties, LABELS_OBJECT_PREFIX + label.getKey());
        properties.put(key, label.getValue());
      }

      for (FieldLabel fieldLabel : labels.fields()) {
        String prefix = LABELS_FIELD_PREFIX + fieldLabel.fieldId() + ".";
        for (Map.Entry<String, String> label : fieldLabel.labels().entrySet()) {
          String key = deconflictedKey(properties, prefix + label.getKey());
          properties.put(key, label.getValue());
        }
      }
    }

    return ImmutableMap.copyOf(properties);
  }

  /**
   * Returns {@code preferredKey} if it is free, otherwise a distinct key so that a catalog label
   * does not overwrite an existing table property (and vice versa) and both stay visible in
   * DESCRIBE EXTENDED / SHOW TBLPROPERTIES.
   */
  private static String deconflictedKey(Map<String, String> properties, String preferredKey) {
    if (!properties.containsKey(preferredKey)) {
      return preferredKey;
    }

    String key = preferredKey + ".catalog";
    for (int suffix = 2; properties.containsKey(key); suffix++) {
      key = preferredKey + ".catalog." + suffix;
    }

    return key;
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    List<SparkMetadataColumn> cols = Lists.newArrayList();

    cols.add(SparkMetadataColumns.SPEC_ID);
    cols.add(SparkMetadataColumns.partition(table));
    cols.add(SparkMetadataColumns.FILE_PATH);
    cols.add(SparkMetadataColumns.ROW_POSITION);
    cols.add(SparkMetadataColumns.IS_DELETED);

    if (TableUtil.supportsRowLineage(table)) {
      cols.add(SparkMetadataColumns.ROW_ID);
      cols.add(SparkMetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);
    }

    return cols.toArray(SparkMetadataColumn[]::new);
  }

  private String fileFormat() {
    return table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
  }

  private String currentSnapshotId() {
    Snapshot currentSnapshot = table.currentSnapshot();
    return currentSnapshot != null ? String.valueOf(currentSnapshot.snapshotId()) : "none";
  }

  @Override
  public String toString() {
    return table.toString();
  }
}
