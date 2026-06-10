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
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

abstract class BaseSparkTable
    implements org.apache.spark.sql.connector.catalog.Table, SupportsMetadataColumns {

  private static final String PROVIDER = "provider";
  private static final String FORMAT = "format";
  private static final String LOCATION = "location";
  private static final String SORT_ORDER = "sort-order";
  private static final String IDENTIFIER_FIELDS = "identifier-fields";
  private static final Set<String> RESERVED_PROPERTIES =
      ImmutableSet.of(
          PROVIDER,
          FORMAT,
          CURRENT_SNAPSHOT_ID,
          LOCATION,
          FORMAT_VERSION,
          SORT_ORDER,
          IDENTIFIER_FIELDS);

  private final Table table;
  private final Schema schema;

  private SparkSession lazySpark = null;
  private StructType lazySparkSchema = null;

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

  @Override
  public StructType schema() {
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
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();

    propsBuilder.put(FORMAT, "iceberg/" + fileFormat());
    propsBuilder.put(PROVIDER, "iceberg");
    propsBuilder.put(LOCATION, table.location());
    propsBuilder.put(CURRENT_SNAPSHOT_ID, currentSnapshotId());

    if (table instanceof BaseTable) {
      TableOperations ops = ((BaseTable) table).operations();
      propsBuilder.put(FORMAT_VERSION, String.valueOf(ops.current().formatVersion()));
    }

    if (table.sortOrder().isSorted()) {
      propsBuilder.put(SORT_ORDER, Spark3Util.describe(table.sortOrder()));
    }

    Set<String> identifierFields = table.schema().identifierFieldNames();
    if (!identifierFields.isEmpty()) {
      propsBuilder.put(IDENTIFIER_FIELDS, "[" + String.join(",", identifierFields) + "]");
    }

    table.properties().entrySet().stream()
        .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
        .forEach(propsBuilder::put);

    return propsBuilder.build();
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
