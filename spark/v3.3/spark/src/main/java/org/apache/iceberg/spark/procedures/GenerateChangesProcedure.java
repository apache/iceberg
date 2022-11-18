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
package org.apache.iceberg.spark.procedures;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.SparkChangelogTable;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;

public class GenerateChangesProcedure extends BaseProcedure {

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<GenerateChangesProcedure>() {
      @Override
      protected GenerateChangesProcedure doBuild() {
        return new GenerateChangesProcedure(tableCatalog());
      }
    };
  }

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        // the snapshot ids input are ignored when the start/end timestamps are provided
        ProcedureParameter.optional("start_snapshot_id_exclusive", DataTypes.LongType),
        ProcedureParameter.optional("end_snapshot_id_inclusive", DataTypes.LongType),
        ProcedureParameter.optional("table_change_view", DataTypes.StringType),
        ProcedureParameter.optional("identifier_columns", DataTypes.StringType),
        ProcedureParameter.optional("start_timestamp", DataTypes.TimestampType),
        ProcedureParameter.optional("end_timestamp", DataTypes.TimestampType),
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("view_name", DataTypes.StringType, false, Metadata.empty())
          });

  private GenerateChangesProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    String tableName = args.getString(0);

    // Read data from the table.changes
    Dataset<Row> df = changelogRecords(tableName, args);

    // Compute the pre-image and post-images if the identifier columns are provided.
    if (!args.isNullAt(4)) {
      String[] identifierColumns = args.getString(4).split(",");
      if (identifierColumns.length > 0) {
        df = withUpdate(df, identifierColumns);
      }
    }

    String viewName = viewName(args, tableName);

    // Create a view for users to query
    df.createOrReplaceTempView(viewName);

    return toOutputRows(viewName);
  }

  private Dataset<Row> changelogRecords(String tableName, InternalRow args) {
    Long[] snapshotIds = getSnapshotIds(tableName, args);

    // we don't have to validate the snapshot ids here because the reader will do it for us.
    DataFrameReader reader = spark().read();
    if (snapshotIds[0] != null) {
      reader = reader.option(SparkReadOptions.START_SNAPSHOT_ID, snapshotIds[0]);
    }

    if (snapshotIds[1] != null) {
      reader = reader.option(SparkReadOptions.END_SNAPSHOT_ID, snapshotIds[1]);
    }

    return reader.table(tableName + "." + SparkChangelogTable.TABLE_NAME);
  }

  @NotNull
  private Long[] getSnapshotIds(String tableName, InternalRow args) {
    Long[] snapshotIds = new Long[] {null, null};

    Long startTimestamp = args.isNullAt(5) ? null : DateTimeUtil.microsToMillis(args.getLong(5));
    Long endTimestamp = args.isNullAt(6) ? null : DateTimeUtil.microsToMillis(args.getLong(6));

    if (startTimestamp == null && endTimestamp == null) {
      snapshotIds[0] = args.isNullAt(1) ? null : args.getLong(1);
      snapshotIds[1] = args.isNullAt(2) ? null : args.getLong(2);
    } else {
      Identifier tableIdent = toIdentifier(tableName, PARAMETERS[0].name());
      Table table = loadSparkTable(tableIdent).table();
      Snapshot[] snapshots = snapshotsFromTimestamp(startTimestamp, endTimestamp, table);

      if (snapshots != null) {
        snapshotIds[0] = snapshots[0].parentId();
        snapshotIds[1] = snapshots[1].snapshotId();
      }
    }
    return snapshotIds;
  }

  private Snapshot[] snapshotsFromTimestamp(Long startTimestamp, Long endTimestamp, Table table) {
    Snapshot[] snapshots = new Snapshot[] {null, null};

    if (startTimestamp != null && endTimestamp != null && startTimestamp > endTimestamp) {
      throw new IllegalArgumentException(
          "Start timestamp must be less than or equal to end timestamp");
    }

    if (startTimestamp == null) {
      snapshots[0] = SnapshotUtil.oldestAncestor(table);
    } else {
      snapshots[0] = SnapshotUtil.oldestAncestorAfter(table, startTimestamp);
    }

    if (endTimestamp == null) {
      snapshots[1] = table.currentSnapshot();
    } else {
      snapshots[1] = table.snapshot(SnapshotUtil.snapshotIdAsOfTime(table, endTimestamp));
    }

    if (snapshots[0] == null || snapshots[1] == null) {
      return null;
    }

    return snapshots;
  }

  @NotNull
  private static String viewName(InternalRow args, String tableName) {
    String viewName = args.isNullAt(3) ? null : args.getString(3);
    if (viewName == null) {
      String shortTableName =
          tableName.contains(".") ? tableName.substring(tableName.lastIndexOf(".") + 1) : tableName;
      viewName = shortTableName + "_changes";
    }
    return viewName;
  }

  private Dataset<Row> withUpdate(Dataset<Row> df, String[] identifiers) {
    Column[] partitionSpec = getPartitionSpec(df, identifiers);
    Column[] sortSpec = sortSpec(df, partitionSpec);

    int changeTypeIdx = df.schema().fieldIndex(MetadataColumns.CHANGE_TYPE.name());
    List<Integer> partitionIdx =
        Arrays.stream(partitionSpec)
            .map(column -> df.schema().fieldIndex(column.toString()))
            .collect(Collectors.toList());

    return df.repartition(partitionSpec)
        .sortWithinPartitions(sortSpec)
        .mapPartitions(
            processRowsWithinTask(changeTypeIdx, partitionIdx), RowEncoder.apply(df.schema()));
  }

  @NotNull
  private static Column[] getPartitionSpec(Dataset<Row> df, String[] identifiers) {
    Column[] partitionSpec = new Column[identifiers.length + 1];
    for (int i = 0; i < identifiers.length; i++) {
      try {
        partitionSpec[i] = df.col(identifiers[i]);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("Identifier column '%s' does not exist in the table", identifiers[i]), e);
      }
    }
    partitionSpec[partitionSpec.length - 1] = df.col(MetadataColumns.CHANGE_ORDINAL.name());
    return partitionSpec;
  }

  @NotNull
  private static Column[] sortSpec(Dataset<Row> df, Column[] partitionSpec) {
    Column[] sortSpec = new Column[partitionSpec.length + 1];
    System.arraycopy(partitionSpec, 0, sortSpec, 0, partitionSpec.length);
    sortSpec[sortSpec.length - 1] = df.col(MetadataColumns.CHANGE_TYPE.name());
    return sortSpec;
  }

  private static MapPartitionsFunction<Row, Row> processRowsWithinTask(
      int changeTypeIndex, List<Integer> partitionIdx) {
    return rowIterator -> {
      Iterator<Row> iterator =
          new Iterator<Row>() {
            private Row nextRow = null;

            @Override
            public boolean hasNext() {
              if (nextRow != null) {
                return true;
              }
              return rowIterator.hasNext();
            }

            @Override
            public Row next() {
              String delete = ChangelogOperation.DELETE.name();
              String insert = ChangelogOperation.INSERT.name();

              // if the next row is cached and changed, return it directly
              if (nextRow != null
                  && !nextRow.getString(changeTypeIndex).equals(delete)
                  && !nextRow.getString(changeTypeIndex).equals(insert)) {
                Row row = nextRow;
                nextRow = null;
                return row;
              }

              Row currentRow = currentRow();

              if (rowIterator.hasNext()) {
                GenericRowWithSchema nextRow = (GenericRowWithSchema) rowIterator.next();

                if (withinPartition(currentRow, nextRow)
                    && currentRow.getString(changeTypeIndex).equals(delete)
                    && nextRow.getString(changeTypeIndex).equals(insert)) {

                  GenericInternalRow deletedRow =
                      new GenericInternalRow(((GenericRowWithSchema) currentRow).values());
                  GenericInternalRow insertedRow = new GenericInternalRow(nextRow.values());

                  // set the change_type to the same value
                  deletedRow.update(changeTypeIndex, "");
                  insertedRow.update(changeTypeIndex, "");

                  if (deletedRow.equals(insertedRow)) {
                    // remove two carry-over rows
                    currentRow = null;
                    this.nextRow = null;
                  } else {
                    deletedRow.update(changeTypeIndex, ChangelogOperation.UPDATE_BEFORE.name());
                    currentRow = RowFactory.create(deletedRow.values());

                    insertedRow.update(changeTypeIndex, ChangelogOperation.UPDATE_AFTER.name());
                    this.nextRow = RowFactory.create(insertedRow.values());
                  }
                } else {
                  this.nextRow = nextRow;
                }
              }

              return currentRow;
            }

            private Row currentRow() {
              if (nextRow != null) {
                Row row = nextRow;
                nextRow = null;
                return row;
              } else {
                return rowIterator.next();
              }
            }

            private boolean withinPartition(Row currentRow, Row nextRow) {
              for (int i = 0; i < partitionIdx.size(); i++) {
                int idx = partitionIdx.get(i);
                if (!nextRow.get(idx).equals(currentRow.get(idx))) {
                  return false;
                }
              }
              return true;
            }
          };

      return Iterators.filter(iterator, Objects::nonNull);
    };
  }

  private InternalRow[] toOutputRows(String viewName) {
    InternalRow row = newInternalRow(UTF8String.fromString(viewName));
    return new InternalRow[] {row};
  }

  @Override
  public String description() {
    return "GenerateChangesProcedure";
  }
}
