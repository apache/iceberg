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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.SparkChangelogTable;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
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
        df = withUpdate2(df, identifierColumns);
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

  private Dataset<Row> withUpdate2(Dataset<Row> df, String[] identifiers) {
    Column[] partitionSpec = getPartitionSpec(df, identifiers);
    return df.repartition(partitionSpec)
        .mapPartitions(toUpdateRows(), RowEncoder.apply(df.schema()));
  }

  private static MapPartitionsFunction<Row, Row> toUpdateRows() {
    return rows -> {
      List<Row> rowsList = Lists.newArrayList(rows);
      if (rowsList.size() != 2) {
        return rows;
      }

      int changeTypeIndex = rowsList.get(0).fieldIndex(MetadataColumns.CHANGE_TYPE.name());
      String changeType0 = rowsList.get(0).getString(changeTypeIndex);
      String changeType1 = rowsList.get(1).getString(changeTypeIndex);
      String delete = ChangelogOperation.DELETE.name();
      String insert = ChangelogOperation.INSERT.name();

      if (changeType0.equals(delete) && changeType1.equals(insert)) {
        return updateRows(
            (InternalRow) rowsList.get(0), (InternalRow) rowsList.get(1), changeTypeIndex);
      } else if (changeType0.equals(insert) && changeType1.equals(delete)) {
        return updateRows(
            (InternalRow) rowsList.get(1), (InternalRow) rowsList.get(0), changeTypeIndex);
      } else {
        throw new RuntimeException("Unexpected change type: " + changeType0 + ", " + changeType1);
      }
    };
  }

  private static Iterator<Row> updateRows(
      InternalRow deletedRow, InternalRow insertedRow, int changeTypeIndex) {
    deletedRow.update(changeTypeIndex, "");
    insertedRow.update(changeTypeIndex, "");

    if (deletedRow.equals(insertedRow)) {
      // remove the carry over row
      return Collections.emptyIterator();
    } else {
      deletedRow.update(changeTypeIndex, ChangelogOperation.UPDATE_BEFORE.name());
      insertedRow.update(changeTypeIndex, ChangelogOperation.UPDATE_AFTER.name());
      return Lists.newArrayList((Row) deletedRow, (Row) insertedRow).iterator();
    }
  }

  private Dataset<Row> withUpdate(Dataset<Row> df, String[] identifiers) {
    Column[] partitionSpec = getPartitionSpec(df, identifiers);
    String countCol = UUID.randomUUID().toString().replace("-", "");
    String rankCol = UUID.randomUUID().toString().replace("-", "");

    Dataset<Row> dfWithUpdate =
        df.withColumn(countCol, functions.count("*").over(Window.partitionBy(partitionSpec)))
            .withColumn(
                rankCol,
                functions
                    .rank()
                    .over(
                        Window.partitionBy(partitionSpec)
                            .orderBy(MetadataColumns.CHANGE_TYPE.name())));

    Dataset<Row> preImageDf =
        dfWithUpdate
            .filter(rankCol + " = 1")
            .filter(countCol + " = 2")
            .drop(rankCol, countCol)
            .withColumn(
                MetadataColumns.CHANGE_TYPE.name(),
                functions.lit(ChangelogOperation.UPDATE_BEFORE.name()));

    Dataset<Row> postImageDf =
        dfWithUpdate
            .filter(rankCol + " = 2")
            .filter(countCol + " = 2")
            .drop(rankCol, countCol)
            .withColumn(
                MetadataColumns.CHANGE_TYPE.name(),
                functions.lit(ChangelogOperation.UPDATE_AFTER.name()));

    // remove the carry-over rows
    Dataset<Row> dfWithoutCarryOver = removeCarryOvers(preImageDf.union(postImageDf));

    // should we throw an exception if count > 2?
    Dataset<Row> othersDf = dfWithUpdate.filter(countCol + " != 2").drop(rankCol, countCol);

    return dfWithoutCarryOver.union(othersDf);
  }

  private Dataset<Row> removeCarryOvers(Dataset<Row> df) {
    Column[] partitionSpec =
        Arrays.stream(df.columns())
            .filter(c -> !c.equals(MetadataColumns.CHANGE_TYPE.name()))
            .map(df::col)
            .toArray(Column[]::new);

    String countCol = UUID.randomUUID().toString().replace("-", "");
    Dataset<Row> dfWithCount =
        df.withColumn(countCol, functions.count("*").over(Window.partitionBy(partitionSpec)));

    return dfWithCount.filter(countCol + " = 1").drop(countCol);
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

  private InternalRow[] toOutputRows(String viewName) {
    InternalRow row = newInternalRow(UTF8String.fromString(viewName));
    return new InternalRow[] {row};
  }

  @Override
  public String description() {
    return "GenerateChangesProcedure";
  }
}
