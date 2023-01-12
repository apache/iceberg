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

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.ChangelogIterator;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.SparkChangelogTable;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

/**
 * A procedure that creates a view for changed rows.
 *
 * <p>The procedure computes update-rows and removes the carry-over rows by default. You can disable
 * them through parameters to get better performance.
 *
 * <p>Carry-over rows are the result of a removal and insertion of the same row within an operation
 * because of the copy-on-write mechanism. For example, given a file which contains row1 (id=1,
 * data='a') and row2 (id=2, data='b'). A copy-on-write delete of row2 would require erasing this
 * file and preserving row1 in a new file. The change-log table would report this as (id=1,
 * data='a', op='DELETE') and (id=1, data='a', op='INSERT'), despite it not being an actual change
 * to the table. The iterator finds the carry-over rows and removes them from the result.
 *
 * <p>An update-row is converted from a pair of delete row and insert row. Identifier columns are
 * needed for identifying whether they refer to the same row. You can either set Identifier Field
 * IDs as the table properties or input them as the procedure parameters. Here is an example of
 * update-row with an identifier column(id). A pair of delete row and insert row with the same id:
 *
 * <ul>
 *   <li>(id=1, data='a', op='DELETE')
 *   <li>(id=1, data='b', op='INSERT')
 * </ul>
 *
 * <p>will be marked as update-rows:
 *
 * <ul>
 *   <li>(id=1, data='a', op='UPDATE_BEFORE')
 *   <li>(id=1, data='b', op='UPDATE_AFTER')
 * </ul>
 */
public class GenerateChangesProcedure extends BaseProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(GenerateChangesProcedure.class);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("table_change_view", DataTypes.StringType),
        ProcedureParameter.optional("options", STRING_MAP),
        ProcedureParameter.optional("compute_updates", DataTypes.BooleanType),
        ProcedureParameter.optional("remove_carryovers", DataTypes.BooleanType),
        ProcedureParameter.optional("identifier_columns", DataTypes.StringType),
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("view_name", DataTypes.StringType, false, Metadata.empty())
          });

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<GenerateChangesProcedure>() {
      @Override
      protected GenerateChangesProcedure doBuild() {
        return new GenerateChangesProcedure(tableCatalog());
      }
    };
  }

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

    // compute updated rows and remove carry-over rows by default
    boolean computeUpdatedRow = args.isNullAt(3) ? true : args.getBoolean(3);
    boolean removeCarryoverRow = args.isNullAt(4) ? true : args.getBoolean(4);

    if (computeUpdatedRow) {
      String[] identifierColumns = identifierColumns(args, tableName);

      if (identifierColumns.length > 0) {
        Column[] repartitionColumns = getRepartitionExpr(df, identifierColumns);
        df = transform(df, repartitionColumns);
      } else {
        LOG.warn("Cannot compute the update-rows because identifier columns are not set");
        if (removeCarryoverRow) {
          df = removeCarryoverRows(df);
        }
      }
    } else if (removeCarryoverRow) {
      df = removeCarryoverRows(df);
    }

    String viewName = viewName(args, tableName);

    // Create a view for users to query
    df.createOrReplaceTempView(viewName);

    return toOutputRows(viewName);
  }

  private Dataset<Row> removeCarryoverRows(Dataset<Row> df) {
    Column[] repartitionColumns =
        Arrays.stream(df.columns())
            .filter(c -> !c.equals(MetadataColumns.CHANGE_TYPE.name()))
            .map(df::col)
            .toArray(Column[]::new);
    return transform(df, repartitionColumns);
  }

  private String[] identifierColumns(InternalRow args, String tableName) {
    String[] identifierColumns = new String[0];
    if (!args.isNullAt(5) && !args.getString(5).isEmpty()) {
      identifierColumns = args.getString(5).split(",");
    }

    if (identifierColumns.length == 0) {
      Identifier tableIdent = toIdentifier(tableName, PARAMETERS[0].name());
      Table table = loadSparkTable(tableIdent).table();
      identifierColumns = table.schema().identifierFieldNames().toArray(new String[0]);
    }

    return identifierColumns;
  }

  private Dataset<Row> changelogRecords(String tableName, InternalRow args) {
    // no need to validate the read options here since the reader will validate them
    DataFrameReader reader = spark().read();
    reader.options(readOptions(args));
    return reader.table(tableName + "." + SparkChangelogTable.TABLE_NAME);
  }

  private Map<String, String> readOptions(InternalRow args) {
    Map<String, String> options = Maps.newHashMap();

    if (!args.isNullAt(2)) {
      args.getMap(2)
          .foreach(
              DataTypes.StringType,
              DataTypes.StringType,
              (k, v) -> {
                if (k.toString().equals(SparkReadOptions.START_TIMESTAMP)
                    || k.toString().equals(SparkReadOptions.END_TIMESTAMP)) {
                  options.put(k.toString(), toMillis(v.toString()));
                } else {
                  options.put(k.toString(), v.toString());
                }
                return BoxedUnit.UNIT;
              });
    }

    return options;
  }

  private static String toMillis(String timestamp) {
    return String.valueOf(Timestamp.valueOf(timestamp).getTime());
  }

  @NotNull
  private static String viewName(InternalRow args, String tableName) {
    String viewName = args.isNullAt(1) ? null : args.getString(1);
    if (viewName == null) {
      String shortTableName =
          tableName.contains(".") ? tableName.substring(tableName.lastIndexOf(".") + 1) : tableName;
      viewName = shortTableName + "_changes";
    }
    return viewName;
  }

  private Dataset<Row> transform(Dataset<Row> df, Column[] repartitionColumns) {
    Column[] sortSpec = sortSpec(df, repartitionColumns);

    int changeTypeIdx = df.schema().fieldIndex(MetadataColumns.CHANGE_TYPE.name());

    List<Integer> repartitionIdx =
        Arrays.stream(repartitionColumns)
            .map(column -> df.schema().fieldIndex(column.toString()))
            .collect(Collectors.toList());

    return df.repartition(repartitionColumns)
        .sortWithinPartitions(sortSpec)
        .mapPartitions(
            (MapPartitionsFunction<Row, Row>)
                rowIterator ->
                    ChangelogIterator.iterator(rowIterator, changeTypeIdx, repartitionIdx),
            RowEncoder.apply(df.schema()));
  }

  @NotNull
  private static Column[] getRepartitionExpr(Dataset<Row> df, String[] identifiers) {
    Column[] repartitionSpec = new Column[identifiers.length + 1];
    for (int i = 0; i < identifiers.length; i++) {
      try {
        repartitionSpec[i] = df.col(identifiers[i]);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("Identifier column '%s' does not exist in the table", identifiers[i]), e);
      }
    }
    repartitionSpec[repartitionSpec.length - 1] = df.col(MetadataColumns.CHANGE_ORDINAL.name());
    return repartitionSpec;
  }

  @NotNull
  private static Column[] sortSpec(Dataset<Row> df, Column[] repartitionSpec) {
    Column[] sortSpec = new Column[repartitionSpec.length + 1];
    System.arraycopy(repartitionSpec, 0, sortSpec, 0, repartitionSpec.length);
    sortSpec[sortSpec.length - 1] = df.col(MetadataColumns.CHANGE_TYPE.name());
    return sortSpec;
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
