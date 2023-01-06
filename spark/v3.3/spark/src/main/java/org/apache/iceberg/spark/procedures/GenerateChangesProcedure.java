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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.ChangelogIterator;
import org.apache.iceberg.spark.source.SparkChangelogTable;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
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
 * <p>The procedure computes updated rows and removes the carry-over rows by default. You can
 * disable them through parameters to get better performance. A pair of carry-over rows exist due to
 * an unchanged row is rewritten by Iceberg. Their values are the same except one is marked as
 * deleted and the other is marked as inserted.
 *
 * <p>Identifier columns are needed for updated rows computation. You can either set Identifier
 * Field IDs as the table properties or input them as the procedure parameters.
 */
public class GenerateChangesProcedure extends BaseProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(GenerateChangesProcedure.class);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("table_change_view", DataTypes.StringType),
        ProcedureParameter.optional("options", STRING_MAP),
        ProcedureParameter.optional("compute_updated_row", DataTypes.BooleanType),
        ProcedureParameter.optional("remove_carried_over_row", DataTypes.BooleanType),
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

    boolean removeCarryoverRow = args.isNullAt(4) ? true : args.getBoolean(4);
    boolean computeUpdatedRow = args.isNullAt(3) ? true : args.getBoolean(3);

    if (computeUpdatedRow) {
      if (hasIdentifierColumns(args)) {
        String[] identifierColumns = args.getString(5).split(",");

        if (identifierColumns.length > 0) {
          Column[] partitionSpec = getPartitionSpec(df, identifierColumns);
          df = transform(df, partitionSpec, false);
        }
      } else {
        LOG.warn("Cannot compute the updated rows because identifier columns are not set");
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
    Column[] partitionSpec =
        Arrays.stream(df.columns())
            .filter(c -> !c.equals(MetadataColumns.CHANGE_TYPE.name()))
            .map(df::col)
            .toArray(Column[]::new);
    return transform(df, partitionSpec, true);
  }

  private boolean hasIdentifierColumns(InternalRow args) {
    return !args.isNullAt(5) && !args.getString(5).isEmpty();
  }

  private Dataset<Row> changelogRecords(String tableName, InternalRow args) {
    // no need to validate the read options here since the reader will validate them
    DataFrameReader reader = spark().read();
    reader.options(readOptions(args));
    return reader.table(tableName + "." + SparkChangelogTable.TABLE_NAME);
  }

  private Map<String, String> readOptions(InternalRow args) {
    Map<String, String> options = Maps.newHashMap();

    // todo: convert timestamp to milliseconds
    if (!args.isNullAt(2)) {
      args.getMap(2)
          .foreach(
              DataTypes.StringType,
              DataTypes.StringType,
              (k, v) -> {
                options.put(k.toString(), v.toString());
                return BoxedUnit.UNIT;
              });
    }

    return options;
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

  private Dataset<Row> transform(
      Dataset<Row> df, Column[] partitionSpec, boolean partitionedByAllColumns) {
    Column[] sortSpec = sortSpec(df, partitionSpec);

    int changeTypeIdx = df.schema().fieldIndex(MetadataColumns.CHANGE_TYPE.name());
    List<Integer> partitionIdx =
        Arrays.stream(partitionSpec)
            .map(column -> df.schema().fieldIndex(column.toString()))
            .collect(Collectors.toList());

    return df.repartition(partitionSpec)
        .sortWithinPartitions(sortSpec)
        .mapPartitions(
            (MapPartitionsFunction<Row, Row>)
                rowIterator -> ChangelogIterator.iterator(rowIterator, changeTypeIdx, partitionIdx),
            RowEncoder.apply(df.schema()));
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

  private InternalRow[] toOutputRows(String viewName) {
    InternalRow row = newInternalRow(UTF8String.fromString(viewName));
    return new InternalRow[] {row};
  }

  @Override
  public String description() {
    return "GenerateChangesProcedure";
  }
}
