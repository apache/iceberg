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
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.ChangelogIterator;
import org.apache.iceberg.spark.source.SparkChangelogTable;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
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
import scala.runtime.BoxedUnit;

/**
 * A procedure that creates a view for changed rows.
 *
 * <p>The procedure removes the carry-over rows by default. If you want to keep them, you can set
 * "remove_carryovers" to be false in the options.
 *
 * <p>The procedure doesn't compute the pre/post update images by default. If you want to compute
 * them, you can set "compute_updates" to be true in the options.
 *
 * <p>Carry-over rows are the result of a removal and insertion of the same row within an operation
 * because of the copy-on-write mechanism. For example, given a file which contains row1 (id=1,
 * data='a') and row2 (id=2, data='b'). A copy-on-write delete of row2 would require erasing this
 * file and preserving row1 in a new file. The changelog table would report this as (id=1, data='a',
 * op='DELETE') and (id=1, data='a', op='INSERT'), despite it not being an actual change to the
 * table. The procedure finds the carry-over rows and removes them from the result.
 *
 * <p>Pre/post update images are converted from a pair of a delete row and an insert row. Identifier
 * columns are used for determining whether an insert and a delete record refer to the same row. If
 * the two records share the same values for the identity columns they are considered to be before
 * and after states of the same row. You can either set identifier fields in the table schema or
 * input them as the procedure parameters. Here is an example of pre/post update images with an
 * identifier column(id). A pair of a delete row and an insert row with the same id:
 *
 * <ul>
 *   <li>(id=1, data='a', op='DELETE')
 *   <li>(id=1, data='b', op='INSERT')
 * </ul>
 *
 * <p>will be marked as pre/post update images:
 *
 * <ul>
 *   <li>(id=1, data='a', op='UPDATE_BEFORE')
 *   <li>(id=1, data='b', op='UPDATE_AFTER')
 * </ul>
 */
public class CreateChangelogViewProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("changelog_view", DataTypes.StringType),
        ProcedureParameter.optional("options", STRING_MAP),
        ProcedureParameter.optional("compute_updates", DataTypes.BooleanType),
        ProcedureParameter.optional("remove_carryovers", DataTypes.BooleanType),
        ProcedureParameter.optional("identifier_columns", STRING_ARRAY),
      };

  private static final int TABLE_NAME_ORDINAL = 0;
  private static final int CHANGELOG_VIEW_NAME_ORDINAL = 1;
  private static final int OPTIONS_ORDINAL = 2;
  private static final int COMPUTE_UPDATES_ORDINAL = 3;
  private static final int REMOVE_CARRYOVERS_ORDINAL = 4;
  private static final int IDENTIFIER_COLUMNS_ORDINAL = 5;

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("changelog_view", DataTypes.StringType, false, Metadata.empty())
          });

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<CreateChangelogViewProcedure>() {
      @Override
      protected CreateChangelogViewProcedure doBuild() {
        return new CreateChangelogViewProcedure(tableCatalog());
      }
    };
  }

  private CreateChangelogViewProcedure(TableCatalog tableCatalog) {
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
    Identifier tableIdent =
        toIdentifier(args.getString(TABLE_NAME_ORDINAL), PARAMETERS[TABLE_NAME_ORDINAL].name());

    // load insert and deletes from the changelog table
    Identifier changelogTableIdent = changelogTableIdent(tableIdent);
    Dataset<Row> df = loadDataSetFromTable(changelogTableIdent, options(args));

    if (shouldComputeUpdateImages(args)) {
      df = computeUpdateImages(identifierColumns(args, tableIdent), df);
    } else if (shouldRemoveCarryoverRows(args)) {
      df = removeCarryoverRows(df);
    }

    String viewName = viewName(args, tableIdent.name());

    df.createOrReplaceTempView(viewName);

    return toOutputRows(viewName);
  }

  private Dataset<Row> computeUpdateImages(String[] identifierColumns, Dataset<Row> df) {
    Preconditions.checkArgument(
        identifierColumns.length > 0,
        "Cannot compute the update-rows because identifier columns are not set");

    Column[] repartitionColumns = new Column[identifierColumns.length + 1];
    for (int i = 0; i < identifierColumns.length; i++) {
      repartitionColumns[i] = df.col(identifierColumns[i]);
    }
    repartitionColumns[repartitionColumns.length - 1] =
        df.col(MetadataColumns.CHANGE_ORDINAL.name());

    return applyChangelogIterator(df, repartitionColumns);
  }

  private boolean shouldComputeUpdateImages(InternalRow args) {
    if (!args.isNullAt(COMPUTE_UPDATES_ORDINAL)) {
      return args.getBoolean(COMPUTE_UPDATES_ORDINAL);
    } else {
      // If the identifier columns are set, we compute pre/post update images by default.
      return !args.isNullAt(IDENTIFIER_COLUMNS_ORDINAL);
    }
  }

  private boolean shouldRemoveCarryoverRows(InternalRow args) {
    if (args.isNullAt(REMOVE_CARRYOVERS_ORDINAL)) {
      return true;
    } else {
      return args.getBoolean(REMOVE_CARRYOVERS_ORDINAL);
    }
  }

  private Dataset<Row> removeCarryoverRows(Dataset<Row> df) {
    Column[] repartitionColumns =
        Arrays.stream(df.columns())
            .filter(c -> !c.equals(MetadataColumns.CHANGE_TYPE.name()))
            .map(df::col)
            .toArray(Column[]::new);
    return applyChangelogIterator(df, repartitionColumns);
  }

  private String[] identifierColumns(InternalRow args, Identifier tableIdent) {
    if (!args.isNullAt(IDENTIFIER_COLUMNS_ORDINAL)) {
      return Arrays.stream(args.getArray(IDENTIFIER_COLUMNS_ORDINAL).array())
          .map(column -> column.toString())
          .toArray(String[]::new);
    } else {
      Table table = loadSparkTable(tableIdent).table();
      return table.schema().identifierFieldNames().toArray(new String[0]);
    }
  }

  private Identifier changelogTableIdent(Identifier tableIdent) {
    List<String> namespace = Lists.newArrayList();
    namespace.addAll(Arrays.asList(tableIdent.namespace()));
    namespace.add(tableIdent.name());
    return Identifier.of(namespace.toArray(new String[0]), SparkChangelogTable.TABLE_NAME);
  }

  private Map<String, String> options(InternalRow args) {
    Map<String, String> options = Maps.newHashMap();

    if (!args.isNullAt(OPTIONS_ORDINAL)) {
      args.getMap(OPTIONS_ORDINAL)
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

  private static String viewName(InternalRow args, String tableName) {
    if (args.isNullAt(CHANGELOG_VIEW_NAME_ORDINAL)) {
      return String.format("`%s_changes`", tableName);
    } else {
      return args.getString(CHANGELOG_VIEW_NAME_ORDINAL);
    }
  }

  private Dataset<Row> applyChangelogIterator(Dataset<Row> df, Column[] repartitionColumns) {
    Column[] sortSpec = sortSpec(df, repartitionColumns);
    StructType schema = df.schema();
    String[] identifierFields =
        Arrays.stream(repartitionColumns).map(Column::toString).toArray(String[]::new);

    return df.repartition(repartitionColumns)
        .sortWithinPartitions(sortSpec)
        .mapPartitions(
            (MapPartitionsFunction<Row, Row>)
                rowIterator -> ChangelogIterator.create(rowIterator, schema, identifierFields),
            RowEncoder.apply(schema));
  }

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
    return "CreateChangelogViewProcedure";
  }
}
