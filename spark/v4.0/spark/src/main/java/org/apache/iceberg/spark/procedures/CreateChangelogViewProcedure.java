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
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.ChangelogIterator;
import org.apache.iceberg.spark.source.SparkChangelogTable;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.OrderUtils;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure;
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A procedure that creates a view for changed rows.
 *
 * <p>The procedure always removes the carry-over rows. Please query {@link SparkChangelogTable}
 * instead when carry-over rows are required.
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

  static final String NAME = "create_changelog_view";

  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter CHANGELOG_VIEW_PARAM =
      optionalInParameter("changelog_view", DataTypes.StringType);
  private static final ProcedureParameter OPTIONS_PARAM =
      optionalInParameter("options", STRING_MAP);
  private static final ProcedureParameter COMPUTE_UPDATES_PARAM =
      optionalInParameter("compute_updates", DataTypes.BooleanType);
  private static final ProcedureParameter IDENTIFIER_COLUMNS_PARAM =
      optionalInParameter("identifier_columns", STRING_ARRAY);
  private static final ProcedureParameter NET_CHANGES =
      optionalInParameter("net_changes", DataTypes.BooleanType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        TABLE_PARAM,
        CHANGELOG_VIEW_PARAM,
        OPTIONS_PARAM,
        COMPUTE_UPDATES_PARAM,
        IDENTIFIER_COLUMNS_PARAM,
        NET_CHANGES,
      };

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
  public BoundProcedure bind(StructType inputType) {
    return this;
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public Iterator<Scan> call(InternalRow args) {
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);

    Identifier tableIdent = input.ident(TABLE_PARAM);

    // load insert and deletes from the changelog table
    Identifier changelogTableIdent = changelogTableIdent(tableIdent);
    Dataset<Row> df = loadRows(changelogTableIdent, options(input));

    boolean netChanges = input.asBoolean(NET_CHANGES, false);
    String[] identifierColumns = identifierColumns(input, tableIdent);
    Set<String> unorderableColumnNames =
        Arrays.stream(df.schema().fields())
            .filter(field -> !OrderUtils.isOrderable(field.dataType()))
            .map(StructField::name)
            .collect(Collectors.toSet());

    Preconditions.checkArgument(
        identifierColumns.length > 0 || unorderableColumnNames.isEmpty(),
        "Identifier field is required as table contains unorderable columns: %s",
        unorderableColumnNames);

    if (shouldComputeUpdateImages(input)) {
      Preconditions.checkArgument(!netChanges, "Not support net changes with update images");
      df = computeUpdateImages(identifierColumns, df);
    } else {
      df = removeCarryoverRows(df, netChanges);
    }

    String viewName = viewName(input, tableIdent.name());

    df.createOrReplaceTempView(viewName);

    return asScanIterator(OUTPUT_TYPE, toOutputRows(viewName));
  }

  private Dataset<Row> computeUpdateImages(String[] identifierColumns, Dataset<Row> df) {
    Preconditions.checkArgument(
        identifierColumns.length > 0,
        "Cannot compute the update images because identifier columns are not set");

    Column[] repartitionSpec = new Column[identifierColumns.length + 1];
    for (int i = 0; i < identifierColumns.length; i++) {
      repartitionSpec[i] = df.col(CreateChangelogViewProcedure.delimitedName(identifierColumns[i]));
    }

    repartitionSpec[repartitionSpec.length - 1] = df.col(MetadataColumns.CHANGE_ORDINAL.name());

    String[] identifierFields = Arrays.copyOf(identifierColumns, identifierColumns.length + 1);
    identifierFields[identifierFields.length - 1] = MetadataColumns.CHANGE_ORDINAL.name();

    return applyChangelogIterator(df, repartitionSpec, identifierFields);
  }

  private boolean shouldComputeUpdateImages(ProcedureInput input) {
    // If the identifier columns are set, we compute pre/post update images by default.
    boolean defaultValue = input.isProvided(IDENTIFIER_COLUMNS_PARAM);
    return input.asBoolean(COMPUTE_UPDATES_PARAM, defaultValue);
  }

  private Dataset<Row> removeCarryoverRows(Dataset<Row> df, boolean netChanges) {
    Predicate<String> columnsToKeep;
    if (netChanges) {
      Set<String> metadataColumn =
          Sets.newHashSet(
              MetadataColumns.CHANGE_TYPE.name(),
              MetadataColumns.CHANGE_ORDINAL.name(),
              MetadataColumns.COMMIT_SNAPSHOT_ID.name());

      columnsToKeep = column -> !metadataColumn.contains(column);
    } else {
      columnsToKeep = column -> !column.equals(MetadataColumns.CHANGE_TYPE.name());
    }

    Column[] repartitionSpec =
        Arrays.stream(df.columns())
            .filter(columnsToKeep)
            .map(CreateChangelogViewProcedure::delimitedName)
            .map(df::col)
            .toArray(Column[]::new);

    return applyCarryoverRemoveIterator(df, repartitionSpec, netChanges);
  }

  private String[] identifierColumns(ProcedureInput input, Identifier tableIdent) {
    if (input.isProvided(IDENTIFIER_COLUMNS_PARAM)) {
      return input.asStringArray(IDENTIFIER_COLUMNS_PARAM);
    } else {
      Table table = loadSparkTable(tableIdent).table();
      return table.schema().identifierFieldNames().stream().toArray(String[]::new);
    }
  }

  private Identifier changelogTableIdent(Identifier tableIdent) {
    List<String> namespace = Lists.newArrayList();
    namespace.addAll(Arrays.asList(tableIdent.namespace()));
    namespace.add(tableIdent.name());
    return Identifier.of(namespace.toArray(new String[0]), SparkChangelogTable.TABLE_NAME);
  }

  private Map<String, String> options(ProcedureInput input) {
    return input.asStringMap(OPTIONS_PARAM, ImmutableMap.of());
  }

  private String viewName(ProcedureInput input, String tableName) {
    String defaultValue = String.format("`%s_changes`", tableName);
    return input.asString(CHANGELOG_VIEW_PARAM, defaultValue);
  }

  private Dataset<Row> applyChangelogIterator(
      Dataset<Row> df, Column[] repartitionSpec, String[] identifierFields) {
    Column[] sortSpec = sortSpec(df, repartitionSpec, false);
    StructType schema = df.schema();

    return df.repartition(repartitionSpec)
        .sortWithinPartitions(sortSpec)
        .mapPartitions(
            (MapPartitionsFunction<Row, Row>)
                rowIterator ->
                    ChangelogIterator.computeUpdates(rowIterator, schema, identifierFields),
            Encoders.row(schema));
  }

  private Dataset<Row> applyCarryoverRemoveIterator(
      Dataset<Row> df, Column[] repartitionSpec, boolean netChanges) {
    Column[] sortSpec = sortSpec(df, repartitionSpec, netChanges);
    StructType schema = df.schema();

    return df.repartition(repartitionSpec)
        .sortWithinPartitions(sortSpec)
        .mapPartitions(
            (MapPartitionsFunction<Row, Row>)
                rowIterator ->
                    netChanges
                        ? ChangelogIterator.removeNetCarryovers(rowIterator, schema)
                        : ChangelogIterator.removeCarryovers(rowIterator, schema),
            Encoders.row(schema));
  }

  /**
   * Ensure that column can be referenced using this name. Issues may come from field names that
   * contain non-standard characters. In Spark, this can be fixed by using <a
   * href="https://spark.apache.org/docs/3.5.0/sql-ref-identifier.html#delimited-identifier">backtick
   * quotes</a>.
   *
   * @param columnName Column name that potentially can contain non-standard characters.
   * @return A name that can be safely used within Spark to reference a column by its name.
   */
  private static String delimitedName(String columnName) {
    boolean delimited = columnName.startsWith("`") && columnName.endsWith("`");
    if (delimited) {
      return columnName;
    } else {
      return "`" + columnName.replaceAll("`", "``") + "`";
    }
  }

  private static Column[] sortSpec(Dataset<Row> df, Column[] repartitionSpec, boolean netChanges) {
    Column changeType = df.col(MetadataColumns.CHANGE_TYPE.name());
    Column changeOrdinal = df.col(MetadataColumns.CHANGE_ORDINAL.name());
    Column[] extraColumns =
        netChanges ? new Column[] {changeOrdinal, changeType} : new Column[] {changeType};

    Column[] sortSpec = new Column[repartitionSpec.length + extraColumns.length];

    System.arraycopy(repartitionSpec, 0, sortSpec, 0, repartitionSpec.length);
    System.arraycopy(extraColumns, 0, sortSpec, repartitionSpec.length, extraColumns.length);

    return sortSpec;
  }

  private InternalRow[] toOutputRows(String viewName) {
    InternalRow row = newInternalRow(UTF8String.fromString(viewName));
    return new InternalRow[] {row};
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "CreateChangelogViewProcedure";
  }
}
