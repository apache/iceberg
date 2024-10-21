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

import static org.apache.iceberg.actions.RewriteDataFiles.OUTPUT_SPEC_ID;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Zorder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.ExtendedParser;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.util.PartitionSpecUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A procedure that rewrites datafiles in a table.
 *
 * @see org.apache.iceberg.spark.actions.SparkActions#rewriteDataFiles(Table)
 */
class RewriteDataFilesProcedure extends BaseProcedure {

  private static final ProcedureParameter TABLE_PARAM =
      ProcedureParameter.required("table", DataTypes.StringType);
  private static final ProcedureParameter STRATEGY_PARAM =
      ProcedureParameter.optional("strategy", DataTypes.StringType);
  private static final ProcedureParameter SORT_ORDER_PARAM =
      ProcedureParameter.optional("sort_order", DataTypes.StringType);
  private static final ProcedureParameter OPTIONS_PARAM =
      ProcedureParameter.optional("options", STRING_MAP);
  private static final ProcedureParameter WHERE_PARAM =
      ProcedureParameter.optional("where", DataTypes.StringType);
  private static final ProcedureParameter PARTITIONED_BY_PARAM =
      ProcedureParameter.optional("partitioned_by", DataTypes.StringType);
  private static final ProcedureParameter CREATE_PARTITION_IF_NOT_EXISTS_PARAM =
      ProcedureParameter.optional("create_partition_if_not_exists", DataTypes.BooleanType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        TABLE_PARAM,
        STRATEGY_PARAM,
        SORT_ORDER_PARAM,
        OPTIONS_PARAM,
        WHERE_PARAM,
        PARTITIONED_BY_PARAM,
        CREATE_PARTITION_IF_NOT_EXISTS_PARAM
      };

  // counts are not nullable since the action result is never null
  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(
                "rewritten_data_files_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(
                "added_data_files_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("rewritten_bytes_count", DataTypes.LongType, false, Metadata.empty()),
            new StructField(
                "failed_data_files_count", DataTypes.IntegerType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new Builder<RewriteDataFilesProcedure>() {
      @Override
      protected RewriteDataFilesProcedure doBuild() {
        return new RewriteDataFilesProcedure(tableCatalog());
      }
    };
  }

  private RewriteDataFilesProcedure(TableCatalog tableCatalog) {
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
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    Identifier tableIdent = input.ident(TABLE_PARAM);
    String strategy = input.asString(STRATEGY_PARAM, null);
    String sortOrderString = input.asString(SORT_ORDER_PARAM, null);
    Map<String, String> options = input.asStringMap(OPTIONS_PARAM, ImmutableMap.of());
    String where = input.asString(WHERE_PARAM, null);
    String partitionedByString = input.asString(PARTITIONED_BY_PARAM, null);
    Boolean createPartitionIfNotExists =
        input.asBoolean(CREATE_PARTITION_IF_NOT_EXISTS_PARAM, false);

    return modifyIcebergTable(
        tableIdent,
        table -> {
          if (partitionedByString != null || createPartitionIfNotExists) {
            Integer partitionSpecId =
                checkAndPreparePartitionSpec(
                    table, partitionedByString, createPartitionIfNotExists, options);
            options.put(OUTPUT_SPEC_ID, partitionSpecId.toString());
            // TODO - Check to see if 'refresh' needs to be called. Leaning towards no need to
            // refresh
          }

          RewriteDataFiles action = actions().rewriteDataFiles(table).options(options);

          if (strategy != null || sortOrderString != null) {
            action = checkAndApplyStrategy(action, strategy, sortOrderString, table.schema());
          }

          action = checkAndApplyFilter(action, where, tableIdent);

          RewriteDataFiles.Result result = action.execute();

          return toOutputRows(result);
        });
  }

  private Integer checkAndPreparePartitionSpec(
      Table table, String partitionBy, Boolean createIfNotExists, Map<String, String> options) {

    Preconditions.checkArgument(
        partitionBy != null && options.containsKey(OUTPUT_SPEC_ID),
        "Cannot specify a partitioning strategy and an output spec ID together. Only 1 can be used at a time");

    Preconditions.checkArgument(
        partitionBy == null && createIfNotExists,
        "Cannot set %s to true if there is no partition spec provided to %s",
        CREATE_PARTITION_IF_NOT_EXISTS_PARAM.name(),
        PARTITIONED_BY_PARAM.name());

    PartitionSpec specForRewrite =
        PartitionSpecUtil.createPartitionSpec(table.schema(), partitionBy);

    for (PartitionSpec spec : table.specs().values()) {
      if (specForRewrite.compatibleWith(spec)) {
        return spec.specId();
      }
    }

    Preconditions.checkArgument(
        !createIfNotExists,
        "Partition spec %s did not match any existing spec and %s has not been enabled",
        partitionBy,
        CREATE_PARTITION_IF_NOT_EXISTS_PARAM.name());

    table.updateSpec().addNonDefaultSpec().useSpec(specForRewrite).commit();

    for (PartitionSpec spec : table.specs().values()) {
      if (specForRewrite.compatibleWith(spec)) {
        return spec.specId();
      }
    }

    // Should never hit this, the commit to add the partition would have failed
    throw new RuntimeException("Failed to retrieve partition spec after commit");
  }

  private RewriteDataFiles checkAndApplyFilter(
      RewriteDataFiles action, String where, Identifier ident) {
    if (where != null) {
      Expression expression = filterExpression(ident, where);
      return action.filter(expression);
    }
    return action;
  }

  private RewriteDataFiles checkAndApplyStrategy(
      RewriteDataFiles action, String strategy, String sortOrderString, Schema schema) {
    List<Zorder> zOrderTerms = Lists.newArrayList();
    List<ExtendedParser.RawOrderField> sortOrderFields = Lists.newArrayList();
    if (sortOrderString != null) {
      ExtendedParser.parseSortOrder(spark(), sortOrderString)
          .forEach(
              field -> {
                if (field.term() instanceof Zorder) {
                  zOrderTerms.add((Zorder) field.term());
                } else {
                  sortOrderFields.add(field);
                }
              });

      if (!zOrderTerms.isEmpty() && !sortOrderFields.isEmpty()) {
        // TODO: we need to allow this in future when SparkAction has handling for this.
        throw new IllegalArgumentException(
            "Cannot mix identity sort columns and a Zorder sort expression: " + sortOrderString);
      }
    }

    // caller of this function ensures that between strategy and sortOrder, at least one of them is
    // not null.
    if (strategy == null || strategy.equalsIgnoreCase("sort")) {
      if (!zOrderTerms.isEmpty()) {
        String[] columnNames =
            zOrderTerms.stream()
                .flatMap(zOrder -> zOrder.refs().stream().map(NamedReference::name))
                .toArray(String[]::new);
        return action.zOrder(columnNames);
      } else if (!sortOrderFields.isEmpty()) {
        return action.sort(buildSortOrder(sortOrderFields, schema));
      } else {
        return action.sort();
      }
    }
    if (strategy.equalsIgnoreCase("binpack")) {
      RewriteDataFiles rewriteDataFiles = action.binPack();
      if (sortOrderString != null) {
        // calling below method to throw the error as user has set both binpack strategy and sort
        // order
        return rewriteDataFiles.sort(buildSortOrder(sortOrderFields, schema));
      }
      return rewriteDataFiles;
    } else {
      throw new IllegalArgumentException(
          "unsupported strategy: " + strategy + ". Only binpack or sort is supported");
    }
  }

  private SortOrder buildSortOrder(
      List<ExtendedParser.RawOrderField> rawOrderFields, Schema schema) {
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    rawOrderFields.forEach(
        rawField -> builder.sortBy(rawField.term(), rawField.direction(), rawField.nullOrder()));
    return builder.build();
  }

  private InternalRow[] toOutputRows(RewriteDataFiles.Result result) {
    int rewrittenDataFilesCount = result.rewrittenDataFilesCount();
    long rewrittenBytesCount = result.rewrittenBytesCount();
    int addedDataFilesCount = result.addedDataFilesCount();
    int failedDataFilesCount = result.failedDataFilesCount();

    InternalRow row =
        newInternalRow(
            rewrittenDataFilesCount,
            addedDataFilesCount,
            rewrittenBytesCount,
            failedDataFilesCount);
    return new InternalRow[] {row};
  }

  @Override
  public String description() {
    return "RewriteDataFilesProcedure";
  }
}
