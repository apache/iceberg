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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Zorder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.ExtendedParser;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
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

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        TABLE_PARAM, STRATEGY_PARAM, SORT_ORDER_PARAM, OPTIONS_PARAM, WHERE_PARAM
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

    return modifyIcebergTable(
        tableIdent,
        table -> {
          RewriteDataFiles action = actions().rewriteDataFiles(table).options(options);

          if (strategy != null || sortOrderString != null) {
            action = checkAndApplyStrategy(action, strategy, sortOrderString, table.schema());
          }

          action = checkAndApplyBranch(table, tableIdent, action);
          action = checkAndApplyFilter(action, where, tableIdent);

          RewriteDataFiles.Result result = action.execute();

          return toOutputRows(result);
        });
  }

  private RewriteDataFiles checkAndApplyFilter(
      RewriteDataFiles action, String where, Identifier ident) {
    if (where != null) {
      Expression expression = filterExpression(ident, where);
      return action.filter(expression);
    }
    return action;
  }
  //
  //  private RewriteDataFiles checkAndApplyBranch(
  //      Table table, Identifier ident, RewriteDataFiles action) {
  //    String branchIdent = Spark3Util.extractBranch(ident);
  //    if (branchIdent != null) {
  //      return action.targetBranch(branchIdent);
  //    }
  //    SparkWriteConf writeConf = new SparkWriteConf(spark(), table, Maps.newHashMap());
  //    String targetBranch = writeConf.branch();
  //    if (targetBranch != null) {
  //      return action.targetBranch(targetBranch);
  //    } else {
  //      return action;
  //    }
  //  }

  private RewriteDataFiles checkAndApplyBranch(
      Table table, Identifier ident, RewriteDataFiles action) {
    String branchIdent = Spark3Util.extractBranch(ident);
    SparkWriteConf writeConf = new SparkWriteConf(spark(), table, branchIdent, Maps.newHashMap());
    String targetBranch = writeConf.branch();
    if (targetBranch != null) {
      action.targetBranch(targetBranch);
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
