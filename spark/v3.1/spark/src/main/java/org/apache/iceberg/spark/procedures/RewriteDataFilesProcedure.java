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

import java.util.Map;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SetWriteDistributionAndOrdering;
import org.apache.spark.sql.catalyst.plans.logical.SortOrderParserUtil;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.execution.datasources.SparkExpressionConverter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.runtime.BoxedUnit;

/**
 * A procedure that rewrites datafiles in a table.
 *
 * @see org.apache.iceberg.spark.actions.SparkActions#rewriteDataFiles(Table)
 */
class RewriteDataFilesProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("strategy", DataTypes.StringType),
        ProcedureParameter.optional("sort_order", DataTypes.StringType),
        ProcedureParameter.optional("options", STRING_MAP),
        ProcedureParameter.optional("where", DataTypes.StringType)
      };

  // counts are not nullable since the action result is never null
  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(
                "rewritten_data_files_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(
                "added_data_files_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("rewritten_bytes_count", DataTypes.LongType, false, Metadata.empty())
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
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());

    return modifyIcebergTable(
        tableIdent,
        table -> {
          RewriteDataFiles action = actions().rewriteDataFiles(table);

          String strategy = args.isNullAt(1) ? null : args.getString(1);
          String sortOrderString = args.isNullAt(2) ? null : args.getString(2);
          SortOrder sortOrder = null;
          if (sortOrderString != null) {
            sortOrder = collectSortOrders(table, sortOrderString);
          }
          if (strategy != null || sortOrder != null) {
            action = checkAndApplyStrategy(action, strategy, sortOrder);
          }

          if (!args.isNullAt(3)) {
            action = checkAndApplyOptions(args, action);
          }

          String where = args.isNullAt(4) ? null : args.getString(4);
          action = checkAndApplyFilter(action, where, table.name());

          RewriteDataFiles.Result result = action.execute();

          return toOutputRows(result);
        });
  }

  private RewriteDataFiles checkAndApplyFilter(
      RewriteDataFiles action, String where, String tableName) {
    if (where != null) {
      try {
        Expression expression =
            SparkExpressionConverter.collectResolvedSparkExpression(spark(), tableName, where);
        return action.filter(SparkExpressionConverter.convertToIcebergExpression(expression));
      } catch (AnalysisException e) {
        throw new IllegalArgumentException("Cannot parse predicates in where option: " + where, e);
      }
    }
    return action;
  }

  private RewriteDataFiles checkAndApplyOptions(InternalRow args, RewriteDataFiles action) {
    Map<String, String> options = Maps.newHashMap();
    args.getMap(3)
        .foreach(
            DataTypes.StringType,
            DataTypes.StringType,
            (k, v) -> {
              options.put(k.toString(), v.toString());
              return BoxedUnit.UNIT;
            });
    return action.options(options);
  }

  private RewriteDataFiles checkAndApplyStrategy(
      RewriteDataFiles action, String strategy, SortOrder sortOrder) {
    // caller of this function ensures that between strategy and sortOrder, at least one of them is
    // not null.
    if (strategy == null || strategy.equalsIgnoreCase("sort")) {
      return action.sort(sortOrder);
    }
    if (strategy.equalsIgnoreCase("binpack")) {
      RewriteDataFiles rewriteDataFiles = action.binPack();
      if (sortOrder != null) {
        // calling below method to throw the error as user has set both binpack strategy and sort
        // order
        return rewriteDataFiles.sort(sortOrder);
      }
      return rewriteDataFiles;
    } else {
      throw new IllegalArgumentException(
          "unsupported strategy: " + strategy + ". Only binpack,sort is supported");
    }
  }

  private SortOrder collectSortOrders(Table table, String sortOrderStr) {
    String prefix = "ALTER TABLE temp WRITE ORDERED BY ";
    try {
      // Note: Reusing the existing Iceberg sql parser to avoid implementing the custom parser for
      // sort orders.
      // To reuse the existing parser, adding a prefix of "ALTER TABLE temp WRITE ORDERED BY"
      // along with input sort order and parsing it as a plan to collect the sortOrder.
      LogicalPlan logicalPlan = spark().sessionState().sqlParser().parsePlan(prefix + sortOrderStr);
      return (new SortOrderParserUtil())
          .collectSortOrder(
              table.schema(), ((SetWriteDistributionAndOrdering) logicalPlan).sortOrder());
    } catch (AnalysisException ex) {
      throw new IllegalArgumentException("Unable to parse sortOrder: " + sortOrderStr);
    }
  }

  private InternalRow[] toOutputRows(RewriteDataFiles.Result result) {
    int rewrittenDataFilesCount = result.rewrittenDataFilesCount();
    long rewrittenBytesCount = result.rewrittenBytesCount();
    int addedDataFilesCount = result.addedDataFilesCount();
    InternalRow row =
        newInternalRow(rewrittenDataFilesCount, addedDataFilesCount, rewrittenBytesCount);
    return new InternalRow[] {row};
  }

  @Override
  public String description() {
    return "RewriteDataFilesProcedure";
  }
}
