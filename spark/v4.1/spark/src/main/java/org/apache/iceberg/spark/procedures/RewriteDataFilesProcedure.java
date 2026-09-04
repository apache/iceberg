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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderStatsHandler;
import org.apache.iceberg.SortOrderStatsHandler.PartitionOverlapStats;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Hilbert;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Zorder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.ExtendedParser;
import org.apache.iceberg.spark.actions.RewriteDataFilesSparkAction;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.catalyst.InternalRow;
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
 * A procedure that rewrites datafiles in a table.
 *
 * @see org.apache.iceberg.spark.actions.SparkActions#rewriteDataFiles(Table)
 */
class RewriteDataFilesProcedure extends BaseProcedure {

  static final String NAME = "rewrite_data_files";

  /**
   * When set to {@code true} on the sort strategy, reports per-partition sort-key overlap depth
   * (and, if {@code min-overlap-depth} is also set, the files a rewrite would select on the overlap
   * axis) without rewriting anything. Only data file metadata is read and no snapshot is committed.
   */
  static final String REPORT_ONLY = "report-only";

  // must match SparkShufflingDataRewritePlanner.MIN_OVERLAP_DEPTH, which is not visible here
  private static final String MIN_OVERLAP_DEPTH = "min-overlap-depth";

  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter STRATEGY_PARAM =
      optionalInParameter("strategy", DataTypes.StringType);
  private static final ProcedureParameter SORT_ORDER_PARAM =
      optionalInParameter("sort_order", DataTypes.StringType);
  private static final ProcedureParameter OPTIONS_PARAM =
      optionalInParameter("options", STRING_MAP);
  private static final ProcedureParameter WHERE_PARAM =
      optionalInParameter("where", DataTypes.StringType);
  private static final ProcedureParameter BRANCH_PARAM =
      optionalInParameter("branch", DataTypes.StringType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        TABLE_PARAM, STRATEGY_PARAM, SORT_ORDER_PARAM, OPTIONS_PARAM, WHERE_PARAM, BRANCH_PARAM
      };

  // counts are not nullable since the action result is never null; the overlap columns are
  // populated only in report-only mode, which returns one row per partition instead of the
  // single summary row (the schema itself never changes with the mode)
  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(
                "rewritten_data_files_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(
                "added_data_files_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("rewritten_bytes_count", DataTypes.LongType, false, Metadata.empty()),
            new StructField(
                "failed_data_files_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(
                "removed_delete_files_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("partition", DataTypes.StringType, true, Metadata.empty()),
            new StructField("max_overlap_depth", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("avg_overlap_depth", DataTypes.DoubleType, true, Metadata.empty()),
            new StructField("candidate_file_count", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("candidate_bytes", DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                "missing_bounds_file_count", DataTypes.IntegerType, true, Metadata.empty())
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
    String strategy = input.asString(STRATEGY_PARAM, null);
    String sortOrderString = input.asString(SORT_ORDER_PARAM, null);
    Map<String, String> options = input.asStringMap(OPTIONS_PARAM, ImmutableMap.of());
    String where = input.asString(WHERE_PARAM, null);
    // Determine target branch: explicit parameter > table branch > main branch
    String branchParam = input.asString(BRANCH_PARAM, null);
    if (branchParam == null) {
      branchParam = loadSparkTable(tableIdent).branch();
      if (branchParam == null) {
        branchParam = SnapshotRef.MAIN_BRANCH;
      }
    }
    String branch = branchParam;

    if (reportOnly(options)) {
      return reportOverlap(tableIdent, strategy, sortOrderString, where, options);
    }

    return modifyIcebergTable(
        tableIdent,
        table -> {
          RewriteDataFilesSparkAction action =
              actions()
                  .rewriteDataFiles(table)
                  .options(withoutReportOnly(options))
                  .toBranch(branch);

          if (strategy != null || sortOrderString != null) {
            action = checkAndApplyStrategy(action, strategy, sortOrderString, table.schema());
          }

          action = checkAndApplyFilter(action, where, tableIdent);

          RewriteDataFiles.Result result = action.execute();

          return asScanIterator(OUTPUT_TYPE, toOutputRows(result));
        });
  }

  private RewriteDataFilesSparkAction checkAndApplyFilter(
      RewriteDataFilesSparkAction action, String where, Identifier ident) {
    if (where != null) {
      Expression expression = filterExpression(ident, where);
      return action.filter(expression);
    }
    return action;
  }

  private RewriteDataFilesSparkAction checkAndApplyStrategy(
      RewriteDataFilesSparkAction action, String strategy, String sortOrderString, Schema schema) {
    List<Zorder> zOrderTerms = Lists.newArrayList();
    List<Hilbert> hilbertTerms = Lists.newArrayList();
    List<ExtendedParser.RawOrderField> sortOrderFields = Lists.newArrayList();
    if (sortOrderString != null) {
      parseSortOrder(sortOrderString, zOrderTerms, hilbertTerms, sortOrderFields);
    }

    // caller of this function ensures that between strategy and sortOrder, at least one of them is
    // not null.
    if (strategy == null || strategy.equalsIgnoreCase("sort")) {
      return applySortStrategy(action, zOrderTerms, hilbertTerms, sortOrderFields, schema);
    }
    if (strategy.equalsIgnoreCase("binpack")) {
      RewriteDataFilesSparkAction binPackAction = action.binPack();
      if (sortOrderString != null) {
        // calling below method to throw the error as user has set both binpack strategy and sort
        // order
        return binPackAction.sort(buildSortOrder(sortOrderFields, schema));
      }
      return binPackAction;
    } else {
      throw new IllegalArgumentException(
          "unsupported strategy: " + strategy + ". Only binpack or sort is supported");
    }
  }

  private void parseSortOrder(
      String sortOrderString,
      List<Zorder> zOrderTerms,
      List<Hilbert> hilbertTerms,
      List<ExtendedParser.RawOrderField> sortOrderFields) {
    ExtendedParser.parseSortOrder(spark(), sortOrderString)
        .forEach(
            field -> {
              if (field.term() instanceof Zorder) {
                zOrderTerms.add((Zorder) field.term());
              } else if (field.term() instanceof Hilbert) {
                hilbertTerms.add((Hilbert) field.term());
              } else {
                sortOrderFields.add(field);
              }
            });

    if (!zOrderTerms.isEmpty() && !hilbertTerms.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot mix Zorder and Hilbert sort expressions: " + sortOrderString);
    }

    if ((!zOrderTerms.isEmpty() || !hilbertTerms.isEmpty()) && !sortOrderFields.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot mix identity sort columns and a Zorder or Hilbert sort expression: "
              + sortOrderString);
    }
  }

  private RewriteDataFilesSparkAction applySortStrategy(
      RewriteDataFilesSparkAction action,
      List<Zorder> zOrderTerms,
      List<Hilbert> hilbertTerms,
      List<ExtendedParser.RawOrderField> sortOrderFields,
      Schema schema) {
    if (!zOrderTerms.isEmpty()) {
      String[] columnNames =
          zOrderTerms.stream()
              .flatMap(zOrder -> zOrder.refs().stream().map(NamedReference::name))
              .toArray(String[]::new);
      return action.zOrder(columnNames);
    } else if (!hilbertTerms.isEmpty()) {
      String[] columnNames =
          hilbertTerms.stream()
              .flatMap(hilbert -> hilbert.refs().stream().map(NamedReference::name))
              .toArray(String[]::new);
      return action.hilbert(columnNames);
    } else if (!sortOrderFields.isEmpty()) {
      return action.sort(buildSortOrder(sortOrderFields, schema));
    } else {
      return action.sort();
    }
  }

  private SortOrder buildSortOrder(
      List<ExtendedParser.RawOrderField> rawOrderFields, Schema schema) {
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    rawOrderFields.forEach(
        rawField -> builder.sortBy(rawField.term(), rawField.direction(), rawField.nullOrder()));
    return builder.build();
  }

  /**
   * Builds an output row from values keyed by column name, so each call site names what it sets and
   * stays correct if columns are added or reordered. An unknown name fails immediately via {@link
   * StructType#fieldIndex(String)}; columns not set are null.
   */
  private InternalRow outputRow(Map<String, Object> valuesByColumn) {
    Object[] values = new Object[OUTPUT_TYPE.size()];
    for (Map.Entry<String, Object> entry : valuesByColumn.entrySet()) {
      values[OUTPUT_TYPE.fieldIndex(entry.getKey())] = entry.getValue();
    }

    return newInternalRow(values);
  }

  private boolean reportOnly(Map<String, String> options) {
    String value = options.get(REPORT_ONLY);
    if (value == null) {
      return false;
    }

    // strict parse so a typo cannot silently fall through to an actual rewrite
    if (value.equalsIgnoreCase("true")) {
      return true;
    } else if (value.equalsIgnoreCase("false")) {
      return false;
    } else {
      throw new IllegalArgumentException(
          String.format("'%s' is set to %s but must be true or false", REPORT_ONLY, value));
    }
  }

  private Map<String, String> withoutReportOnly(Map<String, String> options) {
    if (!options.containsKey(REPORT_ONLY)) {
      return options;
    }

    Map<String, String> filtered = Maps.newHashMap(options);
    filtered.remove(REPORT_ONLY);
    return ImmutableMap.copyOf(filtered);
  }

  private Iterator<Scan> reportOverlap(
      Identifier tableIdent,
      String strategy,
      String sortOrderString,
      String where,
      Map<String, String> options) {
    if (strategy != null && !strategy.equalsIgnoreCase("sort")) {
      throw new IllegalArgumentException(
          String.format("'%s' requires the sort strategy, got: %s", REPORT_ONLY, strategy));
    }

    if (sortOrderString != null) {
      // overlap is measured on the table sort order; see the option docs
      throw new IllegalArgumentException(
          String.format(
              "'%s' reports on the table sort order and cannot be used with sort_order: %s",
              REPORT_ONLY, sortOrderString));
    }

    if (where != null) {
      throw new IllegalArgumentException(
          String.format("'%s' cannot be used with a where filter", REPORT_ONLY));
    }

    Integer minOverlapDepth = PropertyUtil.propertyAsNullableInt(options, MIN_OVERLAP_DEPTH);

    return withIcebergTable(
        tableIdent,
        table -> {
          List<PartitionOverlapStats> stats =
              SortOrderStatsHandler.computeStats(table, null, minOverlapDepth);
          InternalRow[] rows = new InternalRow[stats.size()];
          for (int i = 0; i < stats.size(); i++) {
            PartitionOverlapStats stat = stats.get(i);
            PartitionSpec spec = table.specs().get(stat.specId());
            String partitionPath =
                spec.isPartitioned() ? spec.partitionToPath(stat.partition()) : null;
            Map<String, Object> values = Maps.newHashMap();
            values.put("rewritten_data_files_count", 0);
            values.put("added_data_files_count", 0);
            values.put("rewritten_bytes_count", 0L);
            values.put("failed_data_files_count", 0);
            values.put("removed_delete_files_count", 0);
            values.put(
                "partition", partitionPath == null ? null : UTF8String.fromString(partitionPath));
            values.put("max_overlap_depth", stat.maxOverlapDepth());
            values.put("avg_overlap_depth", stat.avgOverlapDepth());
            values.put("candidate_file_count", stat.candidateFileCount());
            values.put("candidate_bytes", stat.candidateBytes());
            values.put("missing_bounds_file_count", stat.filesMissingBounds());
            rows[i] = outputRow(values);
          }

          return asScanIterator(OUTPUT_TYPE, rows);
        });
  }

  private InternalRow[] toOutputRows(RewriteDataFiles.Result result) {
    int rewrittenDataFilesCount = result.rewrittenDataFilesCount();
    long rewrittenBytesCount = result.rewrittenBytesCount();
    int addedDataFilesCount = result.addedDataFilesCount();
    int failedDataFilesCount = result.failedDataFilesCount();
    int removedDeleteFilesCount = result.removedDeleteFilesCount();

    Map<String, Object> values = Maps.newHashMap();
    values.put("rewritten_data_files_count", rewrittenDataFilesCount);
    values.put("added_data_files_count", addedDataFilesCount);
    values.put("rewritten_bytes_count", rewrittenBytesCount);
    values.put("failed_data_files_count", failedDataFilesCount);
    values.put("removed_delete_files_count", removedDeleteFilesCount);
    InternalRow row = outputRow(values);
    return new InternalRow[] {row};
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "RewriteDataFilesProcedure";
  }
}
