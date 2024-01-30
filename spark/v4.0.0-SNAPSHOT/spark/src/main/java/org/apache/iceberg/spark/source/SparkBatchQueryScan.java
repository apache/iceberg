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
package org.apache.iceberg.spark.source;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkV2Filters;
import org.apache.iceberg.spark.source.broadcastvar.BroadcastHRUnboundPredicate;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.PushedBroadcastFilterData;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkBatchQueryScan extends SparkPartitioningAwareScan<PartitionScanTask>
    implements SupportsRuntimeV2Filtering {

  private static final Logger LOG = LoggerFactory.getLogger(SparkBatchQueryScan.class);

  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final String tag;
  private final List<Expression> runtimeFilterExpressions;
  private volatile boolean hasPushedBroadcastVar = false;
  private final Map<BroadcastHRUnboundPredicate<?>, Integer> broadcastVarAdded = Maps.newHashMap();
  private volatile int taskCreationVersionNum = 0;

  private volatile NamedReference[] partitionAttributes = null;

  SparkBatchQueryScan(
      SparkSession spark,
      Table table,
      Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters,
      Supplier<ScanReport> scanReportSupplier) {
    super(spark, table, scan, readConf, expectedSchema, filters, scanReportSupplier);

    this.snapshotId = readConf.snapshotId();
    this.startSnapshotId = readConf.startSnapshotId();
    this.endSnapshotId = readConf.endSnapshotId();
    this.asOfTimestamp = readConf.asOfTimestamp();
    this.tag = readConf.tag();
    this.runtimeFilterExpressions = Lists.newArrayList();
  }

  Long snapshotId() {
    return snapshotId;
  }

  @Override
  protected Class<PartitionScanTask> taskJavaClass() {
    return PartitionScanTask.class;
  }

  @Override
  public NamedReference[] filterAttributes() {
    Set<Integer> partitionFieldSourceIds = Sets.newHashSet();

    for (PartitionSpec spec : specs()) {
      for (PartitionField field : spec.fields()) {
        partitionFieldSourceIds.add(field.sourceId());
      }
    }

    Map<Integer, String> quotedNameById = SparkSchemaUtil.indexQuotedNameById(expectedSchema());

    // the optimizer will look for an equality condition with filter attributes in a join
    // as the scan has been already planned, filtering can only be done on projected attributes
    // that's why only partition source fields that are part of the read schema can be reported

    return partitionFieldSourceIds.stream()
        .filter(fieldId -> expectedSchema().findField(fieldId) != null)
        .map(fieldId -> Spark3Util.toNamedReference(quotedNameById.get(fieldId)))
        .toArray(NamedReference[]::new);
  }

  @Override
  public NamedReference[] allAttributes() {
    Map<Integer, String> quotedNameById = SparkSchemaUtil.indexQuotedNameById(expectedSchema());
    return this.expectedSchema().columns().stream()
        .map(fieldId -> Spark3Util.toNamedReference(quotedNameById.get(fieldId.fieldId())))
        .toArray(NamedReference[]::new);
  }

  @Override
  public NamedReference[] partitionAttributes() {
    if (this.partitionAttributes == null) {
      Set<Integer> partitionFieldSourceIds = Sets.newHashSet();

      for (PartitionSpec spec : table().specs().values()) {
        for (PartitionField field : spec.fields()) {
          partitionFieldSourceIds.add(field.sourceId());
        }
      }

      Map<Integer, String> quotedNameById = SparkSchemaUtil.indexQuotedNameById(expectedSchema());

      // the optimizer will look for an equality condition with filter attributes in a join
      // as the scan has been already planned, filtering can only be done on projected attributes
      // that's why only partition source fields that are part of the read schema can be reported

      this.partitionAttributes = partitionFieldSourceIds.stream()
              .filter(fieldId -> expectedSchema().findField(fieldId) != null)
              .map(fieldId -> Spark3Util.toNamedReference(quotedNameById.get(fieldId)))
              .toArray(NamedReference[]::new);
    }
    return this.partitionAttributes;
  }

  private List<Expression> collectRangeInExpressions(Expression nonpart) {
    List<Expression> rangeInExprs = new LinkedList<>();
    boolean keepGoing = true;
    Expression currentExpr = nonpart;
    while (keepGoing) {
      if (currentExpr instanceof And) {
        Expression left = ((And) currentExpr).left();
        Expression right = ((And) currentExpr).right();
        if (left instanceof And) {
          currentExpr = left;
          rangeInExprs.add(right);
        } else if (right instanceof And) {
          currentExpr = right;
          rangeInExprs.add(left);
        } else {
          rangeInExprs.add(right);
          rangeInExprs.add(left);
          keepGoing = false;
        }
      } else {
        rangeInExprs.add(currentExpr);
        keepGoing = false;
      }
    }
    return rangeInExprs;
  }

  @Override
  public void filter(Predicate[] predicates) {
    Tuple<Expression, Expression> allFilterExprs = convertRuntimeFilters(predicates);
    // this may contain partition & non partition filters
    Expression broadcastVarTypeFilters = allFilterExprs.getElement1();
    Expression nonBroadcastVarPartitionFilters = allFilterExprs.getElement2();
    // non broadcast runtime filters are surely partition based
    // but of the broadcast type some may be partition based, we need to find out
    List<Expression> netNewBroadcastVarFilters = Collections.emptyList();

    if (broadcastVarTypeFilters != null && broadcastVarTypeFilters != Expressions.alwaysTrue()) {
      List<Expression> newRangeInExprs =
          collectRangeInExpressions(broadcastVarTypeFilters).stream()
              .filter(x -> !this.broadcastVarAdded.containsKey(x))
              .collect(Collectors.toList());
      int versionNumForVariables = this.taskCreationVersionNum + 1;
      if (!newRangeInExprs.isEmpty()) {
        newRangeInExprs.forEach(
            expr ->
                this.broadcastVarAdded.put(
                    (BroadcastHRUnboundPredicate<?>) expr, versionNumForVariables));
        netNewBroadcastVarFilters = newRangeInExprs;
        this.hasPushedBroadcastVar = true;
      }
    }
    Set<String> partitionColNames =
        Arrays.stream(partitionAttributes())
            .flatMap(x -> Arrays.stream(x.fieldNames()))
            .collect(Collectors.toSet());

    Map<Boolean, List<Expression>> partitionAndNonPartitionBased =
        netNewBroadcastVarFilters.stream()
            .collect(
                Collectors.partitioningBy(
                    pred -> partitionColNames.contains(((UnboundPredicate<?>) pred).ref().name())));

    List<Expression> partitionBasedBroadcastVar =
        partitionAndNonPartitionBased.getOrDefault(Boolean.TRUE, Collections.emptyList());
    List<Expression> nonPartitionBasedBroadcastVar =
        partitionAndNonPartitionBased.getOrDefault(Boolean.FALSE, Collections.emptyList());
    List<Expression> partitionBasedBroadcastVarUsableAsDataFilter =
            getPartitionBasedBroadcastVarUsableAsDataFilters(partitionBasedBroadcastVar);
    List<Expression> totalNewDataFilters = new LinkedList<>();
    totalNewDataFilters.addAll(partitionBasedBroadcastVarUsableAsDataFilter);
    totalNewDataFilters.addAll(nonPartitionBasedBroadcastVar);
    if(!totalNewDataFilters.isEmpty()) {
      Expression netFilter = totalNewDataFilters.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
      addFilterAndRecreateScan(netFilter);
      this.resetTasks(null);
    }

    // first filter tasks on the basis of partition filters
    Expression netPartitionFilter = partitionBasedBroadcastVar.stream()
            .reduce(Expressions.alwaysTrue(), Expressions::and);
    if (netPartitionFilter != Expressions.alwaysTrue()) {
      Map<Integer, Evaluator> evaluatorsBySpecId = Maps.newHashMap();

      for (PartitionSpec spec : specs()) {
        Expression inclusiveExpr =
                Projections.inclusive(spec, caseSensitive()).project(netPartitionFilter);
        Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);
        evaluatorsBySpecId.put(spec.specId(), inclusive);
      }

      List<PartitionScanTask> filteredTasks =
              tasks().stream()
                      .filter(
                              task -> {
                                Evaluator evaluator = evaluatorsBySpecId.get(task.spec().specId());
                                return evaluator.eval(task.partition());
                              })
                      .collect(Collectors.toList());

      LOG.info(
              "{} of {} task(s) for table {} matched runtime filter {}",
              filteredTasks.size(),
              tasks().size(),
              table().name(),
              ExpressionUtil.toSanitizedString(netPartitionFilter));

      // don't invalidate tasks if the runtime filter had no effect to avoid planning splits again
      if (filteredTasks.size() < tasks().size()) {
        resetTasks(filteredTasks);
      }

      // save the evaluated filter for equals/hashCode
      runtimeFilterExpressions.add(netPartitionFilter);
    }
  }

  @Override
  public List<PushedBroadcastFilterData> getPushedBroadcastFilters() {
    return this.broadcastVarAdded.keySet().stream()
        .map(pred -> new PushedBroadcastFilterData(pred.ref().name(), pred.getBroadcastVar()))
        .collect(Collectors.toList());
  }

  @Override
  public int getPushedBroadcastFiltersCount() {
    return this.broadcastVarAdded.size();
  }

  @Override
  public Set<Long> getPushedBroadcastVarIds() {
    return this.broadcastVarAdded.keySet().stream()
        .map(x -> x.getBroadcastVar().getBroadcastVarId())
        .collect(Collectors.toSet());
  }

  @Override
  protected void incrementTaskCreationVersion() {
    ++this.taskCreationVersionNum;
  }
  /*
  private List<PartitionScanTask> filterFilesAtDataFileLevelUsingBounds(
      List<Expression> netNewBroadcastVarFilters,
      List<Expression> nonPartitionBasedBroadcastVar,
      List<PartitionScanTask> filteredFiles) {
    if (!netNewBroadcastVarFilters.isEmpty()) {
      final boolean nonPartitionBroadcastFilterExists = !nonPartitionBasedBroadcastVar.isEmpty();
      // if the above is true then we do not need to re-evaluate data filter, iff the net partition
      // filter has identity transform
      // TODO: Asif : handle the case insensitive flag
      Expression newCombinedDataFilters =
          netNewBroadcastVarFilters.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
      final InclusiveMetricsEvaluator evaluator =
          new InclusiveMetricsEvaluator(this.expectedSchema(), newCombinedDataFilters, false);
      return filteredFiles.stream()
          .filter(
              task -> {
                boolean shouldEvalFile =
                    nonPartitionBroadcastFilterExists
                        || task.spec().fields().stream()
                            .anyMatch(pf -> !pf.transform().isIdentity());
                return !shouldEvalFile || evaluator.eval(task.asFileScanTask().file());
              })
          .collect(Collectors.toList());
    }
    return filteredFiles;
  }

  private List<PartitionScanTask> filterFilesAtManifestLevelAndAddToRuntimeFilters(
      List<PartitionScanTask> filteredTasks, Expression netPartitionFilter) {
    if (netPartitionFilter != Expressions.alwaysTrue()) {
      // save the evaluated filter for equals/hashCode
      runtimeFilterExpressions.add(netPartitionFilter);

      LOG.info(
          "Trying to filter {} tasks using runtime filter {}",
          tasks().size(),
          ExpressionUtil.toSanitizedString(netPartitionFilter));

      Map<Integer, Evaluator> evaluatorsBySpecId = Maps.newHashMap();

      for (PartitionSpec spec : specs()) {
        Expression inclusiveExpr =
            Projections.inclusive(spec, caseSensitive()).project(netPartitionFilter);
        Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);
        evaluatorsBySpecId.put(spec.specId(), inclusive);
      }

      return filteredTasks.stream()
          .filter(
              task -> {
                Evaluator evaluator = evaluatorsBySpecId.get(task.spec().specId());
                return evaluator.eval(task.partition());
              })
          .collect(Collectors.toList());
    }
    return filteredTasks;
  }
  */
  // at this moment, Spark can only pass IN filters for a single attribute
  // if there are multiple filter attributes, Spark will pass two separate IN filters

  /**
   * @param filters spark filters
   * @return A Tuple whose first element if not alwaysTrue, will contain the RangeIn filter ( those
   *     which have BroadcastVar) and the second element, if not alwaysTrue, will contain the Non
   *     Broadcast Expressions. When invoked by {@link #filter(Predicate[])} Filter}, the non
   *     broadcast var ( i.e non RangeIn) expressions would always represent runtime filters ( i.e
   *     filters on partition column, and they will not be added to data filters), as they cannot
   *     provide any benefit on executors for data filtering. While the broadcast var ( RangeIn)
   *     types of expression could be on both partitioned and non partitioned columns. Non
   *     Partitioned RangeIn op will be used as data filters( on executor side), as well as file
   *     filters ( on driver side during task creations). While Partitioned RangeIn filters will be
   *     used for file filteration on driver side during task creation and would be added as data
   *     filters for executors only if there exists a non trivial transform.
   */
  private Tuple<Expression, Expression> convertRuntimeFilters(Predicate[] filters) {
    return getExpression(filters, Expressions.alwaysTrue());
  }

  private Tuple<Expression, Expression> getExpression(Predicate[] filters, Expression alWaysTrue) {
    Expression runtimeFilterExpr = alWaysTrue;
    Expression rangeInExpr = alWaysTrue;
    for (Predicate filter : filters) {
      Tuple<Boolean, Expression> exprTuple = SparkV2Filters.convert(filter, this.expectedSchema());
      Expression expr = null;
      boolean isRangeIn = false;
      if (exprTuple != null) {
        expr = exprTuple.getElement2();
        isRangeIn = exprTuple.getElement1();
      }

      if (expr != null) {
        try {
          // avoid expensive evaluation of sorted set of range in op on driver side
          if (expr.op() != Expression.Operation.RANGE_IN) {
            Binder.bind(expectedSchema().asStruct(), expr, caseSensitive());
          }
          if (isRangeIn) {
            rangeInExpr = Expressions.and(rangeInExpr, expr);
          } else {
            runtimeFilterExpr = Expressions.and(runtimeFilterExpr, expr);
          }
        } catch (ValidationException e) {
          LOG.warn("Failed to bind {} to expected schema, skipping runtime filter", expr, e);
        }
      } else {
        LOG.warn("Unsupported runtime filter {}", filter);
      }
    }
    return new Tuple<>(rangeInExpr, runtimeFilterExpr);
  }

  @Override
  public boolean hasPushedBroadCastFilter() {
    return this.hasPushedBroadcastVar;
  }

  @Override
  public Statistics estimateStatistics() {
    if (scan() == null) {
      return estimateStatistics(null);

    } else if (snapshotId != null) {
      Snapshot snapshot = table().snapshot(snapshotId);
      return estimateStatistics(snapshot);

    } else if (asOfTimestamp != null) {
      long snapshotIdAsOfTime = SnapshotUtil.snapshotIdAsOfTime(table(), asOfTimestamp);
      Snapshot snapshot = table().snapshot(snapshotIdAsOfTime);
      return estimateStatistics(snapshot);

    } else if (branch() != null) {
      Snapshot snapshot = table().snapshot(branch());
      return estimateStatistics(snapshot);

    } else if (tag != null) {
      Snapshot snapshot = table().snapshot(tag);
      return estimateStatistics(snapshot);

    } else {
      Snapshot snapshot = table().currentSnapshot();
      return estimateStatistics(snapshot);
    }
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkBatchQueryScan that = (SparkBatchQueryScan) o;
    return table().name().equals(that.table().name())
        && Objects.equals(branch(), that.branch())
        && readSchema().equals(that.readSchema()) // compare Spark schemas to ignore field ids
        && filterExpressions().toString().equals(that.filterExpressions().toString())
        && runtimeFilterExpressions.toString().equals(that.runtimeFilterExpressions.toString())
        && Objects.equals(snapshotId, that.snapshotId)
        && Objects.equals(startSnapshotId, that.startSnapshotId)
        && Objects.equals(endSnapshotId, that.endSnapshotId)
        && Objects.equals(asOfTimestamp, that.asOfTimestamp)
        && Objects.equals(tag, that.tag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(),
        branch(),
        readSchema(),
        filterExpressions().toString(),
        runtimeFilterExpressions.toString(),
        snapshotId,
        startSnapshotId,
        endSnapshotId,
        asOfTimestamp,
        tag);
  }

  @Override
  public String toString() {
    String broadcastVarMissed = getUnusedBroadcastVarString();
    return String.format(
        "IcebergScan(table=%s, branch=%s, type=%s, filters=%s, runtimeFilters=%s, "
            + "caseSensitive=%s, RangeIn UNUSED=%s)",
        table(),
        branch(),
        expectedSchema().asStruct(),
        filterExpressions(),
        runtimeFilterExpressions,
        caseSensitive(),
        broadcastVarMissed);
  }

  private String getUnusedBroadcastVarString() {
    int taskVersionNum = this.taskCreationVersionNum;
    return this.broadcastVarAdded.entrySet().stream()
        .filter(entry -> entry.getValue() > taskVersionNum)
        .map(e -> e.getKey().toString())
        .collect(Collectors.joining(","));
  }

  @Override
  public String description() {
    String broadcastVarMissed = getUnusedBroadcastVarString();
    String superStr = super.description();
    return String.format(
        "%s, [runtimeFilters=%s], caseSensitive=%s,[ Broadcast Var UNUSED =%s]",
        superStr, runtimeFilterExpressions, caseSensitive(), broadcastVarMissed);
  }

  @Override
  public boolean equalToIgnoreRuntimeFilters(org.apache.spark.sql.connector.read.Scan o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SparkBatchQueryScan that = (SparkBatchQueryScan) o;
    return table().name().equals(that.table().name())
        && readSchema().equals(that.readSchema())
        && // compare Spark schemas to ignore field ids
        checkFilterExpressionsEquality(that)
        && Objects.equals(snapshotId, that.snapshotId)
        && Objects.equals(startSnapshotId, that.startSnapshotId)
        && Objects.equals(endSnapshotId, that.endSnapshotId)
        && Objects.equals(asOfTimestamp, that.asOfTimestamp);
  }

  @Override
  public int hashCodeIgnoreRuntimeFilters() {
    return Objects.hash(
        table().name(),
        readSchema(),
        getFilterStringExcludingRangeOp(this.filterExpressions()),
        snapshotId,
        startSnapshotId,
        endSnapshotId,
        asOfTimestamp);
  }

  // getter for testing purposes.
  public int getNumFileScanTasks() {
    return tasks().size();
  }

  private boolean checkFilterExpressionsEquality(SparkBatchQueryScan that) {
    /*
    return filterExpressions().stream().map(expr ->
        ExpressionVisitors.visit(expr, RangeInExcludedStringGenerator.INSTANCE)).collect(Collectors.
        joining(",")).equals(that.filterExpressions().stream().
        map(expr -> ExpressionVisitors.visit(expr, RangeInExcludedStringGenerator.INSTANCE)).
        collect(Collectors.joining(",")));

     */
    String thisStr = getFilterStringExcludingRangeOp(this.filterExpressions());
    String thatStr = getFilterStringExcludingRangeOp(that.filterExpressions());
    return thisStr.equals(thatStr);
  }

  private static String getFilterStringExcludingRangeOp(List<Expression> filters) {
    return filters.stream()
        .filter(
            pred -> {
              if (pred.op() == Expression.Operation.RANGE_IN) {
                return false;
              } else if (pred.op() == Expression.Operation.AND) {
                And temp = (And) pred;
                return temp.left().op() != Expression.Operation.RANGE_IN
                    && temp.right().op() != Expression.Operation.RANGE_IN;
              } else {
                return true;
              }
            })
        .map(Expression::toString)
        .collect(Collectors.joining());
  }

  private static class RangeInExcludedStringGenerator
      extends ExpressionVisitors.ExpressionVisitor<String> {
    private static final RangeInExcludedStringGenerator INSTANCE =
        new RangeInExcludedStringGenerator();

    private RangeInExcludedStringGenerator() {}

    @Override
    public String alwaysTrue() {
      return "true";
    }

    @Override
    public String alwaysFalse() {
      return "false";
    }

    @Override
    public String not(String result) {
      return "NOT (" + result + ")";
    }

    @Override
    public String and(String leftResult, String rightResult) {
      if (leftResult.isEmpty()) {
        return rightResult;
      } else if (rightResult.isEmpty()) {
        return leftResult;
      } else {
        return leftResult + " AND " + rightResult;
      }
    }

    @Override
    public String or(String leftResult, String rightResult) {
      return leftResult + " OR " + rightResult;
    }

    @Override
    public <T> String predicate(BoundPredicate<T> pred) {
      throw new UnsupportedOperationException("Cannot convert bound predicates to SQL");
    }

    @Override
    public <T> String predicate(UnboundPredicate<T> pred) {
      if (pred.op().equals(Expression.Operation.RANGE_IN)) {
        return "";
      } else {
        return pred.toString();
      }
    }
  }
}
