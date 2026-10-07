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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SparkDistributedDataScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.AggregateEvaluator;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundAggregate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.index.HashTransform;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.LeafFileEntry;
import org.apache.iceberg.index.LeafFileReader;
import org.apache.iceberg.index.TrackingFileEntry;
import org.apache.iceberg.index.TrackingFileReader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.InMemoryMetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.IndexSnapshotUtil;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkAggregates;
import org.apache.iceberg.spark.SparkIndexCatalogs;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkV2Filters;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkScanBuilder
    implements ScanBuilder,
        SupportsPushDownAggregates,
        SupportsPushDownV2Filters,
        SupportsPushDownRequiredColumns,
        SupportsReportStatistics,
        SupportsPushDownLimit {

  private static final Logger LOG = LoggerFactory.getLogger(SparkScanBuilder.class);
  private static final Predicate[] NO_PREDICATES = new Predicate[0];
  private Scan localScan;

  private final SparkSession spark;
  private final Table table;
  private final CaseInsensitiveStringMap options;
  private final SparkReadConf readConf;
  private final List<String> metaColumns = Lists.newArrayList();
  private final InMemoryMetricsReporter metricsReporter;

  private Schema schema;
  private boolean caseSensitive;
  private List<Expression> filterExpressions = null;
  private Predicate[] pushedPredicates = NO_PREDICATES;
  private Integer limit = null;
  private Set<String> scalarIndexResolvedFilePaths = null;

  SparkScanBuilder(
      SparkSession spark,
      Table table,
      String branch,
      Schema schema,
      CaseInsensitiveStringMap options) {
    this.spark = spark;
    this.table = table;
    this.schema = schema;
    this.options = options;
    this.readConf = new SparkReadConf(spark, table, branch, options);
    this.caseSensitive = readConf.caseSensitive();
    this.metricsReporter = new InMemoryMetricsReporter();
  }

  SparkScanBuilder(SparkSession spark, Table table, CaseInsensitiveStringMap options) {
    this(spark, table, table.schema(), options);
  }

  SparkScanBuilder(
      SparkSession spark, Table table, String branch, CaseInsensitiveStringMap options) {
    this(spark, table, branch, SnapshotUtil.schemaFor(table, branch), options);
  }

  SparkScanBuilder(
      SparkSession spark, Table table, Schema schema, CaseInsensitiveStringMap options) {
    this(spark, table, null, schema, options);
  }

  private Expression filterExpression() {
    if (filterExpressions != null) {
      return filterExpressions.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
    }
    return Expressions.alwaysTrue();
  }

  public SparkScanBuilder caseSensitive(boolean isCaseSensitive) {
    this.caseSensitive = isCaseSensitive;
    return this;
  }

  @Override
  public Predicate[] pushPredicates(Predicate[] predicates) {
    // there are 3 kinds of filters:
    // (1) filters that can be pushed down completely and don't have to evaluated by Spark
    //     (e.g. filters that select entire partitions)
    // (2) filters that can be pushed down partially and require record-level filtering in Spark
    //     (e.g. filters that may select some but not necessarily all rows in a file)
    // (3) filters that can't be pushed down at all and have to be evaluated by Spark
    //     (e.g. unsupported filters)
    // filters (1) and (2) are used prune files during job planning in Iceberg
    // filters (2) and (3) form a set of post scan filters and must be evaluated by Spark

    List<Expression> expressions = Lists.newArrayListWithExpectedSize(predicates.length);
    List<Predicate> pushableFilters = Lists.newArrayListWithExpectedSize(predicates.length);
    List<Predicate> postScanFilters = Lists.newArrayListWithExpectedSize(predicates.length);

    for (Predicate predicate : predicates) {
      try {
        Expression expr = SparkV2Filters.convert(predicate);

        if (expr != null) {
          // try binding the expression to ensure it can be pushed down
          Binder.bind(schema.asStruct(), expr, caseSensitive);
          expressions.add(expr);
          pushableFilters.add(predicate);
        }

        if (expr == null
            || unpartitioned()
            || !ExpressionUtil.selectsPartitions(expr, table, caseSensitive)) {
          postScanFilters.add(predicate);
        } else {
          LOG.info("Evaluating completely on Iceberg side: {}", predicate);
        }

      } catch (Exception e) {
        LOG.warn("Failed to check if {} can be pushed down: {}", predicate, e.getMessage());
        postScanFilters.add(predicate);
      }
    }

    this.filterExpressions = expressions;
    this.pushedPredicates = pushableFilters.toArray(new Predicate[0]);

    tryPruneUsingScalarIndex();

    return postScanFilters.toArray(new Predicate[0]);
  }

  private static final Set<Expression.Operation> SCALAR_INDEX_PRUNABLE_OPS =
      ImmutableSet.of(
          Expression.Operation.EQ,
          Expression.Operation.IN,
          Expression.Operation.LT,
          Expression.Operation.LT_EQ,
          Expression.Operation.GT,
          Expression.Operation.GT_EQ);

  // Open Question 2 in the design doc: without a bound, a misconfigured index (e.g. a very large
  // hash.num-buckets, or a wide IN list) could resolve to more candidate leaf files than it would
  // ever be worth opening -- planning could end up slower than no index at all. Overridable per
  // table via the scalar-index.max-candidate-leaf-files property, read below.
  private static final String MAX_CANDIDATE_LEAF_FILES_PROPERTY =
      "scalar-index.max-candidate-leaf-files";
  private static final int DEFAULT_MAX_CANDIDATE_LEAF_FILES = 100;

  /**
   * If a SCALAR index exists on a column referenced by an equality, {@code IN}, or range predicate
   * in {@link #filterExpressions}, resolves the predicate(s) against the index's leaf files and
   * records the matching source file paths in {@link #scalarIndexResolvedFilePaths}, so {@link
   * #buildBatchScan} can constrain the scan to just those files via {@link
   * FileScanTaskFilteringScan}.
   *
   * <p>An equality predicate (HASH or IDENTITY transform) resolves to a single transform value. An
   * {@code IN} predicate resolves to one transform value per literal (HASH or IDENTITY), queried as
   * separate points rather than a combined range -- HASH in particular can scatter an IN-list's
   * values across unrelated, non-contiguous buckets, so there is no single [min, max] that would be
   * both correct and useful. A range predicate ({@code <}, {@code <=}, {@code >}, {@code >=} --
   * including a {@code BETWEEN}, which Spark decomposes into two range predicates on the same
   * column) only makes sense against an IDENTITY-transform index: HASH scatters values across
   * buckets, so a contiguous range on the original column does not map to a contiguous range of
   * transform values the way it does for IDENTITY, where the transform value is the key value
   * itself.
   *
   * <p>Restricting the scan to files that could satisfy one AND'd predicate (or set of predicates
   * on the same column) is always sound: any row satisfying the full pushed-down conjunction must
   * also satisfy it, so it must live in one of these files. The predicate itself is still pushed
   * down and applied as a residual regardless, so a wrong or stale resolution here can only miss an
   * optimization, never produce a wrong result -- except for the zero-match case (key confirmed
   * absent from a fully fresh index, with no uncovered files), which is deliberately NOT pruned to
   * zero files here: that would be a correctness-sensitive optimization (a bug would silently
   * return wrong empty results, not just miss a speedup), left as a documented follow-up rather
   * than attempted in this pass.
   *
   * <p>Staleness is handled via the covered/uncovered-files model from Huaxin Gao's Primary Key
   * Index for Apache Iceberg proposal (Section 8, "Staleness Semantics"): files that existed at the
   * index's own snapshot ("covered") can be pruned using the index as usual; files added to the
   * table since ("uncovered") are never known to the index and are always included in the resolved
   * set, unconditionally. This lets a stale index still help, rather than falling back to no
   * pruning at all on any snapshot mismatch. See {@link #uncoveredFilePathsSince}.
   *
   * <p>Only the first column whose predicate(s) resolve against an existing index is used;
   * combining resolutions from multiple SCALAR indexes on an AND'd query would need set
   * intersection across indexes, which is not yet attempted.
   *
   * <p>Any failure -- no index registered, an unsupported predicate shape, a non-append snapshot
   * (e.g. compaction) between the index's snapshot and the current one, an I/O error reading the
   * tracking or leaf file -- falls back silently to normal planning, matching the design proposal's
   * rule that the index must never be required for correctness.
   */
  private void tryPruneUsingScalarIndex() {
    if (filterExpressions == null || filterExpressions.isEmpty()) {
      return;
    }

    Map<String, List<UnboundPredicate<?>>> candidatesByColumn = Maps.newLinkedHashMap();
    for (Expression expr : filterExpressions) {
      if (!(expr instanceof UnboundPredicate) || !SCALAR_INDEX_PRUNABLE_OPS.contains(expr.op())) {
        continue;
      }

      UnboundPredicate<?> predicate = (UnboundPredicate<?>) expr;
      if (!(predicate.term() instanceof NamedReference)) {
        continue;
      }

      String columnName = ((NamedReference<?>) predicate.term()).name();
      candidatesByColumn.computeIfAbsent(columnName, c -> Lists.newArrayList()).add(predicate);
    }

    if (candidatesByColumn.isEmpty()) {
      return;
    }

    // Derived the same way BuildScalarIndexProcedure derives it (TableIdentifier.parse of the
    // core Table's own name), not from the Spark catalog Identifier -- the two must produce an
    // identical TableIdentifier or listIndexes below would silently see a disjoint set.
    org.apache.iceberg.catalog.TableIdentifier tableIdentifier =
        org.apache.iceberg.catalog.TableIdentifier.parse(table.name());
    List<IndexMetadata> availableIndexes;
    try {
      availableIndexes = SparkIndexCatalogs.get().catalogFor(table).listIndexes(tableIdentifier);
    } catch (Exception e) {
      LOG.warn(
          "Failed to list SCALAR indexes for table {}, falling back to normal planning: {}",
          tableIdentifier,
          e.getMessage());
      return;
    }

    if (availableIndexes.isEmpty()) {
      return;
    }

    for (Map.Entry<String, List<UnboundPredicate<?>>> entry : candidatesByColumn.entrySet()) {
      if (tryPruneUsingScalarIndex(availableIndexes, entry.getKey(), entry.getValue())) {
        return;
      }
    }
  }

  /**
   * Attempts to resolve {@code predicates} (all on {@code columnName}, each {@code EQ}, {@code IN},
   * or a range comparison) against a SCALAR index covering that column, discovered from {@code
   * availableIndexes} (as returned by {@link IndexCatalog#listIndexes}) by matching the column's
   * field ID against {@link IndexMetadata#keyColumnIds()} -- not by any naming convention. Returns
   * {@code true} if it resolved and set {@link #scalarIndexResolvedFilePaths}, {@code false} to let
   * {@link #tryPruneUsingScalarIndex()} try the next column's predicates instead.
   */
  private boolean tryPruneUsingScalarIndex(
      List<IndexMetadata> availableIndexes,
      String columnName,
      List<UnboundPredicate<?>> predicates) {
    Types.NestedField keyField = schema.findField(columnName);
    if (keyField == null) {
      return false;
    }

    IndexMetadata metadata =
        availableIndexes.stream()
            .filter(m -> "SCALAR".equals(m.type()))
            .filter(m -> m.keyColumnIds().contains(keyField.fieldId()))
            .findFirst()
            .orElse(null);
    if (metadata == null || table.currentSnapshot() == null) {
      return false;
    }

    try {
      IndexSnapshot indexSnapshot = metadata.currentSnapshot();
      if (indexSnapshot == null) {
        return false;
      }

      // Read the tracking file once and reuse the entries for both the type-promotion check and
      // candidate collection, rather than re-reading (and re-decoding) it per use and per range.
      List<TrackingFileEntry> trackedLeafFiles =
          TrackingFileReader.readAll(table.io().newInputFile(indexSnapshot.trackingFile()));

      if (keyTypePromoted(trackedLeafFiles, keyField, columnName)) {
        return false;
      }

      long currentTableSnapshotId = table.currentSnapshot().snapshotId();
      Set<String> uncoveredFilePaths = ImmutableSet.of();
      if (indexSnapshot.sourceTableSnapshotId() != currentTableSnapshotId) {
        // Stale relative to the current table snapshot -- rather than fall back entirely,
        // find the files added since the index's snapshot (uncovered) so covered files can
        // still be pruned via the index. Throws if the index's snapshot isn't a clean append
        // ancestor of the current one (e.g. a compaction ran in between); the outer catch
        // below falls back to no pruning at all in that case, same as before this change.
        uncoveredFilePaths =
            uncoveredFilePathsSince(indexSnapshot.sourceTableSnapshotId(), currentTableSnapshotId);
      }

      ResolvedPredicate resolved = resolvePredicates(metadata, columnName, predicates);
      if (resolved == null) {
        return false;
      }

      return resolveAndSetFilePaths(
          trackedLeafFiles, resolved, keyField, columnName, uncoveredFilePaths);
    } catch (Exception e) {
      LOG.warn(
          "Failed to use SCALAR index on column {}, falling back to normal planning: {}",
          columnName,
          e.getMessage());
      return false;
    }
  }

  /**
   * Collects the candidate leaf files for {@code resolved}, enforces the planning-cost bound, reads
   * the matching entries, and sets {@link #scalarIndexResolvedFilePaths} to the covered match files
   * plus any uncovered files. Returns {@code true} if pruning was applied, {@code false} to fall
   * back to normal planning.
   */
  private boolean resolveAndSetFilePaths(
      List<TrackingFileEntry> trackedLeafFiles,
      ResolvedPredicate resolved,
      Types.NestedField keyField,
      String columnName,
      Set<String> uncoveredFilePaths) {
    // Dedupe by location: an IN predicate's separate target ranges can resolve to the same leaf
    // file (e.g. two IN values landing in the same HASH bucket), and reading it twice would just
    // waste work, not affect correctness.
    List<TrackingFileEntry> candidateLeafFiles =
        collectCandidateLeafFiles(trackedLeafFiles, resolved.targetRanges);

    int maxCandidateLeafFiles =
        PropertyUtil.propertyAsInt(
            table.properties(),
            MAX_CANDIDATE_LEAF_FILES_PROPERTY,
            DEFAULT_MAX_CANDIDATE_LEAF_FILES);
    if (candidateLeafFiles.size() > maxCandidateLeafFiles) {
      // Resolved to more candidate leaf files than it's worth opening -- bail out to normal
      // planning rather than let a misconfigured index make planning slower than no index at all.
      // The original predicate is still applied downstream regardless, so this is purely a missed
      // optimization, not a correctness concern.
      LOG.info(
          "SCALAR index on {} resolved to {} candidate leaf file(s), exceeding the planning-cost"
              + " bound of {} ({}) -- falling back to normal planning",
          columnName,
          candidateLeafFiles.size(),
          maxCandidateLeafFiles,
          MAX_CANDIDATE_LEAF_FILES_PROPERTY);
      return false;
    }

    List<LeafFileEntry> matches = Lists.newArrayList();
    for (TrackingFileEntry leaf : candidateLeafFiles) {
      matches.addAll(
          LeafFileReader.readMatching(
              table.io().newInputFile(leaf.location()), keyField, resolved.leafFilter));
    }

    if (matches.isEmpty() && uncoveredFilePaths.isEmpty()) {
      // 0 matches (key not present in covered files) and no uncovered files either -- fall back to
      // normal planning rather than prune to zero; the original predicate itself still gets applied
      // downstream and yields no rows.
      return false;
    }

    // Uncovered files are unconditionally included regardless of what the index says -- they're not
    // covered by it, so they must always be scanned.
    Set<String> resolvedPaths = Sets.newHashSet(uncoveredFilePaths);
    matches.forEach(m -> resolvedPaths.add(m.filePath()));
    this.scalarIndexResolvedFilePaths = resolvedPaths;
    LOG.info(
        "SCALAR index on {} resolved {} to {} file(s) ({} covered match(es), {} uncovered"
            + " file(s)): {}",
        columnName,
        resolved.literalValueForLog,
        resolvedPaths.size(),
        matches.size(),
        uncoveredFilePaths.size(),
        resolvedPaths);
    return true;
  }

  private boolean keyTypePromoted(
      List<TrackingFileEntry> trackedLeafFiles, Types.NestedField keyField, String columnName) {
    // If the key column was promoted (e.g. int -> long) since the index was built, a HASH
    // transform value computed from the current type no longer matches the buckets stored in the
    // leaf files. On its own that would just miss matches, but combined with a stale index
    // (uncovered files present) it could prune away covered rows that do match -- a wrong (empty)
    // result. Detect it by comparing the type physically stored in a leaf file against the current
    // key type; the predicate itself is still applied downstream, so results stay correct.
    if (trackedLeafFiles.isEmpty()) {
      return false;
    }

    Type storedKeyType =
        LeafFileReader.storedKeyType(
            table.io().newInputFile(trackedLeafFiles.get(0).location()), keyField);
    if (keyField.type().equals(storedKeyType)) {
      return false;
    }

    LOG.warn(
        "SCALAR index on {} was built for key type {} but the current schema has {} (the column "
            + "was changed since the index was built) -- falling back to normal planning; rebuild "
            + "the index to re-enable pruning",
        columnName,
        storedKeyType,
        keyField.type());
    return true;
  }

  private ResolvedPredicate resolvePredicates(
      IndexMetadata metadata, String columnName, List<UnboundPredicate<?>> predicates) {
    Optional<UnboundPredicate<?>> eqPredicate =
        predicates.stream().filter(p -> p.op() == Expression.Operation.EQ).findFirst();
    if (eqPredicate.isPresent()) {
      return resolveEqPredicate(metadata, columnName, eqPredicate.get());
    }

    Optional<UnboundPredicate<?>> inPredicate =
        predicates.stream().filter(p -> p.op() == Expression.Operation.IN).findFirst();
    if (inPredicate.isPresent()) {
      return resolveInPredicate(metadata, columnName, inPredicate.get());
    }

    return resolveRangePredicates(metadata, columnName, predicates);
  }

  private ResolvedPredicate resolveEqPredicate(
      IndexMetadata metadata, String columnName, UnboundPredicate<?> eqPredicate) {
    Object literalValue = eqPredicate.literal().value();
    long targetTransformValue = transformValue(metadata, literalValue);
    return new ResolvedPredicate(
        ImmutableList.of(new TransformValueRange(targetTransformValue, targetTransformValue)),
        Expressions.equal(columnName, literalValue),
        literalValue);
  }

  private ResolvedPredicate resolveInPredicate(
      IndexMetadata metadata, String columnName, UnboundPredicate<?> inPredicate) {
    List<Object> literalValues = Lists.newArrayList();
    for (Literal<?> literal : inPredicate.literals()) {
      literalValues.add(literal.value());
    }
    if (literalValues.isEmpty()) {
      return null;
    }

    List<TransformValueRange> ranges = Lists.newArrayListWithExpectedSize(literalValues.size());
    for (Object literalValue : literalValues) {
      long transformed = transformValue(metadata, literalValue);
      ranges.add(new TransformValueRange(transformed, transformed));
    }
    return new ResolvedPredicate(ranges, Expressions.in(columnName, literalValues), literalValues);
  }

  private ResolvedPredicate resolveRangePredicates(
      IndexMetadata metadata, String columnName, List<UnboundPredicate<?>> predicates) {
    // No equality or IN predicate in this group -- only IDENTITY preserves enough order for a
    // range comparison to map to a contiguous transform-value range; HASH scatters values across
    // buckets, so a range on the original column tells us nothing about which buckets to look in.
    if (!"IDENTITY".equals(metadata.transformFunction())) {
      return null;
    }

    long lowerBound = Long.MIN_VALUE;
    long upperBound = Long.MAX_VALUE;
    Expression combinedFilter = null;
    for (UnboundPredicate<?> predicate : predicates) {
      long value = ((Number) predicate.literal().value()).longValue();
      switch (predicate.op()) {
        case GT:
        case GT_EQ:
          // Deliberately loose (uses value, not value + 1, for GT): the coarse tracking-file range
          // only needs to be a superset of the true match set -- LeafFileReader re-applies the
          // exact original predicate below, so this can only cost scanning one extra boundary leaf
          // file, never under-prune a real match away.
          lowerBound = Math.max(lowerBound, value);
          break;
        case LT:
        case LT_EQ:
          upperBound = Math.min(upperBound, value);
          break;
        default:
          // EQ/IN are handled above; SCALAR_INDEX_PRUNABLE_OPS admits nothing else here.
          break;
      }
      combinedFilter =
          combinedFilter == null ? predicate : Expressions.and(combinedFilter, predicate);
    }

    if (combinedFilter == null || lowerBound > upperBound) {
      return null;
    }

    return new ResolvedPredicate(
        ImmutableList.of(new TransformValueRange(lowerBound, upperBound)),
        combinedFilter,
        "[" + lowerBound + ", " + upperBound + "]");
  }

  /**
   * The target transform-value ranges, the leaf-file filter, and a log label, resolved from a group
   * of predicates on one indexed column.
   */
  private static final class ResolvedPredicate {
    private final List<TransformValueRange> targetRanges;
    private final Expression leafFilter;
    private final Object literalValueForLog;

    ResolvedPredicate(
        List<TransformValueRange> targetRanges, Expression leafFilter, Object literalValueForLog) {
      this.targetRanges = targetRanges;
      this.leafFilter = leafFilter;
      this.literalValueForLog = literalValueForLog;
    }
  }

  /**
   * One [min, max] transform-value sub-range to query the tracking file for.
   *
   * <p>Package-private (not private), along with {@link #collectCandidateLeafFiles}, so both can be
   * tested directly against a real tracking file without needing a full Spark session -- see
   * TestSparkScanBuilderCandidateLeafFiles in this package.
   */
  static final class TransformValueRange {
    private final long min;
    private final long max;

    TransformValueRange(long min, long max) {
      this.min = min;
      this.max = max;
    }

    long min() {
      return min;
    }

    long max() {
      return max;
    }
  }

  /**
   * Collects the tracking-file entries whose transform-value range overlaps any of {@code
   * targetRanges}, deduped by location. An {@code IN} predicate's separate target ranges can
   * resolve to the same leaf file (e.g. two IN values landing in the same HASH bucket); reading it
   * twice would just waste work, not affect correctness, but is worth avoiding. Reads the tracking
   * file once; prefer {@link #collectCandidateLeafFiles(List, List)} when the entries are already
   * in hand.
   */
  static List<TrackingFileEntry> collectCandidateLeafFiles(
      FileIO io, String trackingFileLocation, List<TransformValueRange> targetRanges) {
    return collectCandidateLeafFiles(
        TrackingFileReader.readAll(io.newInputFile(trackingFileLocation)), targetRanges);
  }

  /** Filters already-read tracking entries by {@code targetRanges}, deduped by location. */
  static List<TrackingFileEntry> collectCandidateLeafFiles(
      List<TrackingFileEntry> trackedLeafFiles, List<TransformValueRange> targetRanges) {
    Map<String, TrackingFileEntry> byLocation = Maps.newLinkedHashMap();
    for (TransformValueRange range : targetRanges) {
      for (TrackingFileEntry entry : trackedLeafFiles) {
        // Overlap: entry range [lower, upper] overlaps [min, max] iff lower <= max AND upper >=
        // min.
        if (entry.transformValueLowerBound() <= range.max()
            && entry.transformValueUpperBound() >= range.min()) {
          byLocation.putIfAbsent(entry.location(), entry);
        }
      }
    }
    return Lists.newArrayList(byLocation.values());
  }

  private static long transformValue(IndexMetadata metadata, Object literalValue) {
    if ("HASH".equals(metadata.transformFunction())) {
      int numBuckets =
          Integer.parseInt(metadata.properties().getOrDefault("hash.num-buckets", "256"));
      return new HashTransform(numBuckets).apply(literalValue);
    }
    return ((Number) literalValue).longValue();
  }

  /**
   * Data file paths added to {@link #table} strictly after {@code sourceSnapshotId} up to and
   * including {@code currentSnapshotId} -- the "uncovered" files in the covered/uncovered-files
   * staleness model (see {@link #tryPruneUsingScalarIndex}). Delegates to {@link
   * IndexSnapshotUtil#addedFilePathsSince}, shared with the write side ({@code
   * BuildScalarIndexProcedure}'s incremental build path) so a fix to this correctness-sensitive
   * logic can't diverge between the two.
   */
  private Set<String> uncoveredFilePathsSince(long sourceSnapshotId, long currentSnapshotId) {
    return IndexSnapshotUtil.addedFilePathsSince(table, sourceSnapshotId, currentSnapshotId);
  }

  private boolean unpartitioned() {
    return table.specs().values().stream().noneMatch(PartitionSpec::isPartitioned);
  }

  @Override
  public Predicate[] pushedPredicates() {
    return pushedPredicates;
  }

  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    if (!canPushDownAggregation(aggregation)) {
      return false;
    }

    AggregateEvaluator aggregateEvaluator;
    List<BoundAggregate<?, ?>> expressions =
        Lists.newArrayListWithExpectedSize(aggregation.aggregateExpressions().length);

    for (AggregateFunc aggregateFunc : aggregation.aggregateExpressions()) {
      try {
        Expression expr = SparkAggregates.convert(aggregateFunc);
        if (expr != null) {
          Expression bound = Binder.bind(schema.asStruct(), expr, caseSensitive);
          expressions.add((BoundAggregate<?, ?>) bound);
        } else {
          LOG.info(
              "Skipping aggregate pushdown: AggregateFunc {} can't be converted to iceberg expression",
              aggregateFunc);
          return false;
        }
      } catch (IllegalArgumentException e) {
        LOG.info("Skipping aggregate pushdown: Bind failed for AggregateFunc {}", aggregateFunc, e);
        return false;
      }
    }

    aggregateEvaluator = AggregateEvaluator.create(expressions);

    if (!metricsModeSupportsAggregatePushDown(aggregateEvaluator.aggregates())) {
      return false;
    }

    org.apache.iceberg.Scan scan =
        buildIcebergBatchScan(true /* include Column Stats */, schemaWithMetadataColumns());

    try (CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles()) {
      for (FileScanTask task : fileScanTasks) {
        if (!task.deletes().isEmpty()) {
          LOG.info("Skipping aggregate pushdown: detected row level deletes");
          return false;
        }

        aggregateEvaluator.update(task.file());
      }
    } catch (IOException e) {
      LOG.info("Skipping aggregate pushdown: ", e);
      return false;
    }

    if (!aggregateEvaluator.allAggregatorsValid()) {
      return false;
    }

    StructType pushedAggregateSchema =
        SparkSchemaUtil.convert(new Schema(aggregateEvaluator.resultType().fields()));
    InternalRow[] pushedAggregateRows = new InternalRow[1];
    StructLike structLike = aggregateEvaluator.result();
    pushedAggregateRows[0] =
        new StructInternalRow(aggregateEvaluator.resultType()).setStruct(structLike);
    localScan =
        new SparkLocalScan(table, pushedAggregateSchema, pushedAggregateRows, filterExpressions);

    return true;
  }

  private boolean canPushDownAggregation(Aggregation aggregation) {
    if (!(table instanceof BaseTable)) {
      return false;
    }

    if (!readConf.aggregatePushDownEnabled()) {
      return false;
    }

    // If group by expression is the same as the partition, the statistics information can still
    // be used to calculate min/max/count, will enable aggregate push down in next phase.
    // TODO: enable aggregate push down for partition col group by expression
    if (aggregation.groupByExpressions().length > 0) {
      LOG.info("Skipping aggregate pushdown: group by aggregation push down is not supported");
      return false;
    }

    return true;
  }

  private boolean metricsModeSupportsAggregatePushDown(List<BoundAggregate<?, ?>> aggregates) {
    MetricsConfig config = MetricsConfig.forTable(table);
    for (BoundAggregate aggregate : aggregates) {
      String colName = aggregate.columnName();
      if (!colName.equals("*")) {
        MetricsModes.MetricsMode mode = config.columnMode(colName);
        if (mode instanceof MetricsModes.None) {
          LOG.info("Skipping aggregate pushdown: No metrics for column {}", colName);
          return false;
        } else if (mode instanceof MetricsModes.Counts) {
          if (aggregate.op() == Expression.Operation.MAX
              || aggregate.op() == Expression.Operation.MIN) {
            LOG.info(
                "Skipping aggregate pushdown: Cannot produce min or max from count for column {}",
                colName);
            return false;
          }
        } else if (aggregate.type().typeId() == Type.TypeID.STRING
            || aggregate.type().typeId() == Type.TypeID.BINARY) {
          // lower_bounds and upper_bounds may have been truncated before, so disable push down
          // regardless of the current mode
          if (aggregate.op() == Expression.Operation.MAX
              || aggregate.op() == Expression.Operation.MIN) {
            LOG.info(
                "Skipping aggregate pushdown: Cannot produce min or max from truncated values for column {}",
                colName);
            return false;
          }
        }
      }
    }

    return true;
  }

  @Override
  public void pruneColumns(StructType requestedSchema) {
    StructType requestedProjection =
        new StructType(
            Stream.of(requestedSchema.fields())
                .filter(field -> MetadataColumns.nonMetadataColumn(field.name()))
                .toArray(StructField[]::new));

    // the projection should include all columns that will be returned, including those only used in
    // filters
    this.schema =
        SparkSchemaUtil.prune(schema, requestedProjection, filterExpression(), caseSensitive);

    Stream.of(requestedSchema.fields())
        .map(StructField::name)
        .filter(MetadataColumns::isMetadataColumn)
        .distinct()
        .forEach(metaColumns::add);
  }

  private Schema schemaWithMetadataColumns() {
    // metadata columns
    List<Types.NestedField> metadataFields =
        metaColumns.stream()
            .distinct()
            .map(name -> MetadataColumns.metadataColumn(table, name))
            .collect(Collectors.toList());
    Schema metadataSchema = calculateMetadataSchema(metadataFields);

    // schema or rows returned by readers
    return TypeUtil.join(schema, metadataSchema);
  }

  private Schema calculateMetadataSchema(List<Types.NestedField> metaColumnFields) {
    Optional<Types.NestedField> partitionField =
        metaColumnFields.stream()
            .filter(f -> MetadataColumns.PARTITION_COLUMN_ID == f.fieldId())
            .findFirst();

    // only calculate potential column id collision if partition metadata column was requested
    if (!partitionField.isPresent()) {
      return new Schema(metaColumnFields);
    }

    Set<Integer> idsToReassign =
        TypeUtil.indexById(partitionField.get().type().asStructType()).keySet();

    // Calculate used ids by union metadata columns with all base table schemas
    Set<Integer> currentlyUsedIds =
        metaColumnFields.stream().map(Types.NestedField::fieldId).collect(Collectors.toSet());
    Set<Integer> allUsedIds =
        table.schemas().values().stream()
            .map(currSchema -> TypeUtil.indexById(currSchema.asStruct()).keySet())
            .reduce(currentlyUsedIds, Sets::union);

    // Reassign selected ids to deduplicate with used ids.
    AtomicInteger nextId = new AtomicInteger();
    return new Schema(
        metaColumnFields,
        ImmutableSet.of(),
        oldId -> {
          if (!idsToReassign.contains(oldId)) {
            return oldId;
          }
          int candidate = nextId.incrementAndGet();
          while (allUsedIds.contains(candidate)) {
            candidate = nextId.incrementAndGet();
          }
          return candidate;
        });
  }

  @Override
  public Scan build() {
    if (localScan != null) {
      return localScan;
    } else {
      return buildBatchScan();
    }
  }

  private Scan buildBatchScan() {
    Schema expectedSchema = schemaWithMetadataColumns();
    return new SparkBatchQueryScan(
        spark,
        table,
        buildIcebergBatchScan(false /* not include Column Stats */, expectedSchema),
        readConf,
        expectedSchema,
        filterExpressions,
        metricsReporter::scanReport);
  }

  private org.apache.iceberg.Scan buildIcebergBatchScan(boolean withStats, Schema expectedSchema) {
    Long snapshotId = readConf.snapshotId();
    Long asOfTimestamp = readConf.asOfTimestamp();
    String branch = readConf.branch();
    String tag = readConf.tag();

    Preconditions.checkArgument(
        snapshotId == null || asOfTimestamp == null,
        "Cannot set both %s and %s to select which table snapshot to scan",
        SparkReadOptions.SNAPSHOT_ID,
        SparkReadOptions.AS_OF_TIMESTAMP);

    Long startSnapshotId = readConf.startSnapshotId();
    Long endSnapshotId = readConf.endSnapshotId();

    if (snapshotId != null || asOfTimestamp != null) {
      Preconditions.checkArgument(
          startSnapshotId == null && endSnapshotId == null,
          "Cannot set %s and %s for incremental scans when either %s or %s is set",
          SparkReadOptions.START_SNAPSHOT_ID,
          SparkReadOptions.END_SNAPSHOT_ID,
          SparkReadOptions.SNAPSHOT_ID,
          SparkReadOptions.AS_OF_TIMESTAMP);
    }

    Preconditions.checkArgument(
        startSnapshotId != null || endSnapshotId == null,
        "Cannot set only %s for incremental scans. Please, set %s too.",
        SparkReadOptions.END_SNAPSHOT_ID,
        SparkReadOptions.START_SNAPSHOT_ID);

    Long startTimestamp = readConf.startTimestamp();
    Long endTimestamp = readConf.endTimestamp();
    Preconditions.checkArgument(
        startTimestamp == null && endTimestamp == null,
        "Cannot set %s or %s for incremental scans and batch scan. They are only valid for "
            + "changelog scans.",
        SparkReadOptions.START_TIMESTAMP,
        SparkReadOptions.END_TIMESTAMP);

    if (startSnapshotId != null) {
      return buildIncrementalAppendScan(startSnapshotId, endSnapshotId, withStats, expectedSchema);
    } else {
      return buildBatchScan(snapshotId, asOfTimestamp, branch, tag, withStats, expectedSchema);
    }
  }

  private org.apache.iceberg.Scan buildBatchScan(
      Long snapshotId,
      Long asOfTimestamp,
      String branch,
      String tag,
      boolean withStats,
      Schema expectedSchema) {
    BatchScan scan =
        newBatchScan()
            .caseSensitive(caseSensitive)
            .filter(filterExpression())
            .project(expectedSchema)
            .metricsReporter(metricsReporter);

    if (withStats) {
      scan = scan.includeColumnStats();
    }

    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }

    if (asOfTimestamp != null) {
      scan = scan.asOfTime(asOfTimestamp);
    }

    if (branch != null) {
      scan = scan.useRef(branch);
    }

    if (tag != null) {
      scan = scan.useRef(tag);
    }

    BatchScan configured = configureSplitPlanning(scan);
    if (scalarIndexResolvedFilePaths != null) {
      // Scoped to the plain SELECT batch-scan path only -- incremental-append, changelog,
      // merge-on-read, and copy-on-write scans have different correctness considerations (e.g.
      // row-level operations may need to see files beyond ones matching a single equality
      // predicate) and are intentionally left untouched by this optimization.
      return new FileScanTaskFilteringScan(configured, scalarIndexResolvedFilePaths);
    }
    return configured;
  }

  private org.apache.iceberg.Scan buildIncrementalAppendScan(
      long startSnapshotId, Long endSnapshotId, boolean withStats, Schema expectedSchema) {
    IncrementalAppendScan scan =
        table
            .newIncrementalAppendScan()
            .fromSnapshotExclusive(startSnapshotId)
            .caseSensitive(caseSensitive)
            .filter(filterExpression())
            .project(expectedSchema)
            .metricsReporter(metricsReporter);

    if (withStats) {
      scan = scan.includeColumnStats();
    }

    if (endSnapshotId != null) {
      scan = scan.toSnapshot(endSnapshotId);
    }

    return configureSplitPlanning(scan);
  }

  @SuppressWarnings("CyclomaticComplexity")
  public Scan buildChangelogScan() {
    Preconditions.checkArgument(
        readConf.snapshotId() == null
            && readConf.asOfTimestamp() == null
            && readConf.branch() == null
            && readConf.tag() == null,
        "Cannot set neither %s, %s, %s and %s for changelogs",
        SparkReadOptions.SNAPSHOT_ID,
        SparkReadOptions.AS_OF_TIMESTAMP,
        SparkReadOptions.BRANCH,
        SparkReadOptions.TAG);

    Long startSnapshotId = readConf.startSnapshotId();
    Long endSnapshotId = readConf.endSnapshotId();
    Long startTimestamp = readConf.startTimestamp();
    Long endTimestamp = readConf.endTimestamp();

    Preconditions.checkArgument(
        !(startSnapshotId != null && startTimestamp != null),
        "Cannot set both %s and %s for changelogs",
        SparkReadOptions.START_SNAPSHOT_ID,
        SparkReadOptions.START_TIMESTAMP);

    Preconditions.checkArgument(
        !(endSnapshotId != null && endTimestamp != null),
        "Cannot set both %s and %s for changelogs",
        SparkReadOptions.END_SNAPSHOT_ID,
        SparkReadOptions.END_TIMESTAMP);

    if (startTimestamp != null && endTimestamp != null) {
      Preconditions.checkArgument(
          startTimestamp < endTimestamp,
          "Cannot set %s to be greater than %s for changelogs",
          SparkReadOptions.START_TIMESTAMP,
          SparkReadOptions.END_TIMESTAMP);
    }

    boolean emptyScan = false;
    if (startTimestamp != null) {
      if (table.currentSnapshot() == null
          || startTimestamp > table.currentSnapshot().timestampMillis()) {
        emptyScan = true;
      }
      startSnapshotId = getStartSnapshotId(startTimestamp);
    }

    if (endTimestamp != null) {
      endSnapshotId = getEndSnapshotId(endTimestamp);
      if ((startSnapshotId == null && endSnapshotId == null)
          || (startSnapshotId != null && startSnapshotId.equals(endSnapshotId))) {
        emptyScan = true;
      }
    }

    Schema expectedSchema = schemaWithMetadataColumns();

    IncrementalChangelogScan scan =
        table
            .newIncrementalChangelogScan()
            .caseSensitive(caseSensitive)
            .filter(filterExpression())
            .project(expectedSchema)
            .metricsReporter(metricsReporter);

    if (startSnapshotId != null) {
      scan = scan.fromSnapshotExclusive(startSnapshotId);
    }

    if (endSnapshotId != null) {
      scan = scan.toSnapshot(endSnapshotId);
    }

    scan = configureSplitPlanning(scan);

    return new SparkChangelogScan(
        spark, table, scan, readConf, expectedSchema, filterExpressions, emptyScan);
  }

  private Long getStartSnapshotId(Long startTimestamp) {
    Snapshot oldestSnapshotAfter = SnapshotUtil.oldestAncestorAfter(table, startTimestamp);

    if (oldestSnapshotAfter == null) {
      return null;
    } else if (oldestSnapshotAfter.timestampMillis() == startTimestamp) {
      return oldestSnapshotAfter.snapshotId();
    } else {
      return oldestSnapshotAfter.parentId();
    }
  }

  private Long getEndSnapshotId(Long endTimestamp) {
    Long endSnapshotId = null;
    for (Snapshot snapshot : SnapshotUtil.currentAncestors(table)) {
      if (snapshot.timestampMillis() <= endTimestamp) {
        endSnapshotId = snapshot.snapshotId();
        break;
      }
    }
    return endSnapshotId;
  }

  public Scan buildMergeOnReadScan() {
    Preconditions.checkArgument(
        readConf.snapshotId() == null && readConf.asOfTimestamp() == null && readConf.tag() == null,
        "Cannot set time travel options %s, %s, %s for row-level command scans",
        SparkReadOptions.SNAPSHOT_ID,
        SparkReadOptions.AS_OF_TIMESTAMP,
        SparkReadOptions.TAG);

    Preconditions.checkArgument(
        readConf.startSnapshotId() == null && readConf.endSnapshotId() == null,
        "Cannot set incremental scan options %s and %s for row-level command scans",
        SparkReadOptions.START_SNAPSHOT_ID,
        SparkReadOptions.END_SNAPSHOT_ID);

    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, readConf.branch());

    if (snapshot == null) {
      return new SparkBatchQueryScan(
          spark,
          table,
          null,
          readConf,
          schemaWithMetadataColumns(),
          filterExpressions,
          metricsReporter::scanReport);
    }

    // remember the current snapshot ID for commit validation
    long snapshotId = snapshot.snapshotId();

    CaseInsensitiveStringMap adjustedOptions =
        Spark3Util.setOption(SparkReadOptions.SNAPSHOT_ID, Long.toString(snapshotId), options);
    SparkReadConf adjustedReadConf =
        new SparkReadConf(spark, table, readConf.branch(), adjustedOptions);

    Schema expectedSchema = schemaWithMetadataColumns();

    BatchScan scan =
        newBatchScan()
            .useSnapshot(snapshotId)
            .caseSensitive(caseSensitive)
            .filter(filterExpression())
            .project(expectedSchema)
            .metricsReporter(metricsReporter);

    scan = configureSplitPlanning(scan);

    return new SparkBatchQueryScan(
        spark,
        table,
        scan,
        adjustedReadConf,
        expectedSchema,
        filterExpressions,
        metricsReporter::scanReport);
  }

  public Scan buildCopyOnWriteScan() {
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, readConf.branch());

    if (snapshot == null) {
      return new SparkCopyOnWriteScan(
          spark,
          table,
          readConf,
          schemaWithMetadataColumns(),
          filterExpressions,
          metricsReporter::scanReport);
    }

    Schema expectedSchema = schemaWithMetadataColumns();

    BatchScan scan =
        table
            .newBatchScan()
            .useSnapshot(snapshot.snapshotId())
            .ignoreResiduals()
            .caseSensitive(caseSensitive)
            .filter(filterExpression())
            .project(expectedSchema)
            .metricsReporter(metricsReporter);

    scan = configureSplitPlanning(scan);

    return new SparkCopyOnWriteScan(
        spark,
        table,
        scan,
        snapshot,
        readConf,
        expectedSchema,
        filterExpressions,
        metricsReporter::scanReport);
  }

  private <T extends org.apache.iceberg.Scan<T, ?, ?>> T configureSplitPlanning(T scan) {
    T configuredScan = scan;

    Long splitSize = readConf.splitSizeOption();
    if (splitSize != null) {
      configuredScan = configuredScan.option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize));
    }

    Integer splitLookback = readConf.splitLookbackOption();
    if (splitLookback != null) {
      configuredScan =
          configuredScan.option(TableProperties.SPLIT_LOOKBACK, String.valueOf(splitLookback));
    }

    Long splitOpenFileCost = readConf.splitOpenFileCostOption();
    if (splitOpenFileCost != null) {
      configuredScan =
          configuredScan.option(
              TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(splitOpenFileCost));
    }

    if (null != limit) {
      configuredScan = configuredScan.minRowsRequested(limit.longValue());
    }

    return configuredScan;
  }

  @Override
  public Statistics estimateStatistics() {
    return ((SupportsReportStatistics) build()).estimateStatistics();
  }

  @Override
  public StructType readSchema() {
    return build().readSchema();
  }

  private BatchScan newBatchScan() {
    if (readConf.distributedPlanningEnabled()) {
      return new SparkDistributedDataScan(spark, table, readConf);
    } else {
      return table.newBatchScan();
    }
  }

  @Override
  public boolean pushLimit(int pushedLimit) {
    this.limit = pushedLimit;
    return true;
  }
}
