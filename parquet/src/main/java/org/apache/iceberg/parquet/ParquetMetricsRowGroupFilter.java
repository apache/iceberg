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
package org.apache.iceberg.parquet;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Bound;
import org.apache.iceberg.expressions.BoundExtract;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetMetricsRowGroupFilter {
  public static final Logger LOG = LoggerFactory.getLogger(ParquetMetricsRowGroupFilter.class);

  private static final int IN_PREDICATE_LIMIT = 200;
  private final Schema schema;
  private final Expression expr;

  /** Is special handling of variants needed? */
  private final boolean hasVariantPredicates;

  /**
   * variantColumnNames is file-schema-scoped (same for all row groups in a file), so cache it. *
   */
  private MessageType cachedVariantFileSchema = null;

  /** Map of schema ID to variant column names. */
  private Map<Integer, String> variantColumnNames = null;

  /** Set of variant top-level column names, derived from variantColumnNames values. */
  private Set<String> variantTopLevelNames = null;

  /**
   * For testing, especially while there are requirements for predecessor patches to be applied.
   *
   * <p>This permits assertions to be made that variant predicate pushdown reached this far and
   * processed shredded columns.
   */
  private static final AtomicLong VARIANT_PREDICATES_SHREDDED_METRICS_EVALUATED = new AtomicLong();

  /** Counter for row groups proven skippable by a shredded variant predicate. */
  private static final AtomicLong VARIANT_PREDICATES_SHREDDED_SKIPPED = new AtomicLong();

  public ParquetMetricsRowGroupFilter(Schema schema, Expression unbound) {
    this(schema, unbound, true);
  }

  public ParquetMetricsRowGroupFilter(Schema schema, Expression unbound, boolean caseSensitive) {
    this.schema = schema;
    StructType struct = schema.asStruct();
    this.expr = Binder.bind(struct, Expressions.rewriteNot(unbound), caseSensitive);
    this.hasVariantPredicates = hasVariantPredicates(expr);
  }

  /**
   * Test whether the row group may contain records that match the expression.
   *
   * @param fileSchema schema for the Parquet file
   * @param rowGroup metadata for a row group
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  @SuppressWarnings("ReferenceEquality")
  public boolean shouldRead(MessageType fileSchema, BlockMetaData rowGroup) {
    // identity check: same object means same file schema, no need to recompute variant column names
    // REVIST: would the cached schema ever change within a file?
    if (hasVariantPredicates && fileSchema != cachedVariantFileSchema) {
      cachedVariantFileSchema = fileSchema;
      variantColumnNames = buildVariantColumnNames(fileSchema);
      variantTopLevelNames = ImmutableSet.copyOf(variantColumnNames.values());
    }
    return new MetricsEvalVisitor().eval(fileSchema, rowGroup);
  }

  private static final boolean ROWS_MIGHT_MATCH = true;
  private static final boolean ROWS_CANNOT_MATCH = false;

  private record VariantColumnInfo(PrimitiveType type, int id, ColumnChunkMetaData chunkMetaData) {}

  /** Evaluate metrics, including variants. */
  private class MetricsEvalVisitor extends BoundExpressionVisitor<Boolean> {
    private Map<Integer, Statistics<?>> stats = null;
    private Map<Integer, Long> valueCounts = null;
    private Map<Integer, Function<Object, Object>> conversions = null;

    /**
     * ID-less columns collected during the main column scan for lazy variantInfoByColumnPath build.
     */
    private List<VariantColumnInfo> shreddedVariantColumns = null;

    /**
     * Built lazily on the first compareVariant() call; null means not yet built. TODO: should
     * construction be synchronized?
     */
    private Map<ColumnPath, VariantColumnInfo> variantInfoByColumnPath = null;

    private boolean eval(MessageType fileSchema, BlockMetaData rowGroup) {
      if (rowGroup.getRowCount() <= 0) {
        return ROWS_CANNOT_MATCH;
      }

      this.stats = Maps.newHashMap();
      this.valueCounts = Maps.newHashMap();
      this.conversions = Maps.newHashMap();
      this.shreddedVariantColumns = hasVariantPredicates ? Lists.newArrayList() : null;
      this.variantInfoByColumnPath = null;
      for (ColumnChunkMetaData col : rowGroup.getColumns()) {
        PrimitiveType colType = fileSchema.getType(col.getPath().toArray()).asPrimitiveType();
        if (colType.getId() != null) {
          int id = colType.getId().intValue();
          Type icebergType = schema.findType(id);
          stats.put(id, col.getStatistics());
          valueCounts.put(id, col.getValueCount());
          conversions.put(id, ParquetConversions.converterFromParquet(colType, icebergType));
        } else if (shreddedVariantColumns != null) {
          // Shredded variant typed_value columns have no Iceberg field ID, just parquet ones.
          // Pre-filter to only those under known variant top-level column names; buildVariantInfo()
          // then just copies the list into a map on the first compareVariant() call.
          ColumnPath columnPath = col.getPath();
          String[] pathParts = columnPath.toArray();
          if (pathParts.length > 0 && variantTopLevelNames.contains(pathParts[0])) {
            shreddedVariantColumns.add(new VariantColumnInfo(colType, -1, col));
          }
        }
      }
      return ExpressionVisitors.visitEvaluator(expr, this);
    }

    /** Build variantInfoByColumnPath lazily on the first compareVariant() call. */
    private void buildVariantInfo() {
      variantInfoByColumnPath = Maps.newHashMap();
      for (VariantColumnInfo colInfo : shreddedVariantColumns) {
        variantInfoByColumnPath.put(colInfo.chunkMetaData().getPath(), colInfo);
      }
    }

    @Override
    public Boolean alwaysTrue() {
      return ROWS_MIGHT_MATCH; // all rows match
    }

    @Override
    public Boolean alwaysFalse() {
      return ROWS_CANNOT_MATCH; // all rows fail
    }

    @Override
    public Boolean not(Boolean result) {
      return !result;
    }

    @Override
    public Boolean and(Boolean leftResult, Boolean rightResult) {
      return leftResult && rightResult;
    }

    @Override
    public Boolean or(Boolean leftResult, Boolean rightResult) {
      return leftResult || rightResult;
    }

    @Override
    public <T> Boolean isNull(BoundReference<T> ref) {
      // no need to check whether the field is required because binding evaluates that case
      // if the column has no null values, the expression cannot match
      int id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_MIGHT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && !colStats.isEmpty() && colStats.getNumNulls() == 0) {
        // there are stats and no values are null => all values are non-null
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notNull(BoundReference<T> ref) {
      // no need to check whether the field is required because binding evaluates that case
      // if the column has no non-null values, the expression cannot match
      int id = ref.fieldId();

      // When filtering nested types or variant types, notNull() is an implicit filter passed
      // even though complex filters aren't pushed down in Parquet. Leave these type filters
      // to be evaluated post scan.
      Type type = schema.findType(id);
      if (type instanceof Type.NestedType || type.isVariantType()) {
        return ROWS_MIGHT_MATCH;
      }

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && valueCount - colStats.getNumNulls() == 0) {
        // (num nulls == value count) => all values are null => no non-null values
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean isNaN(BoundReference<T> ref) {
      int id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && valueCount - colStats.getNumNulls() == 0) {
        // (num nulls == value count) => all values are null => no nan values
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notNaN(BoundReference<T> ref) {
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (allNulls(colStats, valueCount)) {
          return ROWS_CANNOT_MATCH;
        }

        if (minMaxUndefined(colStats)) {
          return ROWS_MIGHT_MATCH;
        }

        T lower = min(colStats, id);
        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp >= 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (allNulls(colStats, valueCount)) {
          return ROWS_CANNOT_MATCH;
        }

        if (minMaxUndefined(colStats)) {
          return ROWS_MIGHT_MATCH;
        }

        T lower = min(colStats, id);
        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp > 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (allNulls(colStats, valueCount)) {
          return ROWS_CANNOT_MATCH;
        }

        if (minMaxUndefined(colStats)) {
          return ROWS_MIGHT_MATCH;
        }

        T upper = max(colStats, id);
        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp <= 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (allNulls(colStats, valueCount)) {
          return ROWS_CANNOT_MATCH;
        }

        if (minMaxUndefined(colStats)) {
          return ROWS_MIGHT_MATCH;
        }

        T upper = max(colStats, id);
        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp < 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      // Leave all nested column type and variant type filters to be
      // evaluated post scan.
      Type type = schema.findType(id);
      if (type instanceof Type.NestedType || type.isVariantType()) {
        return ROWS_MIGHT_MATCH;
      }

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (allNulls(colStats, valueCount)) {
          return ROWS_CANNOT_MATCH;
        }

        if (minMaxUndefined(colStats)) {
          return ROWS_MIGHT_MATCH;
        }

        T lower = min(colStats, id);
        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp > 0) {
          return ROWS_CANNOT_MATCH;
        }

        T upper = max(colStats, id);
        cmp = lit.comparator().compare(upper, lit.value());
        if (cmp < 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
      // because the bounds are not necessarily a min or max value, this cannot be answered using
      // them. notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
      int id = ref.fieldId();

      // Leave all nested column type and variant type filters to be
      // evaluated post scan.
      Type type = schema.findType(id);
      if (type instanceof Type.NestedType || type.isVariantType()) {
        return ROWS_MIGHT_MATCH;
      }

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (allNulls(colStats, valueCount)) {
          return ROWS_CANNOT_MATCH;
        }

        if (minMaxUndefined(colStats)) {
          return ROWS_MIGHT_MATCH;
        }

        Collection<T> literals = literalSet;

        if (literals.size() > IN_PREDICATE_LIMIT) {
          // skip evaluating the predicate if the number of values is too big
          return ROWS_MIGHT_MATCH;
        }

        T lower = min(colStats, id);
        literals =
            literals.stream()
                .filter(v -> ref.comparator().compare(lower, v) <= 0)
                .collect(Collectors.toList());
        if (literals.isEmpty()) { // if all values are less than lower bound, rows cannot match.
          return ROWS_CANNOT_MATCH;
        }

        T upper = max(colStats, id);
        literals =
            literals.stream()
                .filter(v -> ref.comparator().compare(upper, v) >= 0)
                .collect(Collectors.toList());
        if (literals
            .isEmpty()) { // if all remaining values are greater than upper bound, rows cannot
          // match.
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
      // because the bounds are not necessarily a min or max value, this cannot be answered using
      // them. notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in col.
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      @SuppressWarnings("unchecked")
      Statistics<Binary> colStats = (Statistics<Binary>) stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (allNulls(colStats, valueCount)) {
          return ROWS_CANNOT_MATCH;
        }

        if (minMaxUndefined(colStats)) {
          return ROWS_MIGHT_MATCH;
        }

        ByteBuffer prefixAsBytes = lit.toByteBuffer();

        Comparator<ByteBuffer> comparator = Comparators.unsignedBytes();

        Binary lower = colStats.genericGetMin();
        // truncate lower bound so that its length in bytes is not greater than the length of prefix
        int lowerLength = Math.min(prefixAsBytes.remaining(), lower.length());
        int lowerCmp =
            comparator.compare(
                BinaryUtil.truncateBinary(lower.toByteBuffer(), lowerLength), prefixAsBytes);
        if (lowerCmp > 0) {
          return ROWS_CANNOT_MATCH;
        }

        Binary upper = colStats.genericGetMax();
        // truncate upper bound so that its length in bytes is not greater than the length of prefix
        int upperLength = Math.min(prefixAsBytes.remaining(), upper.length());
        int upperCmp =
            comparator.compare(
                BinaryUtil.truncateBinary(upper.toByteBuffer(), upperLength), prefixAsBytes);
        if (upperCmp < 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notStartsWith(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();
      Long valueCount = valueCounts.get(id);

      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_MIGHT_MATCH;
      }

      @SuppressWarnings("unchecked")
      Statistics<Binary> colStats = (Statistics<Binary>) stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (mayContainNull(colStats)) {
          return ROWS_MIGHT_MATCH;
        }

        if (minMaxUndefined(colStats)) {
          return ROWS_MIGHT_MATCH;
        }

        Binary lower = colStats.genericGetMin();
        Binary upper = colStats.genericGetMax();

        // notStartsWith will match unless all values must start with the prefix. this happens when
        // the lower and upper
        // bounds both start with the prefix.
        if (lower != null && upper != null) {
          ByteBuffer prefix = lit.toByteBuffer();
          Comparator<ByteBuffer> comparator = Comparators.unsignedBytes();

          // if lower is shorter than the prefix, it can't start with the prefix
          if (lower.length() < prefix.remaining()) {
            return ROWS_MIGHT_MATCH;
          }

          // truncate lower bound to the prefix and check for equality
          int cmp =
              comparator.compare(
                  BinaryUtil.truncateBinary(lower.toByteBuffer(), prefix.remaining()), prefix);
          if (cmp == 0) {
            // the lower bound starts with the prefix; check the upper bound
            // if upper is shorter than the prefix, it can't start with the prefix
            if (upper.length() < prefix.remaining()) {
              return ROWS_MIGHT_MATCH;
            }

            // truncate upper bound so that its length in bytes is not greater than the length of
            // prefix
            cmp =
                comparator.compare(
                    BinaryUtil.truncateBinary(upper.toByteBuffer(), prefix.remaining()), prefix);
            if (cmp == 0) {
              // both bounds match the prefix, so all rows must match the prefix and none do not
              // match
              return ROWS_CANNOT_MATCH;
            }
          }
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @SuppressWarnings("unchecked")
    private <T> T min(Statistics<?> statistics, int id) {
      return (T) conversions.get(id).apply(statistics.genericGetMin());
    }

    @SuppressWarnings("unchecked")
    private <T> T max(Statistics<?> statistics, int id) {
      return (T) conversions.get(id).apply(statistics.genericGetMax());
    }

    @Override
    public <T> Boolean predicate(BoundPredicate<T> pred) {
      if (pred.term() instanceof BoundExtract<T> term) {
        // it's a variant predicate: process accordingly.
        return recordOutcome(compareVariant(pred, term));
      } else {
        return super.predicate(pred);
      }
    }

    /**
     * Compare a variant with the predicate. For floats and doubles, expects the parquet writer to
     * have normalized -0.0 to +0.0.
     *
     * @param pred predicate
     * @param extract extracted variant reference
     * @param <T> type of predicate
     * @return true if the file rows should be read (i.e. false iff we are confident they can be
     *     skipped)
     */
    private <T> boolean compareVariant(BoundPredicate<T> pred, BoundExtract<T> extract) {
      if (variantInfoByColumnPath == null) {
        buildVariantInfo();
      }
      int fieldId = extract.ref().fieldId();
      LOG.debug("comparing variant {}", extract);
      String colName = variantColumnNames.get(fieldId);
      if (colName == null) {
        // not in the variant columns
        return ROWS_MIGHT_MATCH;
      }
      // Build the column path of which a shredded field would have
      ColumnPath columnPath = ParquetVariantUtil.shreddedColumnPath(colName, extract.path());
      final VariantColumnInfo columnInfo = variantInfoByColumnPath.get(columnPath);
      if (columnInfo == null) {
        // the column isn't shredded in this file, so no statistics are available.
        return ROWS_MIGHT_MATCH;
      }
      // increment shredded metrics counter.
      VARIANT_PREDICATES_SHREDDED_METRICS_EVALUATED.incrementAndGet();

      // now do the evaluation.
      LOG.debug("Evaluating column {} with info {}", columnPath, columnInfo);
      PrimitiveType parquetType = columnInfo.type();
      final ColumnChunkMetaData col = columnInfo.chunkMetaData;
      Statistics<?> colStats = col.getStatistics();
      long valueCount = col.getValueCount();
      if (parquetType == null || colStats == null) {
        // no type info or column stats
        return ROWS_MIGHT_MATCH;
      }

      // everything here onwards expects colStats != null
      if (pred.isUnaryPredicate()) {
        return evalUnaryPredicate(pred, colStats, valueCount);
      }
      final Boolean outcome = evalShreddedColumnStats(colStats, valueCount);
      if (outcome != null) {
        return outcome;
      }
      if (pred.isSetPredicate()) {
        // set check
        return evalMembershipPredicateOnShreddedVariant(pred, extract, parquetType, colStats);
      }
      if (!pred.isLiteralPredicate()) {
        return ROWS_MIGHT_MATCH;
      }
      // get this far: it's a shredded variant with column statistics
      return evalBinaryPredicateOnShreddedVariant(pred, extract, parquetType, colStats);
    }

    /**
     * Increment the skipped counter on {@code ROWS_CANNOT_MATCH} and return the input unchanged.
     */
    private boolean recordOutcome(boolean shouldRead) {
      if (!shouldRead) {
        VARIANT_PREDICATES_SHREDDED_SKIPPED.incrementAndGet();
      }
      return shouldRead;
    }

    /**
     * Evaluate the statistics, return an Boolean value if there was enough information to make a
     * decision.
     *
     * <p>This is a bit contrived, but it keeps the complexity of {@code compareVariant()} below the
     * checkstyle limit.
     *
     * @param colStats column statistics
     * @param valueCount number of values.
     * @return an boolean which is null if no conclusion is reached.
     */
    private Boolean evalShreddedColumnStats(Statistics<?> colStats, long valueCount) {
      if (colStats.isEmpty()) {
        return ROWS_MIGHT_MATCH;
      }
      if (allNulls(colStats, valueCount)) {
        // there's no non-null columns, therefore all comparators will be false
        return ROWS_CANNOT_MATCH;
      }
      if (minMaxUndefined(colStats)) {
        // min or max undefined, so ranged comparisons not possible
        return ROWS_MIGHT_MATCH;
      }
      return null;
    }

    /**
     * Evaluate a predicate against two shredded values: should the rowgroup be read?
     *
     * @param pred predicate
     * @param extract bounded extrat
     * @param parquetType the parquet type of the column
     * @param colStats column statistics.
     * @param <T> type of the arguments
     * @return true if the rowgroup should be read.
     */
    @SuppressWarnings("unchecked")
    private <T> boolean evalBinaryPredicateOnShreddedVariant(
        BoundPredicate<T> pred,
        BoundExtract<T> extract,
        PrimitiveType parquetType,
        Statistics<?> colStats) {
      // it's a binary predicate with valid results from comparisons with
      // the stats. So get their min and max, compare them with the literal value.
      Literal<T> lit = pred.asLiteralPredicate().literal();
      // get the type converter for the evaluation
      Function<Object, Object> converter =
          ParquetConversions.converterFromParquet(parquetType, extract.type());
      T min = (T) converter.apply(colStats.genericGetMin());
      T max = (T) converter.apply(colStats.genericGetMax());

      final int minVsLiteral = lit.comparator().compare(min, lit.value());
      final int literalVsMax = lit.comparator().compare(lit.value(), max);

      // is the min-max range within that of the predicate?
      boolean inRange =
          switch (pred.op()) {
              // min value is less than the literal
            case LT -> minVsLiteral < 0;
              // min value is less than or equal to the literal
            case LT_EQ -> minVsLiteral <= 0;
              // max value is > the literal
            case GT -> literalVsMax < 0;
              // max value is > or == to the literal
            case GT_EQ -> literalVsMax <= 0;
              // min <= lit and max >= min so one element
              // may match lit.
            case EQ -> minVsLiteral <= 0 && literalVsMax <= 0;
              // anything else
            default -> true;
          };

      return inRange;
    }

    /**
     * Evaluate an IN predicate against a shredded variant column's min/max statistics.
     *
     * <p>A row group can be skipped if no value in the IN set falls within [min, max].
     *
     * @param pred IN predicate
     * @param extract the bound extract term
     * @param parquetType the Parquet type of the shredded column
     * @param colStats column statistics
     * @param <T> type of the predicate values
     * @return true if the row group might match, false if it cannot match
     */
    @SuppressWarnings("unchecked")
    private <T> boolean evalMembershipPredicateOnShreddedVariant(
        BoundPredicate<T> pred,
        BoundExtract<T> extract,
        PrimitiveType parquetType,
        Statistics<?> colStats) {

      if (pred.op() != Expression.Operation.IN) {
        // not looking at other set member operations.
        return ROWS_MIGHT_MATCH;
      }
      Set<T> literalSet = pred.asSetPredicate().literalSet();

      if (literalSet.size() > IN_PREDICATE_LIMIT) {
        return ROWS_MIGHT_MATCH;
      }
      LOG.debug("Set membership evaluation");
      Function<Object, Object> converter =
          ParquetConversions.converterFromParquet(parquetType, extract.type());
      T min = (T) converter.apply(colStats.genericGetMin());
      T max = (T) converter.apply(colStats.genericGetMax());

      // keep only values that are >= min
      Collection<T> candidates =
          literalSet.stream().filter(v -> pred.term().comparator().compare(min, v) <= 0).toList();

      // nothing is above the minimum
      if (candidates.isEmpty()) {
        return ROWS_CANNOT_MATCH;
      }

      // second filter: keep only values that are <= max
      candidates =
          candidates.stream().filter(v -> pred.term().comparator().compare(max, v) >= 0).toList();

      final boolean match = candidates.isEmpty() ? ROWS_CANNOT_MATCH : ROWS_MIGHT_MATCH;
      LOG.debug("Outcome match={}", match);
      return match;
    }

    /**
     * Evaluate a Unary Predicate. Pulled out of the main compareVariant() call due to checkstyle
     * rejecting the complexity of that method.
     *
     * @param pred predicate being evaluated.
     * @param colStats column statistics
     * @param valueCount number of elements in the rowgroup
     * @param <T> type of predicate
     * @return true if the rowgroup should be read.
     */
    private <T> boolean evalUnaryPredicate(
        BoundPredicate<T> pred, Statistics<?> colStats, long valueCount) {
      LOG.debug("Evaluating unary predicate: {}", pred.op());
      switch (pred.op()) {
        case IS_NULL -> {
          // If every row has a non-null typed value, no row can match IS_NULL
          if (!colStats.isEmpty() && colStats.isNumNullsSet() && colStats.getNumNulls() == 0) {
            return ROWS_CANNOT_MATCH;
          }
          return ROWS_MIGHT_MATCH;
        }
        case NOT_NULL -> {
          // If every row has a null typed value, no row can match NOT_NULL
          if (!colStats.isEmpty() && allNulls(colStats, valueCount)) {
            return ROWS_CANNOT_MATCH;
          }
          return ROWS_MIGHT_MATCH;
        }
        default -> {
          return ROWS_MIGHT_MATCH;
        }
      }
    }

    @Override
    public <T> Boolean handleNonReference(Bound<T> term) {
      return ROWS_MIGHT_MATCH;
    }
  }

  /**
   * Older versions of Parquet statistics which may have a null count but undefined min and max
   * statistics. This is similar to the current behavior when NaN values are present.
   *
   * <p>This is specifically for 1.5.0-CDH Parquet builds and later which contain the different
   * unusual hasNonNull behavior. OSS Parquet builds are not effected because PARQUET-251 prohibits
   * the reading of these statistics from versions of Parquet earlier than 1.8.0.
   *
   * @param statistics Statistics to check
   * @return true if min and max statistics are null
   */
  static boolean nullMinMax(Statistics statistics) {
    return statistics.getMaxBytes() == null || statistics.getMinBytes() == null;
  }

  /**
   * The internal logic of Parquet-MR says that if numNulls is set but hasNonNull value is false,
   * then the min/max of the column are undefined.
   *
   * <p>Parquet also sets this for a float/double if there is a NaN in the row group.
   */
  static boolean minMaxUndefined(Statistics statistics) {
    return (statistics.isNumNullsSet() && !statistics.hasNonNullValue()) || nullMinMax(statistics);
  }

  static boolean allNulls(Statistics statistics, long valueCount) {
    return statistics.isNumNullsSet() && valueCount == statistics.getNumNulls();
  }

  private static boolean mayContainNull(Statistics statistics) {
    return !statistics.isNumNullsSet() || statistics.getNumNulls() > 0;
  }

  /**
   * Scan the schema for variant types and build a map of variant columns.
   *
   * @param fileSchema file schema.
   * @return the map of variant column names, may be empty.
   */
  private Map<Integer, String> buildVariantColumnNames(MessageType fileSchema) {
    LOG.debug("Building variant column names...");
    Map<Integer, String> names = Maps.newHashMap();
    for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
      if (field.getId() != null) {
        int id = field.getId().intValue();
        Type icebergType = schema.findType(id);
        if (icebergType != null && icebergType.isVariantType()) {
          names.put(id, field.getName());
        }
      }
    }
    LOG.debug("Found {} names", names.size());
    return names;
  }

  /**
   * Does an expression have variant predicates?
   *
   * @param expr expression to evaluate.
   * @return true if any part of the expression refers to a variant.
   */
  private static boolean hasVariantPredicates(Expression expr) {
    return ExpressionVisitors.visit(expr, HasVariantPredicateVisitor.INSTANCE);
  }

  /**
   * Visitor for scanning an expression for variants.
   *
   * <p>This isn't trying to evaluate the expression, so and/or/not aggregate the results, rather
   * than apply boolean predicates on child nodes.
   */
  private static final class HasVariantPredicateVisitor extends BoundExpressionVisitor<Boolean> {
    static final HasVariantPredicateVisitor INSTANCE = new HasVariantPredicateVisitor();

    @Override
    public Boolean alwaysTrue() {
      return false;
    }

    @Override
    public Boolean alwaysFalse() {
      return false;
    }

    @Override
    public Boolean not(Boolean result) {
      return result;
    }

    @Override
    public Boolean and(Boolean leftResult, Boolean rightResult) {
      return leftResult || rightResult;
    }

    @Override
    public Boolean or(Boolean leftResult, Boolean rightResult) {
      return leftResult || rightResult;
    }

    @Override
    public <T> Boolean predicate(BoundPredicate<T> pred) {
      return pred.term() instanceof BoundExtract;
    }

    @Override
    public <T> Boolean handleNonReference(Bound<T> term) {
      return false;
    }
  }

  /**
   * The number of times shredded metric predicates have been evaluated.
   *
   * @return zero or a positive integer
   */
  @VisibleForTesting
  static long variantPredicatesShreddedMetricsEvaluated() {
    return VARIANT_PREDICATES_SHREDDED_METRICS_EVALUATED.get();
  }

  /**
   * The number of row groups proven skippable by a shredded variant predicate. Will always be equal
   * to or less than the value of {@link #variantPredicatesShreddedMetricsEvaluated()}.
   *
   * @return zero or a positive integer
   */
  @VisibleForTesting
  static long variantPredicatesShreddedSkipped() {
    return VARIANT_PREDICATES_SHREDDED_SKIPPED.get();
  }

  /** Reset both shredded metrics counters (examined and skipped). */
  @VisibleForTesting
  static void resetShreddedMetricsCounters() {
    VARIANT_PREDICATES_SHREDDED_METRICS_EVALUATED.set(0);
    VARIANT_PREDICATES_SHREDDED_SKIPPED.set(0);
  }
}
