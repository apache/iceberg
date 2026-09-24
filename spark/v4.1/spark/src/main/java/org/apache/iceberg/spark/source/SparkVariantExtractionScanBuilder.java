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

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.TimeTravel;
import org.apache.iceberg.spark.data.SparkVariantExtractionUtil;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.SupportsPushDownVariantExtractions;
import org.apache.spark.sql.connector.read.VariantExtraction;
import org.apache.spark.sql.types.VariantType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SparkScanBuilder} for tables with one or more top-level VARIANT columns.
 *
 * <p>Spark's {@code buildScanWithPushedVariants} is unsafe when a scan has multiple variant columns
 * and only a subset of extractions are accepted ({@code rewriteExpr} would emit invalid {@code
 * GetStructField} on columns whose type was not rewritten). This builder uses an all-or-nothing
 * policy: decline the entire batch if any extraction is unsupported, or if any full-variant slot
 * ({@code expectedDataType = VariantType}, path {@code $}) is present.
 *
 * <p>{@link #pushVariantExtractions} receives extractions collected from {@code Project} and {@code
 * Filter} on each scan subtree ({@code PhysicalOperation}); it does not walk through {@code Join}
 * or {@code Aggregate}. Typed paths usually come from filters. Plain {@code VARIANT} attributes in
 * the visible project list also add a full-variant slot ({@code expectedDataType = VariantType},
 * Spark path {@code $}; for example when Spark defaults to {@code scan.output}, or passthroughs the
 * raw column on a join branch). {@code variant_get} in {@code GROUP BY} or aggregate functions is
 * not collected; extract in a subquery first if agg pushdown is needed.
 *
 * <p>Iceberg declines batches that include a full-variant slot because accepting only the typed
 * extractions would rewrite the scan to a struct while {@code variant_get} above the join/aggregate
 * barrier still references the pre-rewrite attribute, causing binding failures or {@code
 * ClassCastException}. For example TPC-DS q42 on the item scan:
 *
 * <pre>{@code
 * HashAggregate GROUP BY variant_get(item_data, '$.category', ...)
 *   Project [..., variant_get(item_data, '$.category', ...)]   // above Join — not collected
 *     Join ON ss.ss_item_sk = i.i_item_sk
 *       Project [i_item_sk, item_data]                         // plain passthrough → full variant
 *         Filter variant_get(item_data, '$.manager_id', ...) = ...
 *           Scan item                                          // PhysicalOperation stops here
 * }</pre>
 */
class SparkVariantExtractionScanBuilder extends SparkScanBuilder
    implements SupportsPushDownVariantExtractions {

  private static final Logger LOG =
      LoggerFactory.getLogger(SparkVariantExtractionScanBuilder.class);

  SparkVariantExtractionScanBuilder(
      SparkSession spark, Table table, CaseInsensitiveStringMap options) {
    super(spark, table, options);
  }

  SparkVariantExtractionScanBuilder(
      SparkSession spark,
      Table table,
      Schema schema,
      Snapshot snapshot,
      String branch,
      TimeTravel timeTravel,
      CaseInsensitiveStringMap options) {
    super(spark, table, schema, snapshot, branch, timeTravel, options);
  }

  @Override
  public boolean[] pushVariantExtractions(VariantExtraction[] extractions) {
    // Default false: Spark treats each extraction as declined unless explicitly accepted.
    boolean[] accepted = new boolean[extractions.length];

    if (!readConf().variantExtractionPushDownEnabled()) {
      return accepted;
    }

    boolean[] candidates = new boolean[extractions.length];
    int typedExtractions = 0;
    int typedCandidates = 0;

    for (int i = 0; i < extractions.length; i++) {
      VariantExtraction extraction = extractions[i];
      if (extraction.expectedDataType() instanceof VariantType) {
        // Full-variant slot (expectedDataType = VariantType): decline the batch. See class javadoc.
        return accepted;
      }

      typedExtractions += 1;
      String[] colPath = extraction.columnName();
      boolean candidate =
          colPath.length == 1
              && isVariantColumn(colPath[0])
              && SparkVariantExtractionUtil.isSupportedExtractionPath(
                  SparkVariantExtractionUtil.extractionPath(extraction))
              && SparkVariantExtractionUtil.isSupportedPushdownTargetType(
                  extraction.expectedDataType());
      candidates[i] = candidate;
      if (candidate) {
        typedCandidates += 1;
      }
    }

    // Accept all typed extractions in the batch, including the same path with different target
    // types
    // when both appear in the visible Project/Filter (e.g. SELECT variant_get(v,'$.size','double'),
    // variant_get(v,'$.size','long')). variant_get inside Aggregate is not collected by Spark.
    // The Parquet reader shares one physical column read per path and casts to each target type.
    boolean acceptAllTyped = typedExtractions > 0 && typedCandidates == typedExtractions;

    if (acceptAllTyped) {
      for (int i = 0; i < extractions.length; i++) {
        accepted[i] = candidates[i];
      }

      LOG.info(
          "Accepted {}/{} variant extraction(s) for scan of table {}",
          typedCandidates,
          extractions.length,
          table().name());
      setVariantExtractions(extractions, accepted);
    } else if (typedCandidates > 0) {
      LOG.info(
          "Declined variant extraction pushdown for table {} because {} of {} typed extraction(s)"
              + " have unsupported paths, target types, or reference non-variant columns",
          table().name(),
          typedExtractions - typedCandidates,
          typedExtractions);
    }

    return accepted;
  }

  private boolean isVariantColumn(String colName) {
    org.apache.iceberg.types.Types.NestedField field = schema().findField(colName);
    return field != null && field.type().typeId() == Type.TypeID.VARIANT;
  }
}
