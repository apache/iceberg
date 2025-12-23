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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkV2Filters;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class SparkRuntimeFilterableScan extends SparkPartitioningAwareScan<PartitionScanTask>
    implements SupportsRuntimeV2Filtering {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeFilterableScan.class);

  private final List<Expression> runtimeFilters;

  protected SparkRuntimeFilterableScan(
      SparkSession spark,
      Table table,
      Schema schema,
      Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan,
      SparkReadConf readConf,
      Schema projection,
      List<Expression> filters,
      Supplier<ScanReport> scanReportSupplier) {
    super(spark, table, schema, scan, readConf, projection, filters, scanReportSupplier);
    this.runtimeFilters = Lists.newArrayList();
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

    Map<Integer, String> quotedNameById = SparkSchemaUtil.indexQuotedNameById(projection());

    // the optimizer will look for an equality condition with filter attributes in a join
    // as the scan has been already planned, filtering can only be done on projected attributes
    // that's why only partition source fields that are part of the read schema can be reported

    return partitionFieldSourceIds.stream()
        .filter(fieldId -> projection().findField(fieldId) != null)
        .map(fieldId -> Spark3Util.toNamedReference(quotedNameById.get(fieldId)))
        .toArray(NamedReference[]::new);
  }

  @Override
  public void filter(Predicate[] predicates) {
    Expression runtimeFilter = convertRuntimePredicates(predicates);

    if (runtimeFilter != Expressions.alwaysTrue()) {
      Map<Integer, Evaluator> evaluatorsBySpecId = Maps.newHashMap();

      for (PartitionSpec spec : specs()) {
        Expression inclusiveExpr =
            Projections.inclusive(spec, caseSensitive()).project(runtimeFilter);
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
          ExpressionUtil.toSanitizedString(runtimeFilter));

      // don't invalidate tasks if the runtime filter had no effect to avoid planning splits again
      if (filteredTasks.size() < tasks().size()) {
        resetTasks(filteredTasks);
      }

      // save the evaluated filter for equals/hashCode
      runtimeFilters.add(runtimeFilter);
    }
  }

  protected Map<String, DeleteFileSet> rewritableDeletes(boolean forDVs) {
    Map<String, DeleteFileSet> rewritableDeletes = Maps.newHashMap();

    for (ScanTask task : tasks()) {
      FileScanTask fileScanTask = task.asFileScanTask();
      for (DeleteFile deleteFile : fileScanTask.deletes()) {
        if (shouldRewrite(deleteFile, forDVs)) {
          rewritableDeletes
              .computeIfAbsent(fileScanTask.file().location(), ignored -> DeleteFileSet.create())
              .add(deleteFile);
        }
      }
    }

    return rewritableDeletes;
  }

  // for DVs all position deletes must be rewritten
  // for position deletes, only file-scoped deletes must be rewritten
  private boolean shouldRewrite(DeleteFile deleteFile, boolean forDVs) {
    if (forDVs) {
      return deleteFile.content() != FileContent.EQUALITY_DELETES;
    }

    return ContentFileUtil.isFileScoped(deleteFile);
  }

  // at this moment, Spark can only pass IN filters for a single attribute
  // if there are multiple filter attributes, Spark will pass two separate IN filters
  private Expression convertRuntimePredicates(Predicate[] predicates) {
    Expression filter = Expressions.alwaysTrue();

    for (Predicate predicate : predicates) {
      Expression expr = SparkV2Filters.convert(predicate);
      if (expr != null) {
        try {
          Binder.bind(projection().asStruct(), expr, caseSensitive());
          filter = Expressions.and(filter, expr);
        } catch (ValidationException e) {
          LOG.warn("Failed to bind {} to expected schema, skipping runtime filter", expr, e);
        }
      } else {
        LOG.warn("Unsupported runtime filter {}", predicate);
      }
    }

    return filter;
  }

  protected String runtimeFiltersDesc() {
    return Spark3Util.describe(runtimeFilters);
  }
}
