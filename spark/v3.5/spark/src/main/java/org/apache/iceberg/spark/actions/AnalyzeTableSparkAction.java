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
package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.AnalyzeTable;
import org.apache.iceberg.actions.ImmutableAnalyzeTable;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Computes the statistic of the given columns and stores it as Puffin files. */
public class AnalyzeTableSparkAction extends BaseSparkAction<AnalyzeTableSparkAction>
    implements AnalyzeTable {

  private static final Logger LOG = LoggerFactory.getLogger(AnalyzeTableSparkAction.class);
  private final Table table;
  private Set<String> columnsToBeAnalyzed;
  private Set<String> statsToBecollected =
      Sets.newHashSet(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1);

  AnalyzeTableSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected AnalyzeTableSparkAction self() {
    return this;
  }

  @Override
  public Result execute() {
    String desc = String.format("Analyzing table %s", table.name());
    JobGroupInfo info = newJobGroupInfo("ANALYZE-TABLE", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    LOG.info("Starting the analysis of {}", table.name());
    validateColumns();
    List<AnalysisResult> analysisResults =
        statsToBecollected.stream()
            .map(
                statsName -> {
                  switch (statsName) {
                    case StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1:
                      return generateNDVAndCommit();
                    default:
                      return ImmutableAnalyzeTable.AnalysisResult.builder()
                          .statsName(statsName)
                          .statsCollected(false)
                          .addAllErrors(Lists.newArrayList("Stats type not supported"))
                          .build();
                  }
                })
            .collect(Collectors.toList());
    return ImmutableAnalyzeTable.Result.builder().analysisResults(analysisResults).build();
  }

  private void validateColumns() {
    validateEmptyColumns();
    validateTypes();
  }

  private void validateEmptyColumns() {
    if (columnsToBeAnalyzed == null || columnsToBeAnalyzed.isEmpty()) {
      throw new ValidationException("No columns to analyze for the table", table.name());
    }
  }

  private void validateTypes() {
    columnsToBeAnalyzed.forEach(
        columnName -> {
          Types.NestedField field = table.schema().findField(columnName);
          if (field == null) {
            throw new ValidationException("No column with %s name in the table", columnName);
          }
          Type type = field.type();
          if (type.isListType() || type.isMapType() || type.isNestedType() || type.isStructType()) {
            throw new ValidationException(
                "Analysis not supported on column %s of type %s", columnName, type.typeId());
          }
        });
  }

  private AnalysisResult generateNDVAndCommit() {
    try {
      StatisticsFile statisticsFile =
          NDVSketchGenerator.generateNDV(
              spark(), table, columnsToBeAnalyzed.toArray(new String[0]));
      table
          .updateStatistics()
          .setStatistics(table.currentSnapshot().snapshotId(), statisticsFile)
          .commit();
      return ImmutableAnalyzeTable.AnalysisResult.builder()
          .statsName(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1)
          .statsCollected(true)
          .build();
    } catch (IOException ioe) {
      List<String> errors = Lists.newArrayList();
      errors.add(ioe.getMessage());
      return ImmutableAnalyzeTable.AnalysisResult.builder()
          .statsName(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1)
          .statsCollected(false)
          .addAllErrors(errors)
          .build();
    }
  }

  @Override
  public AnalyzeTable columns(Set<String> columns) {
    this.columnsToBeAnalyzed = columns;
    return this;
  }

  @Override
  public AnalyzeTable stats(Set<String> statsToBeCollected) {
    this.statsToBecollected = statsToBeCollected;
    return this;
  }
}
