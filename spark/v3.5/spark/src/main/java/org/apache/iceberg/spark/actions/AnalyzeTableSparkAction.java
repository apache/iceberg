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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.AnalyzeTable;
import org.apache.iceberg.actions.ImmutableAnalyzeTable;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
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
  private Set<String> columns = ImmutableSet.of();
  private Set<String> types = StandardBlobTypes.blobTypes();
  private Long snapshotId;

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
    if (snapshotId == null) {
      snapshotId = table.currentSnapshot().snapshotId();
    }
    String desc = String.format("Analyzing table %s for snapshot id %s", table.name(), snapshotId);
    JobGroupInfo info = newJobGroupInfo("ANALYZE-TABLE", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    LOG.info("Starting the analysis of {} for snapshot {}", table.name(), snapshotId);
    List<AnalysisResult> analysisResults =
        types.stream()
            .map(
                statsName -> {
                  switch (statsName) {
                    case StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1:
                      return generateNDVAndCommit();
                    default:
                      return ImmutableAnalyzeTable.AnalysisResult.builder()
                          .type(statsName)
                          .addAllErrors(Lists.newArrayList("Stats type not supported"))
                          .build();
                  }
                })
            .collect(Collectors.toList());
    return ImmutableAnalyzeTable.Result.builder().analysisResults(analysisResults).build();
  }

  private boolean analyzableTypes(Set<String> columnNames) {
    return columnNames.stream()
        .anyMatch(
            columnName -> {
              Types.NestedField field = table.schema().findField(columnName);
              if (field == null) {
                throw new ValidationException("No column with %s name in the table", columnName);
              }
              Type.TypeID type = field.type().typeId();
              return type == Type.TypeID.INTEGER
                  || type == Type.TypeID.LONG
                  || type == Type.TypeID.STRING
                  || type == Type.TypeID.DOUBLE;
            });
  }

  private AnalysisResult generateNDVAndCommit() {
    try {
      if (snapshotId == null) {
        snapshotId = table.currentSnapshot().snapshotId();
      }

      StatisticsFile statisticsFile =
          NDVSketchGenerator.generateNDV(
              spark(), table, snapshotId, columns.toArray(new String[0]));
      table.updateStatistics().setStatistics(snapshotId, statisticsFile).commit();
      return ImmutableAnalyzeTable.AnalysisResult.builder()
          .type(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1)
          .build();
    } catch (IOException ioe) {
      List<String> errors = Lists.newArrayList();
      errors.add(ioe.getMessage());
      return ImmutableAnalyzeTable.AnalysisResult.builder()
          .type(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1)
          .addAllErrors(errors)
          .build();
    }
  }

  @Override
  public AnalyzeTable columns(String... columnNames) {
    Preconditions.checkArgument(
        columnNames != null && columnNames.length > 0, "Columns cannot be null/empty");
    Set<String> columnsSet = Sets.newHashSet(Arrays.asList(columnNames));
    Preconditions.checkArgument(
        analyzableTypes(columnsSet),
        "Cannot be applied to the given columns, since the column's type is not supported");
    this.columns = columnsSet;
    return this;
  }

  @Override
  public AnalyzeTable types(Set<String> statisticTypes) {
    Preconditions.checkArgument(
        Sets.newHashSet(StandardBlobTypes.blobTypes()).containsAll(statisticTypes),
        "type not supported");
    this.types = statisticTypes;
    return this;
  }

  @Override
  public AnalyzeTable snapshot(String snapshotIdStr) {
    this.snapshotId = Long.parseLong(snapshotIdStr);
    return this;
  }
}
