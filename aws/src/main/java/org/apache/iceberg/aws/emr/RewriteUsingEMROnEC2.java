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
package org.apache.iceberg.aws.emr;

import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_CLUSTER_ID;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_CLUSTER_ID_DEFAULT;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_SYNCHRONOUS_ENABLED;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_SYNCHRONOUS_ENABLED_DEFAULT;

import java.util.List;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.OptimizeTableUtil;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.Rewrite;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.AddJobFlowStepsRequest;
import software.amazon.awssdk.services.emr.model.AddJobFlowStepsResponse;
import software.amazon.awssdk.services.emr.model.DescribeStepRequest;
import software.amazon.awssdk.services.emr.model.DescribeStepResponse;
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig;
import software.amazon.awssdk.services.emr.model.StepConfig;
import software.amazon.awssdk.services.emr.model.StepState;

/**
 * An implementation of {@link Rewrite} to rewrite tables by launching Spark SQL jobs in EMR-on-EC2
 */
public class RewriteUsingEMROnEC2 implements Rewrite {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteUsingEMROnEC2.class);

  private TableOperations tableOperations;
  private String tableName;
  private EmrClient emrClient;

  private String emrClusterId;

  private boolean isSynchronousRewriteEnabled;

  @Override
  public void initialize(MetricsReport report) {
    Preconditions.checkArgument(null != report, "Invalid metrics report: null");
    this.tableOperations = ((CommitReport) report).tableOperations();
    this.tableName = ((CommitReport) report).tableName();
    this.emrClient = AwsClientFactories.defaultFactory().emr();
    this.emrClusterId =
        OptimizeTableUtil.propertyAsString(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_CLUSTER_ID,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_CLUSTER_ID_DEFAULT);
    Preconditions.checkArgument(
        null != emrClusterId,
        "%s should be be set",
        AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_CLUSTER_ID);
    this.isSynchronousRewriteEnabled =
        OptimizeTableUtil.propertyAsBoolean(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_SYNCHRONOUS_ENABLED,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_SYNCHRONOUS_ENABLED_DEFAULT);
  }

  @Override
  public void rewrite() {
    String stepId = submitEmrStep();
    // Wait for job to complete in case of synchronous rewrite
    if (isSynchronousRewriteEnabled) {
      waitForStepToComplete(stepId);
    }
    emrClient.close();
  }

  /**
   * Submit an EMR step to an EMR cluster
   *
   * @return EMR step id
   */
  private String submitEmrStep() {
    AddJobFlowStepsResponse response =
        emrClient.addJobFlowSteps(
            AddJobFlowStepsRequest.builder()
                .jobFlowId(emrClusterId)
                .steps(
                    StepConfig.builder()
                        .name(String.format("Rewrite job for %s", tableName))
                        .actionOnFailure(OptimizeTableUtil.EMR_STEP_CONTINUE)
                        .hadoopJarStep(
                            HadoopJarStepConfig.builder()
                                .jar(OptimizeTableUtil.COMMAND_RUNNER_JAR)
                                .args(buildArguments().toArray(new String[0]))
                                .build())
                        .build())
                .build());
    if (response != null && response.hasStepIds()) {
      String stepId = response.stepIds().get(OptimizeTableUtil.EMR_STEP_INDEX);
      LOG.info(
          "Rewrite job submitted for table {} on EMR-EC2 cluster {} (step {})",
          tableName,
          emrClusterId,
          stepId);
      return stepId;
    }
    return OptimizeTableUtil.BLANK;
  }

  /**
   * Build the {@link HadoopJarStepConfig} arguments
   *
   * @return {@link HadoopJarStepConfig} arguments
   */
  private List<String> buildArguments() {
    List<String> arguments = Lists.newArrayList(OptimizeTableUtil.buildSparkSqlCommand());
    arguments.addAll(OptimizeTableUtil.buildSparkSqlExtensions());
    arguments.addAll(OptimizeTableUtil.buildSparkSqlCatalog(tableOperations));
    arguments.addAll(OptimizeTableUtil.buildSparkSqlCatalogImplementation(tableOperations));
    arguments.addAll(OptimizeTableUtil.buildDefaultSparkConfigurations(tableOperations));
    arguments.addAll(OptimizeTableUtil.buildSparkSqlExecuteFlag());
    arguments.addAll(
        OptimizeTableUtil.buildSparkSqlRewriteDataFilesCommand(tableName, tableOperations));
    return arguments;
  }

  /**
   * Checks job status regularly for a specific EMR step id after {@link
   * OptimizeTableUtil#SLEEP_WAIT_DURATION_MS} duration
   *
   * @param stepId EMR step id
   */
  private void waitForStepToComplete(String stepId) {
    StepState stepState = stepState(emrClusterId, stepId);
    while (stepState == StepState.PENDING || stepState == StepState.RUNNING) {
      stepState = stepState(emrClusterId, stepId);
      LOG.info("Status of rewrite job: {}", stepState);
      try {
        Thread.sleep(OptimizeTableUtil.SLEEP_WAIT_DURATION_MS);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            String.format("Failed to request status of the rewrite job for table %s", tableName),
            e);
      }
    }
  }

  /**
   * Get EMR {@link StepState} for a given cluster id and step id
   *
   * @param clusterId EMR cluster id
   * @param stepId EMR step id
   * @return EMR {@link StepState}
   */
  private StepState stepState(String clusterId, String stepId) {
    DescribeStepResponse describeStepResponse =
        emrClient.describeStep(
            DescribeStepRequest.builder().clusterId(clusterId).stepId(stepId).build());
    return describeStepResponse.step().status().state();
  }
}
