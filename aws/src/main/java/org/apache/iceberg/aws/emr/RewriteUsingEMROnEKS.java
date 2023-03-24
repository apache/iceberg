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
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_RELEASE_LABEL;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_RELEASE_LABEL_DEFAULT;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_UPLOAD_BUCKET;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_UPLOAD_BUCKET_DEFAULT;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_EXECUTION_ROLE;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_EXECUTION_ROLE_DEFAULT;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_SYNCHRONOUS_ENABLED;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_SYNCHRONOUS_ENABLED_DEFAULT;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.OptimizeTableUtil;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.Rewrite;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emrcontainers.EmrContainersClient;
import software.amazon.awssdk.services.emrcontainers.model.CloudWatchMonitoringConfiguration;
import software.amazon.awssdk.services.emrcontainers.model.ConfigurationOverrides;
import software.amazon.awssdk.services.emrcontainers.model.DescribeJobRunRequest;
import software.amazon.awssdk.services.emrcontainers.model.DescribeJobRunResponse;
import software.amazon.awssdk.services.emrcontainers.model.JobDriver;
import software.amazon.awssdk.services.emrcontainers.model.JobRunState;
import software.amazon.awssdk.services.emrcontainers.model.MonitoringConfiguration;
import software.amazon.awssdk.services.emrcontainers.model.S3MonitoringConfiguration;
import software.amazon.awssdk.services.emrcontainers.model.SparkSqlJobDriver;
import software.amazon.awssdk.services.emrcontainers.model.StartJobRunRequest;
import software.amazon.awssdk.services.emrcontainers.model.StartJobRunResponse;

/**
 * An implementation of {@link Rewrite} to rewrite tables by launching Spark SQL jobs in EMR-on-EKS
 */
public class RewriteUsingEMROnEKS implements Rewrite {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteUsingEMROnEKS.class);

  private TableOperations tableOperations;
  private String tableName;
  private EmrContainersClient emrContainersClient;
  private String emrClusterId;
  private String emrReleaseLabel;
  private String emrUploadBucket;
  private String executionRole;
  private String queryFilePath;
  private boolean isSynchronousRewriteEnabled;

  @Override
  public void initialize(MetricsReport report) {
    Preconditions.checkArgument(null != report, "Invalid metrics report: null");
    this.tableOperations = ((CommitReport) report).tableOperations();
    this.tableName = ((CommitReport) report).tableName();
    this.emrContainersClient = AwsClientFactories.defaultFactory().emrContainers();
    this.emrClusterId =
        OptimizeTableUtil.propertyAsString(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_CLUSTER_ID,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_CLUSTER_ID_DEFAULT);
    Preconditions.checkArgument(
        null != emrClusterId,
        "%s should be be set",
        AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_CLUSTER_ID);
    this.executionRole =
        OptimizeTableUtil.propertyAsString(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_EXECUTION_ROLE,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_EXECUTION_ROLE_DEFAULT);
    Preconditions.checkArgument(
        null != executionRole,
        "%s should be be set",
        AUTO_OPTIMIZE_REWRITE_DATA_FILES_EXECUTION_ROLE);
    this.emrReleaseLabel =
        OptimizeTableUtil.propertyAsString(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_RELEASE_LABEL,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_RELEASE_LABEL_DEFAULT);
    Preconditions.checkArgument(
        null != emrReleaseLabel,
        "%s should be be set",
        AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_RELEASE_LABEL);
    this.emrUploadBucket =
        OptimizeTableUtil.propertyAsString(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_UPLOAD_BUCKET,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_UPLOAD_BUCKET_DEFAULT);
    Preconditions.checkArgument(
        null != emrUploadBucket,
        "%s should be be set",
        AUTO_OPTIMIZE_REWRITE_DATA_FILES_EMR_UPLOAD_BUCKET);
    this.isSynchronousRewriteEnabled =
        OptimizeTableUtil.propertyAsBoolean(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_SYNCHRONOUS_ENABLED,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_SYNCHRONOUS_ENABLED_DEFAULT);
    this.queryFilePath =
        String.format("%s/query/query-%s.sql", emrUploadBucket, java.util.UUID.randomUUID());
  }

  @Override
  public void rewrite() {
    createQueryFile();
    String jobId = submitJob();
    // Wait for job to complete in case of synchronous rewrite
    if (isSynchronousRewriteEnabled) {
      waitForJobToComplete(jobId);
    }
    emrContainersClient.close();
  }

  /**
   * Submit a job to an EMR-on-EKS cluster
   *
   * @return job id
   */
  private String submitJob() {
    StartJobRunResponse response =
        emrContainersClient.startJobRun(
            StartJobRunRequest.builder()
                .name(String.format("RewriteJob-%s", tableName))
                .virtualClusterId(emrClusterId)
                .executionRoleArn(executionRole)
                .releaseLabel(emrReleaseLabel)
                .jobDriver(
                    JobDriver.builder()
                        .sparkSqlJobDriver(
                            SparkSqlJobDriver.builder()
                                .entryPoint(queryFilePath)
                                .sparkSqlParameters(buildSparkSqlParameters())
                                .build())
                        .build())
                .configurationOverrides(
                    ConfigurationOverrides.builder()
                        .monitoringConfiguration(
                            MonitoringConfiguration.builder()
                                .persistentAppUI(OptimizeTableUtil.PERSISTENT_APP_UI_ENABLED)
                                .cloudWatchMonitoringConfiguration(
                                    CloudWatchMonitoringConfiguration.builder()
                                        .logGroupName(
                                            OptimizeTableUtil.EMR_CONTAINERS_LOG_GROUP_NAME)
                                        .logStreamNamePrefix(
                                            OptimizeTableUtil.EMR_CONTAINERS_LOG_PREFIX)
                                        .build())
                                .s3MonitoringConfiguration(
                                    S3MonitoringConfiguration.builder()
                                        .logUri(String.format("%s/logs/", emrUploadBucket))
                                        .build())
                                .build())
                        .build())
                .build());
    if (response != null) {
      String jobId = response.id();
      LOG.info(
          "Rewrite job submitted for table {} on EMR-on-EKS cluster {} (job id {})",
          tableName,
          emrClusterId,
          jobId);
      return jobId;
    }
    return OptimizeTableUtil.BLANK;
  }

  /** Create query file containing the Spark SQL in queryFilePath */
  private void createQueryFile() {
    OutputFile out = tableOperations.io().newOutputFile(queryFilePath);
    try (OutputStream os = out.createOrOverwrite()) {
      StringBuilder stringBuilder = new StringBuilder();
      for (String parameter :
          OptimizeTableUtil.buildSparkSqlRewriteDataFilesCommand(tableName, tableOperations)) {
        stringBuilder.append(parameter);
        stringBuilder.append(OptimizeTableUtil.BLANK);
      }
      IOUtils.write(stringBuilder.toString(), os);
    } catch (IOException e) {
      LOG.error("Exception occurred while writing the {}: ", queryFilePath, e);
    }
  }

  /**
   * Build the Spark SQL Parameters
   *
   * @return Spark SQL Parameters
   */
  private String buildSparkSqlParameters() {
    StringBuilder stringBuilder = new StringBuilder();
    List<String> parameters = Lists.newArrayList(OptimizeTableUtil.buildSparkSqlExtensions());
    parameters.addAll(OptimizeTableUtil.buildSparkSqlCatalog(tableOperations));
    parameters.addAll(OptimizeTableUtil.buildSparkSqlCatalogImplementation(tableOperations));
    parameters.addAll(OptimizeTableUtil.buildDefaultSparkConfigurations(tableOperations));
    parameters.addAll(
        OptimizeTableUtil.buildSparkKubernetesFileUploadPath(
            String.format("%s/upload/", emrUploadBucket)));
    parameters.addAll(OptimizeTableUtil.buildIcebergJarPath());

    for (String parameter : parameters) {
      stringBuilder.append(parameter);
      stringBuilder.append(OptimizeTableUtil.BLANK);
    }

    return stringBuilder.toString();
  }

  /**
   * Checks job status regularly for a specific EMR job id after {@link
   * OptimizeTableUtil#SLEEP_WAIT_DURATION_MS} duration
   */
  private void waitForJobToComplete(String jobId) {
    JobRunState jobRunState = jobState(emrClusterId, jobId);
    while (jobRunState == JobRunState.SUBMITTED
        || jobRunState == JobRunState.PENDING
        || jobRunState == JobRunState.RUNNING) {
      jobRunState = jobState(emrClusterId, jobId);
      LOG.info("Status of rewrite job: {}", jobRunState);
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
   * Get EMR {@link JobRunState} for a given cluster id and job id
   *
   * @param clusterId EMR cluster id
   * @param jobId EMR job id
   * @return EMR {@link JobRunState}
   */
  private JobRunState jobState(String clusterId, String jobId) {
    DescribeJobRunResponse describeJobRunResponse =
        emrContainersClient.describeJobRun(
            DescribeJobRunRequest.builder().virtualClusterId(clusterId).id(jobId).build());
    return describeJobRunResponse.jobRun().state();
  }
}
