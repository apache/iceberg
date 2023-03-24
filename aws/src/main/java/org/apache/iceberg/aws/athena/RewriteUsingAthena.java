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
package org.apache.iceberg.aws.athena;

import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_ATHENA_DATABASE;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_ATHENA_DATABASE_DEFAULT;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_ATHENA_OUTPUT_BUCKET;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_ATHENA_OUTPUT_BUCKET_DEFAULT;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_SYNCHRONOUS_ENABLED;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_SYNCHRONOUS_ENABLED_DEFAULT;

import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.OptimizeTableUtil;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.Rewrite;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

/** An implementation of {@link Rewrite} to rewrite tables by launching OPTIMIZE jobs in Athena */
public class RewriteUsingAthena implements Rewrite {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteUsingAthena.class);

  private String tableName;

  private AthenaClient athenaClient;

  private boolean isSynchronousRewriteEnabled;

  private String athenaOutputBucket;

  private String athenaDatabase;

  @Override
  public void initialize(MetricsReport report) {
    Preconditions.checkArgument(null != report, "Invalid metrics report: null");
    TableOperations tableOperations = ((CommitReport) report).tableOperations();
    this.tableName = OptimizeTableUtil.tableName(((CommitReport) report).tableName());
    this.athenaClient = AwsClientFactories.defaultFactory().athena();
    this.isSynchronousRewriteEnabled =
        OptimizeTableUtil.propertyAsBoolean(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_SYNCHRONOUS_ENABLED,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_SYNCHRONOUS_ENABLED_DEFAULT);
    this.athenaOutputBucket =
        OptimizeTableUtil.propertyAsString(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_ATHENA_OUTPUT_BUCKET,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_ATHENA_OUTPUT_BUCKET_DEFAULT);
    Preconditions.checkArgument(null != athenaOutputBucket, "Invalid output bucket: null");
    this.athenaDatabase =
        OptimizeTableUtil.propertyAsString(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_ATHENA_DATABASE,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_ATHENA_DATABASE_DEFAULT);
  }

  @Override
  public void rewrite() {
    String queryExecutionId = submitAthenaQuery();
    // Wait for job to complete in case of synchronous rewrite
    if (isSynchronousRewriteEnabled) {
      waitForQueryToComplete(queryExecutionId);
    }
    athenaClient.close();
  }

  /**
   * Submits a sample query to Amazon Athena and returns the execution ID of the query.
   *
   * @return Query execution id
   */
  private String submitAthenaQuery() {
    try {
      // The QueryExecutionContext allows us to set the database.
      QueryExecutionContext queryExecutionContext =
          QueryExecutionContext.builder().database(athenaDatabase).build();

      // The result configuration specifies where the results of the query should go.
      ResultConfiguration resultConfiguration =
          ResultConfiguration.builder().outputLocation(athenaOutputBucket).build();

      StartQueryExecutionRequest startQueryExecutionRequest =
          StartQueryExecutionRequest.builder()
              .queryString(String.format(OptimizeTableUtil.ATHENA_QUERY_FORMAT, tableName))
              .queryExecutionContext(queryExecutionContext)
              .resultConfiguration(resultConfiguration)
              .build();

      StartQueryExecutionResponse startQueryExecutionResponse =
          athenaClient.startQueryExecution(startQueryExecutionRequest);
      LOG.info(
          "Rewrite job submitted for table {}.{} on Athena (query execution id: {})",
          athenaDatabase,
          tableName,
          startQueryExecutionResponse.queryExecutionId());
      return startQueryExecutionResponse.queryExecutionId();

    } catch (AthenaException e) {
      LOG.error("Exception occurred while submitting query: ", e);
    }
    return OptimizeTableUtil.BLANK;
  }

  /**
   * Wait for an Amazon Athena query to complete, fail or to be cancelled.
   *
   * @param queryExecutionId Query execution id
   */
  private void waitForQueryToComplete(String queryExecutionId) {
    GetQueryExecutionRequest getQueryExecutionRequest =
        GetQueryExecutionRequest.builder().queryExecutionId(queryExecutionId).build();

    GetQueryExecutionResponse getQueryExecutionResponse;
    boolean isQueryStillRunning = true;
    while (isQueryStillRunning) {
      getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
      String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
      if (queryState.equals(QueryExecutionState.FAILED.toString())) {
        throw new RuntimeException(
            "The Amazon Athena query failed to run with error message: "
                + getQueryExecutionResponse.queryExecution().status().stateChangeReason());
      } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
        throw new RuntimeException("The Amazon Athena query was cancelled.");
      } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
        isQueryStillRunning = false;
      } else {
        // Sleep an amount of time before retrying again.
        try {
          Thread.sleep(OptimizeTableUtil.SLEEP_WAIT_DURATION_MS);
        } catch (InterruptedException e) {
          LOG.error("Exception occurred while waiting for query to complete: ", e);
        }
      }
      LOG.info("Status of rewrite job: {}", queryState);
    }
  }
}
