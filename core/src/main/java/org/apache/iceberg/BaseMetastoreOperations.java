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
package org.apache.iceberg;

import static org.apache.iceberg.TableProperties.COMMIT_NUM_STATUS_CHECKS;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_STATUS_CHECKS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MAX_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_MIN_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS_DEFAULT;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseMetastoreOperations {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreOperations.class);

  /**
   * Attempt to load the table and see if any current or past metadata location matches the one we
   * were attempting to set. This is used as a last resort when we are dealing with exceptions that
   * may indicate the commit has failed but don't have proof that this is the case. Note that all
   * the previous locations must also be searched on the chance that a second committer was able to
   * successfully commit on top of our commit.
   *
   * @param tableName full name of the table
   * @param newMetadataLocation the path of the new commit file
   * @param properties properties for retry
   * @param loadMetadataLocations supply all the metadata locations
   * @return Commit Status of Success, Failure or Unknown
   */
  protected BaseMetastoreTableOperations.CommitStatus checkCommitStatus(
      String tableName,
      String newMetadataLocation,
      Map<String, String> properties,
      Supplier<List<String>> loadMetadataLocations) {
    int maxAttempts =
        PropertyUtil.propertyAsInt(
            properties, COMMIT_NUM_STATUS_CHECKS, COMMIT_NUM_STATUS_CHECKS_DEFAULT);
    long minWaitMs =
        PropertyUtil.propertyAsLong(
            properties, COMMIT_STATUS_CHECKS_MIN_WAIT_MS, COMMIT_STATUS_CHECKS_MIN_WAIT_MS_DEFAULT);
    long maxWaitMs =
        PropertyUtil.propertyAsLong(
            properties, COMMIT_STATUS_CHECKS_MAX_WAIT_MS, COMMIT_STATUS_CHECKS_MAX_WAIT_MS_DEFAULT);
    long totalRetryMs =
        PropertyUtil.propertyAsLong(
            properties,
            COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS,
            COMMIT_STATUS_CHECKS_TOTAL_WAIT_MS_DEFAULT);

    AtomicReference<BaseMetastoreTableOperations.CommitStatus> status =
        new AtomicReference<>(BaseMetastoreTableOperations.CommitStatus.UNKNOWN);

    Tasks.foreach(newMetadataLocation)
        .retry(maxAttempts)
        .suppressFailureWhenFinished()
        .exponentialBackoff(minWaitMs, maxWaitMs, totalRetryMs, 2.0)
        .onFailure(
            (location, checkException) ->
                LOG.error("Cannot check if commit to {} exists.", tableName, checkException))
        .run(
            location -> {
              List<String> allMetadataLocations = loadMetadataLocations.get();
              boolean commitSuccess = allMetadataLocations.contains(newMetadataLocation);

              if (commitSuccess) {
                LOG.info(
                    "Commit status check: Commit to {} of {} succeeded",
                    tableName,
                    newMetadataLocation);
                status.set(BaseMetastoreTableOperations.CommitStatus.SUCCESS);
              } else {
                LOG.warn(
                    "Commit status check: Commit to {} of {} unknown, new metadata location is not current "
                        + "or in history",
                    tableName,
                    newMetadataLocation);
              }
            });

    if (status.get() == BaseMetastoreTableOperations.CommitStatus.UNKNOWN) {
      LOG.error(
          "Cannot determine commit state to {}. Failed during checking {} times. "
              + "Treating commit state as unknown.",
          tableName,
          maxAttempts);
    }
    return status.get();
  }
}
