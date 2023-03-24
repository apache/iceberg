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
package org.apache.iceberg.aws.reporter;

import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_COMMIT_THRESHOLD;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_COMMIT_THRESHOLD_DEFAULT;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_IMPL;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_IMPL_DEFAULT;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_TIME_MS_THRESHOLD;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_TIME_THRESHOLD_MS_DEFAULT;

import java.util.List;
import java.util.ListIterator;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.OptimizeTableUtil;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.Rewrite;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link MetricsReporter} to decide and launch rewrite jobs. The
 * implementation lets users to collect table activities during writes to make better decisions on
 * how to optimize each table differently.
 */
public class OptimizeTableReporter implements MetricsReporter {

  private static final Logger LOG = LoggerFactory.getLogger(OptimizeTableReporter.class);

  @Override
  public void report(MetricsReport report) {
    Preconditions.checkArgument(null != report, "Invalid metrics report: null");
    // CommitReport restricts RewriteDataFilesReporter only to the write path
    if (report instanceof CommitReport) {
      LOG.info("Received metrics report: {}", report);
      TableOperations tableOperations = ((CommitReport) report).tableOperations();
      // If the table should be rewritten to eliminate small files
      if (shouldRewrite(tableOperations)) {
        String rewriteImpl =
            OptimizeTableUtil.propertyAsString(
                tableOperations,
                AUTO_OPTIMIZE_REWRITE_DATA_FILES_IMPL,
                AUTO_OPTIMIZE_REWRITE_DATA_FILES_IMPL_DEFAULT);
        Preconditions.checkArgument(
            null != rewriteImpl,
            "Auto optimize rewrite data files implementation %s should be set",
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_IMPL);
        Rewrite rewrite = OptimizeTableUtil.loadRewrite(rewriteImpl);
        rewrite.initialize(report);
        rewrite.rewrite();
      }
    }
  }

  /**
   * Determine if the table should be optimized based upon the defined thresholds (commit based,
   * time based)
   *
   * @param tableOperations {@link TableOperations} instance
   * @return true/false based upon which thresholds are met
   */
  private boolean shouldRewrite(TableOperations tableOperations) {
    List<Snapshot> snapshotList = tableOperations.current().snapshots();
    ListIterator<Snapshot> listIterator = snapshotList.listIterator(snapshotList.size());
    int numSnapshotsSinceLastRewrite = 0;
    int commitThreshold =
        OptimizeTableUtil.propertyAsInt(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_COMMIT_THRESHOLD,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_COMMIT_THRESHOLD_DEFAULT);
    int timeThreshold =
        OptimizeTableUtil.propertyAsInt(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_TIME_MS_THRESHOLD,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_TIME_THRESHOLD_MS_DEFAULT);
    // Traverse back from the latest snapshot
    while (listIterator.hasPrevious()) {
      Snapshot snapshot = listIterator.previous();
      // Reached the latest rewrite snapshot without meeting any thresholds, so return false
      if (snapshot.operation().equals(DataOperations.REPLACE)) {
        return false;
      }

      // Met the time threshold, so return true
      if (System.currentTimeMillis() - snapshot.timestampMillis() >= timeThreshold) {
        LOG.info("Time threshold has been met");
        return true;
      }

      // Met the commit threshold, so return true
      numSnapshotsSinceLastRewrite++;
      if (numSnapshotsSinceLastRewrite >= commitThreshold) {
        LOG.info("Commit threshold has been met");
        return true;
      }
    }

    // Traversed the entire snapshot list without meeting any thresholds, so return false
    return false;
  }
}
