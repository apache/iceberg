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

import org.apache.iceberg.SnapshotUpdate;
import org.apache.spark.sql.connector.write.MergeSummary;

/** Base class for Spark write implementations with shared utility methods. */
abstract class BaseSparkWrite {

  protected void setMergeSummaryProperties(
      SnapshotUpdate<?> operation, MergeSummary mergeSummary) {
    setIfPositive(
        operation, "spark.merge-into.num-target-rows-copied", mergeSummary.numTargetRowsCopied());
    setIfPositive(
        operation, "spark.merge-into.num-target-rows-deleted", mergeSummary.numTargetRowsDeleted());
    setIfPositive(
        operation, "spark.merge-into.num-target-rows-updated", mergeSummary.numTargetRowsUpdated());
    setIfPositive(
        operation,
        "spark.merge-into.num-target-rows-inserted",
        mergeSummary.numTargetRowsInserted());
    setIfPositive(
        operation,
        "spark.merge-into.num-target-rows-matched-updated",
        mergeSummary.numTargetRowsMatchedUpdated());
    setIfPositive(
        operation,
        "spark.merge-into.num-target-rows-matched-deleted",
        mergeSummary.numTargetRowsMatchedDeleted());
    setIfPositive(
        operation,
        "spark.merge-into.num-target-rows-not-matched-by-source-updated",
        mergeSummary.numTargetRowsNotMatchedBySourceUpdated());
    setIfPositive(
        operation,
        "spark.merge-into.num-target-rows-not-matched-by-source-deleted",
        mergeSummary.numTargetRowsNotMatchedBySourceDeleted());
  }

  private void setIfPositive(SnapshotUpdate<?> operation, String key, long value) {
    if (value >= 0) {
      operation.set(key, String.valueOf(value));
    }
  }
}

