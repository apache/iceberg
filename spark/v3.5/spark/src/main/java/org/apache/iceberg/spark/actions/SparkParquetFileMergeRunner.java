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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of SparkBinPackFileRewriteRunner that is intended to use ParquetFileMerger for
 * efficient row-group level merging of Parquet files when applicable.
 *
 * <p>Currently, this class serves as a placeholder for future optimization where Parquet files can
 * be merged at the row-group level without full deserialization. For now, it delegates to the
 * standard Spark bin-pack rewrite logic.
 *
 * <p>The decision to use this runner vs. SparkBinPackFileRewriteRunner is controlled by the
 * configuration option {@code use-parquet-file-merger}.
 */
public class SparkParquetFileMergeRunner extends SparkBinPackFileRewriteRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SparkParquetFileMergeRunner.class);

  public SparkParquetFileMergeRunner(SparkSession spark, Table table) {
    super(spark, table);
  }

  @Override
  public String description() {
    return "PARQUET-MERGE";
  }

  @Override
  protected void doRewrite(String groupId, RewriteFileGroup group) {
    // Check if all files are Parquet format
    if (canUseMerger(group)) {
      LOG.info(
          "Processing {} Parquet files for potential row-group level merge (group: {})",
          group.rewrittenFiles().size(),
          groupId);
      // TODO: Implement ParquetFileMerger integration for row-group level merging
      // For now, fall back to standard Spark rewrite which still works correctly
    }

    // Use standard Spark rewrite (same as parent class)
    super.doRewrite(groupId, group);
  }

  private boolean canUseMerger(RewriteFileGroup group) {
    // Check if all files are Parquet format
    return group.rewrittenFiles().stream().allMatch(file -> file.format() == FileFormat.PARQUET);
  }
}
