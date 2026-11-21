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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.FileRewriteRunner;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.spark.sql.SparkSession;

/**
 * Supplies the {@link FileRewriteRunner} for bin-pack data file rewrites from outside Iceberg, for
 * example a native or accelerated engine.
 *
 * <p>Providers are discovered with {@link java.util.ServiceLoader} and used only when selected by
 * {@link #name()}: with the {@value RewriteDataFilesSparkAction#REWRITE_RUNNER_PROVIDER} option of
 * the action, or for a session with {@value
 * org.apache.iceberg.spark.SparkSQLProperties#REWRITE_RUNNER_PROVIDER}. The selected provider is
 * asked only when the rewrite uses bin-pack, chosen explicitly or by default; sort and z-order
 * rewrites never consult it.
 *
 * <p>Planning, commits, partial progress and validation stay with the action. A runner rewrites the
 * live rows of each {@link RewriteFileGroup} it is given into new data files, as Spark's bin-pack
 * runner would.
 */
public interface FileRewriteRunnerProvider {

  /** The name that selects this provider. */
  String name();

  /**
   * Returns the runner for a bin-pack rewrite of the table, or null to leave the rewrite to Spark's
   * bin-pack runner.
   *
   * @param spark the action's Spark session
   * @param table the table being rewritten
   * @return a runner, or null to decline
   */
  FileRewriteRunner<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> create(
      SparkSession spark, Table table);
}
