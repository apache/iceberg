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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.FileRewriteRunner;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.spark.sql.SparkSession;

/** Test providers, registered in META-INF/services and used only when selected by name. */
public class TestRewriteRunnerProviders {

  private TestRewriteRunnerProviders() {}

  /** Rewrites with Spark's bin-pack runner and counts the groups it rewrites. */
  public static class Recording implements FileRewriteRunnerProvider {
    static final String NAME = "test-recording";
    static final AtomicInteger GROUPS = new AtomicInteger();

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public FileRewriteRunner<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> create(
        SparkSession spark, Table table) {
      return new SparkBinPackFileRewriteRunner(spark, table) {
        @Override
        public String description() {
          return "RECORDING";
        }

        @Override
        public Set<DataFile> rewrite(RewriteFileGroup group) {
          GROUPS.incrementAndGet();
          return super.rewrite(group);
        }
      };
    }
  }

  /** Declines every table. */
  public static class Declining implements FileRewriteRunnerProvider {
    static final String NAME = "test-declining";
    static final AtomicInteger CALLS = new AtomicInteger();

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public FileRewriteRunner<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> create(
        SparkSession spark, Table table) {
      CALLS.incrementAndGet();
      return null;
    }
  }
}
