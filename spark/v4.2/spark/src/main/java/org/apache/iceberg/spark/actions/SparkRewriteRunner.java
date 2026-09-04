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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.FileRewriteRunner;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.actions.RewriteGroupBase;
import org.apache.iceberg.actions.RewritePositionDeleteFiles;
import org.apache.iceberg.actions.RewritePositionDeletesGroup;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.SparkSession;

/**
 * Base class for Spark file rewrite runners which implement the {@link FileRewriteRunner} API for
 * Spark based file rewrites. This class encapsulates the common interface and attributes for Spark
 * file rewrite runners. The actual implementation of the file rewrite logic is implemented by the
 * subclasses.
 *
 * @param <I> the Java type of the plan info like {@link RewriteDataFiles.FileGroupInfo} or {@link
 *     RewritePositionDeleteFiles.FileGroupInfo}
 * @param <T> the Java type of the input scan tasks (input)
 * @param <F> the Java type of the content files (input and output)
 * @param <G> the Java type of the rewrite file group like {@link RewriteFileGroup} or {@link
 *     RewritePositionDeletesGroup}
 */
abstract class SparkRewriteRunner<
        I,
        T extends ContentScanTask<F>,
        F extends ContentFile<F>,
        G extends RewriteGroupBase<I, T, F>>
    implements FileRewriteRunner<I, T, F, G> {
  private final SparkSession spark;
  private final Table table;

  SparkRewriteRunner(SparkSession spark, Table table) {
    this.spark = spark;
    this.table = table;
  }

  SparkSession spark() {
    return spark;
  }

  Table table() {
    return table;
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.of();
  }

  @Override
  public void init(Map<String, String> options) {}

  PartitionSpec spec(int specId) {
    return table().specs().get(specId);
  }
}
