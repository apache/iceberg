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
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.FileRewriteExecutor;
import org.apache.iceberg.actions.FileRewriteGroup;
import org.apache.iceberg.actions.FileRewritePlan;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * Common parent for data and positional delete rewrite executors.
 *
 * @param <I> the Java type of the plan info
 * @param <T> the Java type of the tasks to read content files
 * @param <F> the Java type of the content files
 * @param <G> the Java type of the planned groups
 * @param <P> the Java type of the plan to execute
 */
abstract class SparkRewriteExecutor<
        I,
        T extends ContentScanTask<F>,
        F extends ContentFile<F>,
        G extends FileRewriteGroup<I, T, F>,
        P extends FileRewritePlan<I, T, F, G>>
    implements FileRewriteExecutor<I, T, F, G, P> {
  private final Table table;
  private long writeMaxFileSize;

  SparkRewriteExecutor(Table table) {
    this.table = table;
  }

  Table table() {
    return table;
  }

  long writeMaxFileSize() {
    return writeMaxFileSize;
  }

  @Override
  public void initPlan(P plan) {
    this.writeMaxFileSize = plan.writeMaxFileSize();
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.of();
  }

  @Override
  public void init(Map<String, String> options) {}
}
