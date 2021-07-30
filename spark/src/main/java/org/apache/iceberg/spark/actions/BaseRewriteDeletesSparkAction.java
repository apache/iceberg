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

import java.io.IOException;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BaseRewriteDeletesResult;
import org.apache.iceberg.actions.RewriteDeleteStrategy;
import org.apache.iceberg.actions.RewriteDeletes;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseRewriteDeletesSparkAction
    extends BaseSnapshotUpdateSparkAction<RewriteDeletes, RewriteDeletes.Result>
    implements RewriteDeletes {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRewriteDeletesSparkAction.class);

  private final Table table;
  private final FileIO fileIO;
  private final SparkSession spark;

  private RewriteDeleteStrategy strategy;

  protected BaseRewriteDeletesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.spark = spark;
    this.table = table;
    this.fileIO = table.io();
  }

  @Override
  public RewriteDeletes strategy(String strategyImpl) {
    DynConstructors.Ctor<RewriteDeleteStrategy> ctor;
    try {
      ctor = DynConstructors.builder(RewriteDeleteStrategy.class)
          .impl(strategyImpl, SparkSession.class, Table.class)
          .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(String.format(
          "Cannot initialize RewriteDeleteStrategy implementation %s: %s", strategyImpl, e.getMessage()), e);
    }

    try {
      strategy = ctor.newInstance(spark, table);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize RewriteDeleteStrategy, %s does not implement RewriteDeleteStrategy",
              strategyImpl), e);
    }

    return this;
  }

  @Override
  public Result execute() {
    if (strategy == null) {
      // use ConvertEqDeletesStrategy for default right now.
      strategy = new ConvertEqDeletesStrategy(spark, table);
    }
    CloseableIterable<FileScanTask> fileScanTasks = table.newScan()
            .ignoreResiduals()
            .planFiles();

    Set<DeleteFile> deletesToAdd = Sets.newHashSet();

    try {
      Iterable<DeleteFile> deletesToReplace = strategy.selectDeletesToRewrite(fileScanTasks);
      Iterable<Set<DeleteFile>> groupedDeletes = strategy.planDeleteGroups(deletesToReplace);
      for (Set<DeleteFile> deletesInGroup : groupedDeletes) {
        deletesToAdd.addAll(strategy.rewriteDeletes(deletesInGroup));
      }

      table.newRewrite()
          .rewriteFiles(ImmutableSet.of(), Sets.newHashSet(deletesToReplace),
              ImmutableSet.of(), Sets.newHashSet(deletesToAdd))
          .commit();

      return new BaseRewriteDeletesResult(Sets.newHashSet(deletesToReplace), Sets.newHashSet(deletesToAdd));
    } catch (Exception e) {
      Tasks.foreach(Iterables.transform(deletesToAdd, f -> f.path().toString()))
          .noRetry()
          .suppressFailureWhenFinished()
          .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
          .run(fileIO::deleteFile);
      throw e;
    } finally {
      try {
        fileScanTasks.close();
      } catch (IOException io) {
        LOG.error("Cannot properly close file iterable while planning for rewrite", io);
      }
    }
  }

  @Override
  protected RewriteDeletes self() {
    return this;
  }
}
