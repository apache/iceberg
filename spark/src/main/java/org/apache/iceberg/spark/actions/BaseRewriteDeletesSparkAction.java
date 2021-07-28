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

import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BaseRewriteDeletesResult;
import org.apache.iceberg.actions.RewriteDeleteStrategy;
import org.apache.iceberg.actions.RewriteDeletes;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
  private boolean isRewriteEqDelete;
  private boolean isRewritePosDelete;
  private RewriteDeleteStrategy strategy;

  protected BaseRewriteDeletesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.fileIO = table.io();
    this.strategy = new ConvertEqDeletesStrategy(spark, table);
  }

  /**
   * Set the rewrite delete strategy.
   *
   * @param newStrategy the strategy for rewrite deletes.
   */
  public void setStrategy(RewriteDeleteStrategy newStrategy) {
    this.strategy = newStrategy;
  }

  @Override
  public Result execute() {
    if (isRewriteEqDelete) {
      Iterable<DeleteFile> deletesToReplace = strategy.selectDeletes();
      Iterable<DeleteFile> deletesToAdd = strategy.rewriteDeletes();
      rewriteEqualityDeletes(Sets.newHashSet(deletesToReplace), Sets.newHashSet(deletesToAdd));

      return new BaseRewriteDeletesResult(Sets.newHashSet(deletesToReplace), Sets.newHashSet(deletesToAdd));
    }

    return new BaseRewriteDeletesResult(Collections.emptySet(), Collections.emptySet());
  }

  @Override
  public RewriteDeletes rewriteEqDeletes() {
    this.isRewriteEqDelete = true;
    return this;
  }

  @Override
  public RewriteDeletes rewritePosDeletes() {
    this.isRewritePosDelete = true;
    return null;
  }

  @Override
  protected RewriteDeletes self() {
    return this;
  }

  private void rewriteEqualityDeletes(Set<DeleteFile> eqDeletes, Set<DeleteFile> posDeletes) {
    Preconditions.checkArgument(eqDeletes.stream().allMatch(f -> f.content().equals(FileContent.EQUALITY_DELETES)),
        "The deletes to be converted should be equality deletes");
    Preconditions.checkArgument(posDeletes.stream().allMatch(f -> f.content().equals(FileContent.POSITION_DELETES)),
        "The converted deletes should be position deletes");
    try {
      RewriteFiles rewriteFiles = table.newRewrite();
      rewriteFiles.rewriteFiles(ImmutableSet.of(), Sets.newHashSet(eqDeletes), ImmutableSet.of(),
          Sets.newHashSet(posDeletes));
      rewriteFiles.commit();
    } catch (Exception e) {
      Tasks.foreach(Iterables.transform(posDeletes, f -> f.path().toString()))
          .noRetry()
          .suppressFailureWhenFinished()
          .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
          .run(fileIO::deleteFile);
      throw e;
    }
  }
}
