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
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteStrategy;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.actions.rewrite.Spark3BinPackStrategy;
import org.apache.spark.sql.SparkSession;

public class RewriteDataFilesSpark3 extends BaseRewriteDataFilesSparkAction {
  private FileRewriteCoordinator coordinator = FileRewriteCoordinator.get();

  protected RewriteDataFilesSpark3(SparkSession spark, Table table) {
    super(spark, table);
  }

  @Override
  protected RewriteStrategy rewriteStrategy(Strategy type) {
    switch (type) {
      case BINPACK: return new Spark3BinPackStrategy(table(), spark());
    }
    throw new IllegalArgumentException(String.format(
        "Cannot create rewrite strategy for %s because %s is not yet supported in Spark3", type, type));
  }

  @Override
  protected void commitFileGroups(Set<String> completedGroups) {
    try {
      coordinator.commitRewrite(table(), completedGroups);
    } catch (Exception e) {
      completedGroups.forEach(this::abortFileGroup);
      throw e;
    }
  }

  @Override
  protected void abortFileGroup(String setId) {
    coordinator.abortRewrite(table(), setId);
  }

  @Override
  protected RewriteDataFiles self() {
    return this;
  }
}
