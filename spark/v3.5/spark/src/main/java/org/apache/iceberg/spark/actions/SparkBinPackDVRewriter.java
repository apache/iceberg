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
import java.util.List;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.spark.sql.SparkSession;

class SparkBinPackDVRewriter extends SparkBinPackPositionDeletesRewriter {

  SparkBinPackDVRewriter(SparkSession spark, Table table) {
    super(spark, table);
  }

  @Override
  public String description() {
    return "BIN-PACK";
  }

  @Override
  protected Iterable<List<PositionDeletesScanTask>> filterFileGroups(
      List<List<PositionDeletesScanTask>> groups) {
    return Iterables.filter(groups, this::shouldRewrite);
  }

  private boolean shouldRewrite(List<PositionDeletesScanTask> group) {
    return enoughInputFiles(group)
        || enoughContent(group)
        || tooMuchContent(group)
        || tooHighDeleteRatio(group);
  }

  private boolean tooHighDeleteRatio(List<PositionDeletesScanTask> group) {
    if (group.isEmpty()) {
      return false;
    }

    long liveDataSize = group.stream().mapToLong(task -> task.file().contentSizeInBytes()).sum();

    String puffinLocation = group.get(0).file().location();
    long totalDataSize;
    try (PuffinReader reader = Puffin.read(table().io().newInputFile(puffinLocation)).build()) {
      totalDataSize = reader.dataSize();
    } catch (IOException e) {
      // TODO: probably better to just return false here
      throw new RuntimeIOException(e);
    }

    double liveRatio = liveDataSize / (double) totalDataSize;
    return 1.0d - liveRatio >= 0.7;
  }
}
