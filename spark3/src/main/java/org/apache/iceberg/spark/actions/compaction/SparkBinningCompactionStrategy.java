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

package org.apache.iceberg.spark.actions.compaction;

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.source.SourceUtil;
import org.apache.spark.sql.SparkSession;

public class SparkBinningCompactionStrategy extends BinningCompactionStrategy {

  private final SparkSession spark;

  public SparkBinningCompactionStrategy(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  public List<DataFile> rewriteFiles(Table table, List<FileScanTask> filesToRewrite) {
    return SourceUtil.rewriteFiles(spark, table, filesToRewrite, targetSize());
  }
}
