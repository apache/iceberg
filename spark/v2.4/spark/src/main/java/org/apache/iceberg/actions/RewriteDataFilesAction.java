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

package org.apache.iceberg.actions;

import java.util.List;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.RewriteResult;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.source.RowDataRewriter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

/**
 * @deprecated since 0.12.0, keeping this in Spark 2.4 for backward compatibility.
 * This implementation is no longer maintained, the new implementation is available with Spark 3.x
 */
@Deprecated
public class RewriteDataFilesAction
    extends BaseRewriteDataFilesAction<RewriteDataFilesAction> {

  private final JavaSparkContext sparkContext;
  private FileIO fileIO;

  RewriteDataFilesAction(SparkSession spark, Table table) {
    super(table);
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @Override
  protected RewriteDataFilesAction self() {
    return this;
  }

  @Override
  protected FileIO fileIO() {
    if (this.fileIO == null) {
      this.fileIO = SparkUtil.serializableFileIO(table());
    }
    return this.fileIO;
  }

  @Override
  protected RewriteResult rewriteDataForTasks(List<CombinedScanTask> combinedScanTasks) {
    JavaRDD<CombinedScanTask> taskRDD = sparkContext.parallelize(combinedScanTasks, combinedScanTasks.size());
    Broadcast<Table> tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table()));
    RowDataRewriter rowDataRewriter = new RowDataRewriter(tableBroadcast, spec(), caseSensitive());
    return rowDataRewriter.rewriteDataForTasks(taskRDD);
  }
}
