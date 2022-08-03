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

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * An action to delete orphan files.
 *
 * @deprecated since 0.14.0, will be removed in 1.0.0; use {@link SparkActions} and {@link
 *     DeleteOrphanFilesSparkAction} instead.
 */
@Deprecated
public class BaseDeleteOrphanFilesSparkAction extends DeleteOrphanFilesSparkAction {

  public BaseDeleteOrphanFilesSparkAction(SparkSession spark, Table table) {
    super(spark, table);
  }

  @Override
  public BaseDeleteOrphanFilesSparkAction executeDeleteWith(ExecutorService executorService) {
    super.executeDeleteWith(executorService);
    return this;
  }

  @Override
  public BaseDeleteOrphanFilesSparkAction location(String newLocation) {
    super.location(newLocation);
    return this;
  }

  @Override
  public BaseDeleteOrphanFilesSparkAction olderThan(long newOlderThanTimestamp) {
    super.olderThan(newOlderThanTimestamp);
    return this;
  }

  @Override
  public BaseDeleteOrphanFilesSparkAction deleteWith(Consumer<String> newDeleteFunc) {
    super.deleteWith(newDeleteFunc);
    return this;
  }

  public BaseDeleteOrphanFilesSparkAction compareToFileList(Dataset<Row> files) {
    super.compareToFileList(files);
    return this;
  }
}
