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
import org.apache.iceberg.actions.SnapshotUpdate;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.SparkSession;

abstract class BaseSnapshotUpdateSparkAction<ThisT, R> extends BaseSparkAction<ThisT, R>
    implements SnapshotUpdate<ThisT, R> {

  private final Map<String, String> summary = Maps.newHashMap();

  protected BaseSnapshotUpdateSparkAction(SparkSession spark) {
    super(spark);
  }

  @Override
  public ThisT snapshotProperty(String property, String value) {
    summary.put(property, value);
    return self();
  }

  protected void commit(org.apache.iceberg.SnapshotUpdate<?> update) {
    summary.forEach(update::set);
    update.commit();
  }
}
