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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL_DEFAULT;
import static org.apache.iceberg.TableProperties.MERGE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.MERGE_ISOLATION_LEVEL_DEFAULT;
import static org.apache.iceberg.TableProperties.UPDATE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.UPDATE_ISOLATION_LEVEL_DEFAULT;

import java.util.Map;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;

class SparkRowLevelOperationBuilder implements RowLevelOperationBuilder {

  private final SparkSession spark;
  private final Table table;
  private final String branch;
  private final RowLevelOperationInfo info;
  private final RowLevelOperationMode mode;
  private final IsolationLevel isolationLevel;

  SparkRowLevelOperationBuilder(
      SparkSession spark, Table table, String branch, RowLevelOperationInfo info) {
    this.spark = spark;
    this.table = table;
    this.branch = branch;
    this.info = info;
    this.mode = mode(new SparkWriteConf(spark, table, branch, ImmutableMap.of()), info.command());
    this.isolationLevel = isolationLevel(table.properties(), info.command());
  }

  @Override
  public RowLevelOperation build() {
    switch (mode) {
      case COPY_ON_WRITE:
        return new SparkCopyOnWriteOperation(spark, table, branch, info, isolationLevel);
      case MERGE_ON_READ:
        return new SparkPositionDeltaOperation(spark, table, branch, info, isolationLevel);
      default:
        throw new IllegalArgumentException("Unsupported operation mode: " + mode);
    }
  }

  private RowLevelOperationMode mode(SparkWriteConf conf, Command command) {
    String modeName;

    switch (command) {
      case DELETE:
        modeName = conf.writeDeleteMode();
        break;
      case UPDATE:
        modeName = conf.writeUpdateMode();
        break;
      case MERGE:
        modeName = conf.writeMergeMode();
        break;
      default:
        throw new IllegalArgumentException("Unsupported command: " + command);
    }

    return RowLevelOperationMode.fromName(modeName);
  }

  private IsolationLevel isolationLevel(Map<String, String> properties, Command command) {
    String levelName;

    switch (command) {
      case DELETE:
        levelName = properties.getOrDefault(DELETE_ISOLATION_LEVEL, DELETE_ISOLATION_LEVEL_DEFAULT);
        break;
      case UPDATE:
        levelName = properties.getOrDefault(UPDATE_ISOLATION_LEVEL, UPDATE_ISOLATION_LEVEL_DEFAULT);
        break;
      case MERGE:
        levelName = properties.getOrDefault(MERGE_ISOLATION_LEVEL, MERGE_ISOLATION_LEVEL_DEFAULT);
        break;
      default:
        throw new IllegalArgumentException("Unsupported command: " + command);
    }

    return IsolationLevel.fromName(levelName);
  }
}
