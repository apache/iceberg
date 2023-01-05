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

import java.io.Serializable;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.Util;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.InputPartition;

class SparkInputPartition implements InputPartition, Serializable {
  private final ScanTaskGroup<?> taskGroup;
  private final Broadcast<Table> tableBroadcast;
  private final String expectedSchemaString;
  private final boolean caseSensitive;

  private transient Schema expectedSchema = null;
  private transient String[] preferredLocations = null;

  SparkInputPartition(
      ScanTaskGroup<?> taskGroup,
      Broadcast<Table> tableBroadcast,
      String expectedSchemaString,
      boolean caseSensitive,
      boolean localityPreferred) {
    this.taskGroup = taskGroup;
    this.tableBroadcast = tableBroadcast;
    this.expectedSchemaString = expectedSchemaString;
    this.caseSensitive = caseSensitive;
    if (localityPreferred) {
      Table table = tableBroadcast.value();
      this.preferredLocations = Util.blockLocations(table.io(), taskGroup);
    } else {
      this.preferredLocations = HadoopInputFile.NO_LOCATION_PREFERENCE;
    }
  }

  @Override
  public String[] preferredLocations() {
    return preferredLocations;
  }

  @SuppressWarnings("unchecked")
  public <T extends ScanTask> ScanTaskGroup<T> taskGroup() {
    return (ScanTaskGroup<T>) taskGroup;
  }

  public Table table() {
    return tableBroadcast.value();
  }

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  public Schema expectedSchema() {
    if (expectedSchema == null) {
      this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
    }

    return expectedSchema;
  }
}
