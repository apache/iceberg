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

import java.util.function.Supplier;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

class SparkScanBroadcasts {

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final Supplier<FileIO> fileIO;

  // lazy variables
  private Broadcast<Table> tableBroadcast = null;
  private Broadcast<FileIO> fileIOBroadcast = null;

  SparkScanBroadcasts(JavaSparkContext sparkContext, Table table, Supplier<FileIO> fileIO) {
    this.sparkContext = sparkContext;
    this.table = table;
    this.fileIO = fileIO;
  }

  synchronized Broadcast<Table> table() {
    if (tableBroadcast == null) {
      this.tableBroadcast = sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
    }

    return tableBroadcast;
  }

  synchronized Broadcast<FileIO> fileIO() {
    if (fileIOBroadcast == null) {
      this.fileIOBroadcast = sparkContext.broadcast(SerializableFileIOWithSize.wrap(fileIO.get()));
    }

    return fileIOBroadcast;
  }
}
