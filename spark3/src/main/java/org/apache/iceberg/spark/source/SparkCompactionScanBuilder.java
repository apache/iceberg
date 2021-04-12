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

import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkCompactionScanBuilder implements ScanBuilder {

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final boolean caseSensitive;
  private final CaseInsensitiveStringMap options;

  SparkCompactionScanBuilder(SparkSession spark, Table table, CaseInsensitiveStringMap options) {
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
    this.table = table;
    this.caseSensitive = Boolean.parseBoolean(spark.conf().get("spark.sql.caseSensitive"));
    this.options = options;
  }

  @Override
  public Scan build() {
    Broadcast<FileIO> io = sparkContext.broadcast(SparkUtil.serializableFileIO(table));
    Broadcast<EncryptionManager> encryption = sparkContext.broadcast(table.encryption());
    return new SparkCompactionScan(table, io, encryption, caseSensitive, options);
  }
}
