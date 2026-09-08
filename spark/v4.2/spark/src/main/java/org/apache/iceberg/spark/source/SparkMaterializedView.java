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

import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.view.View;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A fresh materialized view exposed to Spark as a readable table.
 *
 * <p>Spark 4.2 represents views as an immutable {@code View} value produced by {@link
 * SparkView#toView}, so a materialized view cannot extend it. Reads are served directly from the
 * view's storage table while the Iceberg view metadata remains available for planning.
 */
public class SparkMaterializedView implements Table, SupportsRead {
  private final String catalogName;
  private final View icebergView;
  private final Table storageTable;

  public SparkMaterializedView(String catalogName, View icebergView, Table storageTable) {
    this.catalogName = catalogName;
    this.icebergView = icebergView;
    this.storageTable = storageTable;
  }

  public View view() {
    return icebergView;
  }

  public String catalogName() {
    return catalogName;
  }

  public Table storageTable() {
    return storageTable;
  }

  @Override
  public String name() {
    return icebergView.name();
  }

  @Override
  public StructType schema() {
    return storageTable.schema();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return ((SupportsRead) storageTable).newScanBuilder(options);
  }

  @Override
  public Set<TableCapability> capabilities() {
    return ImmutableSet.of(TableCapability.BATCH_READ);
  }
}
