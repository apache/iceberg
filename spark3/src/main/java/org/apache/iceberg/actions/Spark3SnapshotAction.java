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

import java.util.Map;
import org.apache.iceberg.spark.actions.BaseSnapshotTableSparkAction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * Creates a new Iceberg table based on a source Spark table. The new Iceberg table will
 * have a different data and metadata directory allowing it to exist independently of the
 * source table.
 *
 * @deprecated since 0.12.0, will be removed in 0.13.0; use {@link SnapshotTable} instead.
 */
@Deprecated
public class Spark3SnapshotAction implements SnapshotAction {

  private final SnapshotTable delegate;

  public Spark3SnapshotAction(SparkSession spark, CatalogPlugin sourceCatalog,
                              Identifier sourceTableIdent, CatalogPlugin destCatalog,
                              Identifier destTableIdent) {

    delegate = new BaseSnapshotTableSparkAction(spark, sourceCatalog, sourceTableIdent, destCatalog, destTableIdent);
  }

  @Override
  public SnapshotAction withLocation(String location) {
    delegate.tableLocation(location);
    return this;
  }

  @Override
  public SnapshotAction withProperties(Map<String, String> properties) {
    delegate.tableProperties(properties);
    return this;
  }

  @Override
  public SnapshotAction withProperty(String key, String value) {
    delegate.tableProperty(key, value);
    return this;
  }

  @Override
  public Long execute() {
    SnapshotTable.Result result = delegate.execute();
    return result.importedDataFilesCount();
  }
}
