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
import org.apache.iceberg.spark.actions.BaseMigrateTableSparkAction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * Takes a Spark table in the sourceCatalog and attempts to transform it into an Iceberg
 * Table in the same location with the same identifier. Once complete the identifier which
 * previously referred to a non-iceberg table will refer to the newly migrated iceberg
 * table.
 *
 * @deprecated since 0.12.0, will be removed in 0.13.0; use {@link MigrateTable} instead.
 */
@Deprecated
public class Spark3MigrateAction implements CreateAction {

  private final MigrateTable delegate;

  public Spark3MigrateAction(SparkSession spark, CatalogPlugin sourceCatalog, Identifier sourceTableName) {
    this.delegate = new BaseMigrateTableSparkAction(spark, sourceCatalog, sourceTableName);
  }

  @Override
  public CreateAction withProperties(Map<String, String> properties) {
    delegate.tableProperties(properties);
    return this;
  }

  @Override
  public CreateAction withProperty(String key, String value) {
    delegate.tableProperty(key, value);
    return this;
  }

  @Override
  public Long execute() {
    MigrateTable.Result result = delegate.execute();
    return result.migratedDataFilesCount();
  }
}
