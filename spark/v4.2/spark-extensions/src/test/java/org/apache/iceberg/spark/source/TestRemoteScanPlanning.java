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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.sql.TestSelect;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRemoteScanPlanning extends TestSelect {
  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, binaryTableName = {3}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.REST.catalogName(),
        SparkCatalogConfig.REST.implementation(),
        ImmutableMap.builder()
            .putAll(SparkCatalogConfig.REST.properties())
            .put(CatalogProperties.URI, restCatalog.properties().get(CatalogProperties.URI))
            .put(
                RESTCatalogProperties.SCAN_PLANNING_MODE,
                RESTCatalogProperties.ScanPlanningMode.SERVER.modeName())
            .build(),
        SparkCatalogConfig.REST.catalogName() + ".default.binary_table"
      }
    };
  }

  @TestTemplate
  public void fileIOIsPropagated() {
    RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize(
        "test",
        ImmutableMap.<String, String>builder()
            .putAll(restCatalog.properties())
            .put(
                RESTCatalogProperties.SCAN_PLANNING_MODE,
                RESTCatalogProperties.ScanPlanningMode.SERVER.modeName())
            .build());
    Table table = catalog.loadTable(tableIdent);

    SparkScanBuilder builder = new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty());
    verifyFileIOHasPlanId(builder.build().toBatch(), table);
    verifyFileIOHasPlanId(builder.buildCopyOnWriteScan().toBatch(), table);
  }

  private void verifyFileIOHasPlanId(Batch batch, Table table) {
    FileIO fileIOForScan =
        (FileIO)
            assertThat(batch)
                .extracting("fileIO")
                .isInstanceOf(Supplier.class)
                .asInstanceOf(InstanceOfAssertFactories.type(Supplier.class))
                .actual()
                .get();
    assertThat(fileIOForScan.properties()).containsKey(RESTCatalogProperties.REST_SCAN_PLAN_ID);
    assertThat(table.io().properties()).doesNotContainKey(RESTCatalogProperties.REST_SCAN_PLAN_ID);
  }
}
