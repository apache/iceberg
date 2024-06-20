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
package org.apache.iceberg.spark.extensions;

import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestHiveViews extends TestViews {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, location = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK_WITH_HIVE_VIEWS.catalogName(),
        SparkCatalogConfig.SPARK_WITH_HIVE_VIEWS.implementation(),
        SparkCatalogConfig.SPARK_WITH_HIVE_VIEWS.properties(),
        "file:" + TestHiveMetastore.HIVE_LOCAL_DIR
      }
    };
  }
}
