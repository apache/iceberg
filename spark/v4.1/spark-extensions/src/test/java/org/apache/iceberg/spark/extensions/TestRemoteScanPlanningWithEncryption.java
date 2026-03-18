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

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.encryption.UnitestKMS;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.sql.TestTableEncryption;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRemoteScanPlanningWithEncryption extends TestTableEncryption {

  private static Map<String, String> encryptionProperties(Map<String, String> props) {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.putAll(props);
    // TODO: This property is required for encrypted tables, but feels odd when scan planning is
    // enabled as the client then has no KMS interaction.
    newProps.put(CatalogProperties.ENCRYPTION_KMS_IMPL, UnitestKMS.class.getCanonicalName());
    return newProps;
  }

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.REST.catalogName(),
        SparkCatalogConfig.REST.implementation(),
        encryptionProperties(
            ImmutableMap.<String, String>builder()
                .putAll(SparkCatalogConfig.REST.properties())
                .put(CatalogProperties.URI, restCatalog.properties().get(CatalogProperties.URI))
                // this flag is typically only set by the server, but we set it from the client
                // for testing
                .put(RESTCatalogProperties.REST_SCAN_PLANNING_ENABLED, "true")
                .build())
      }
    };
  }
}
