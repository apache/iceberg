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
package org.apache.iceberg.snowflake;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.util.EnvironmentUtil;

class TestConfigurations {
  private static TestConfigurations instance;
  private Map<String, String> properties;

  private TestConfigurations() {
    Map<String, String> props = new HashMap<String, String>();
    // add catalog properties
    props.put(CatalogProperties.URI, "env:" + "SNOW_URI");

    // add jdbc properties, env: prefix is added to annotate that these variables should be
    // resolved by EnvironmentUtil.
    props.put("jdbc." + CatalogProperties.URI, "env:" + "SNOW_URI");
    props.put("jdbc." + CatalogProperties.USER, "env:" + "SNOW_USER");
    props.put("jdbc.password", "env:" + "SNOW_PASSWORD");
    props.put("jdbc.warehouse", "env:" + "SNOW_WAREHOUSE");
    props.put("jdbc.database", "env:" + "SNOW_TEST_DB_NAME");

    properties = EnvironmentUtil.resolveAll(props);
  }

  String getURI() {
    return properties.get(CatalogProperties.URI);
  }

  String getUser() {
    return properties.get("jdbc." + CatalogProperties.USER);
  }

  String getWarehouse() {
    return properties.get("jdbc.warehouse");
  }

  String getDatabase() {
    return properties.get("jdbc.database");
  }

  Map<String, String> getProperties() {
    return properties;
  }

  static synchronized TestConfigurations getInstance() {
    if (instance == null) {
      instance = new TestConfigurations();
    }
    return instance;
  }
}
